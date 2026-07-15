package slurm

// Gang scheduling.
//
// Pods that share the annotation `interlink.eu/gang-name` are BUFFERED in the
// plugin instead of being submitted one sbatch each. When the buffer reaches
// `interlink.eu/gang-size` members, ONE `sbatch --nodes=N` co-scheduled SLURM
// job is submitted for the whole group (one job, N ranks on N nodes), which
// co-allocates the entire gang atomically.
//
// Everything in this file is gated behind BOTH Config.GangSchedulingEnabled AND
// the presence of the gang-name annotation, so a pod without the annotation (or
// with the feature disabled) always takes the original single-pod path in
// Create.go. This file only READS interlink.eu/gang-* annotations; it never
// looks at ray.io/* or JobSet labels.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	exec "github.com/alexellis/go-execute/pkg/v1"
	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"al.essio.dev/pkg/shellescape"
)

// gangCoordinationPort is the coordination port exported to every rank as
// MASTER_PORT / the Ray GCS port. Set to Ray's native GCS port 6379 so KubeRay's
// injected RAY_PORT/RAY_ADDRESS env (which hardcode 6379) stay CONSISTENT with the
// port the head actually binds -- otherwise a Ray client that discovers the
// cluster via the RAY_ADDRESS env var (rather than an explicit --address) would
// dial the wrong port. torch/MPI don't care about the specific value, so 6379 is
// safe for all frameworks. Fixed for now; a future change can make it per-gang /
// annotation-driven.
const gangCoordinationPort = 6379

// isGangPod reports whether this pod should take the gang path: the feature must
// be enabled AND the pod must carry a non-empty interlink.eu/gang-name.
func isGangPod(config SlurmConfig, metadata metav1.ObjectMeta) bool {
	if !config.GangSchedulingEnabled {
		return false
	}
	name := strings.TrimSpace(metadata.Annotations[GangNameAnnotation])
	return name != ""
}

// gangSizeFromMeta parses interlink.eu/gang-size. It is required and must be a
// positive integer.
func gangSizeFromMeta(metadata metav1.ObjectMeta) (int, error) {
	raw, ok := metadata.Annotations[GangSizeAnnotation]
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("gang pod is missing required annotation %s", GangSizeAnnotation)
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", GangSizeAnnotation, raw, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("invalid %s=%q: must be >= 1", GangSizeAnnotation, raw)
	}
	return n, nil
}

// gangRoleFromMeta returns the normalized role (head or worker; default worker).
func gangRoleFromMeta(metadata metav1.ObjectMeta) string {
	role := strings.ToLower(strings.TrimSpace(metadata.Annotations[GangRoleAnnotation]))
	if role == GangRoleHead {
		return GangRoleHead
	}
	return GangRoleWorker
}

// gangGuaranteeTimeout returns the configured gang buffering timeout. When gang
// scheduling is enabled, NewSlurmConfig has already validated GangTimeout as a
// positive duration, so this returns exactly that value. The 10m fallback is
// defensive for the unset / programmatically-constructed-config cases; it uses
// the SAME predicate the config validation rejects on (unparseable or <=0), so
// the two never disagree on a value that reached this point.
func gangGuaranteeTimeout(config SlurmConfig) time.Duration {
	if strings.TrimSpace(config.GangTimeout) == "" {
		return 10 * time.Minute
	}
	d, err := time.ParseDuration(strings.TrimSpace(config.GangTimeout))
	if err != nil || d <= 0 {
		return 10 * time.Minute
	}
	return d
}

// assignRanks assigns a deterministic rank 0..N-1 to every buffered member:
//   - the head (role==head) is ALWAYS rank 0,
//   - members that carry an explicit interlink.eu/gang-rank keep it if free,
//   - everyone else is filled into the remaining ranks in arrival order.
//
// It mutates each BufferedMember.Rank and returns the members ordered by rank.
// This runs only at submission time (last arrival), so all members are present.
func assignRanks(members map[string]*BufferedMember) []*BufferedMember {
	// Stable arrival order: PodUID is not time-ordered, so we approximate arrival
	// by any explicit rank first, then by UID for determinism. The head is forced
	// to rank 0 regardless.
	all := make([]*BufferedMember, 0, len(members))
	for _, m := range members {
		all = append(all, m)
	}
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].PodUID < all[j].PodUID
	})

	size := len(all)
	taken := make([]bool, size)
	assigned := make(map[string]bool, size)

	// 1) Head is pinned to rank 0.
	for _, m := range all {
		if m.Role == GangRoleHead {
			m.Rank = 0
			taken[0] = true
			assigned[m.PodUID] = true
			break // only the first head wins rank 0
		}
	}

	// 2) Honor explicit gang-rank annotations that are in range and still free.
	for _, m := range all {
		if assigned[m.PodUID] {
			continue
		}
		if raw, ok := m.Pod.ObjectMeta.Annotations[GangRankAnnotation]; ok {
			if r, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && r >= 0 && r < size && !taken[r] {
				m.Rank = r
				taken[r] = true
				assigned[m.PodUID] = true
			}
		}
	}

	// 3) Fill everyone else into the lowest remaining free ranks, in UID order.
	next := 0
	for _, m := range all {
		if assigned[m.PodUID] {
			continue
		}
		for next < size && taken[next] {
			next++
		}
		if next >= size {
			break // defensive; shouldn't happen when size == len(members)
		}
		m.Rank = next
		taken[next] = true
		assigned[m.PodUID] = true
	}

	// Return ordered by rank.
	ordered := make([]*BufferedMember, 0, len(all))
	ordered = append(ordered, all...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Rank < ordered[j].Rank
	})
	return ordered
}

// produceGangSLURMScript renders ONE co-scheduled sbatch script for the whole
// gang into headMember.FilesPath/job.slurm and returns its path. It reproduces
// the validated 03-nodesN-ray.slurm shape:
//   - #SBATCH --nodes=N --ntasks-per-node=1, --job-name=<gang>, output to the
//     head member's dir (reusing buildSbatchFlags for flavor/annotation flags,
//     with the default --mem=1 suppressed),
//   - a head-address block (scontrol show hostnames + srun hostname --ip-address),
//   - generic coordination env exported to every rank (MASTER_ADDR/MASTER_PORT/
//     WORLD_SIZE/RANK/RAY_ADDRESS),
//   - one per-rank `srun --overlap --nodes=1 --ntasks=1 -w <node>` that runs the
//     member's OWN job.sh (already written by produceSLURMScript into that
//     member's dir), routing -o/-e into that member's own dir. Running each
//     member's job.sh means each rank produces the SAME per-pod artifacts a
//     single-pod job would (run-<ctn>.out and run-<ctn>.status) in its own dir.
//   - a readiness barrier after the head before the workers start,
//   - teardown that reaps the daemons and exits with the head/driver rc.
//
// Per-pod status behaviour (what actually happens, no overstatement):
// each gang member DIVERGES on its OWN terminal exit — its run-<ctn>.status is
// read by the gang-only branch in Status.go, so a failed rank shows Failed on its
// own pod while the gang and the other ranks keep running (no scancel). What
// remains INHERENTLY SHARED is whole-allocation SLURM state: an operator scancel,
// a node failure, or the walltime limit terminate the one job, so all member pods
// observe those together via the shared squeue JID. That shared-infrastructure
// ceiling is expected and by design — only per-rank workload exit codes are
// isolated (via each member's own run-<ctn>.status file).
func produceGangSLURMScript(
	Ctx context.Context,
	config SlurmConfig,
	entry *GangEntry,
	ordered []*BufferedMember,
) (string, error) {
	if len(ordered) == 0 {
		return "", fmt.Errorf("gang %s: no members to render", entry.Name)
	}
	head := ordered[0]
	headPath := head.FilesPath
	n := len(ordered)

	if err := os.MkdirAll(headPath, os.ModePerm); err != nil {
		log.G(Ctx).Error(err)
		return "", err
	}

	// --- #SBATCH header --------------------------------------------------------
	// Reuse the head member's flavor/annotation flags (with default --mem
	// suppressed, per the BSC-validated twin). --nodes / --ntasks-per-node are
	// gang-specific and prepended explicitly. Deduplication in buildSbatchFlags
	// runs on the flavor/annotation set; our fixed --nodes/--ntasks-per-node are
	// added after and are not user-overridable here.
	flags := buildSbatchFlags(Ctx, config, head.Pod, head.Pod.ObjectMeta, head.RuntimeCommands,
		head.ResourceLimits, head.IsDefaultCPU, head.IsDefaultRam, head.Flavor, true /*suppressDefaultMem*/)

	// --- launcher selection (injected-launcher path) --------------------------
	// interlink.eu/launcher picks a launcher file from the registry ConfigMap; its
	// declared topology (per-rank | collective) replaces the old gang-mode branch.
	// Loaded + validated FRESH here (crash-no-fallback: a bad launcher fails the
	// whole submit rather than silently reverting to a built-in default).
	gpusPerNode := gpusPerNodeFromFlags(flags)
	launcherName := selectedLauncherName(head.Pod.ObjectMeta)
	var launcher *launcherDef
	if launcherName != "" {
		ld, lerr := loadLauncher(Ctx, config, launcherName)
		if lerr != nil {
			return "", fmt.Errorf("gang %s: %w", entry.Name, lerr)
		}
		launcher = ld
	}
	// #SBATCH --ntasks-per-node: a collective launcher over GPUs wants one task per
	// GPU (gang6: QUDA's Ntask = total GPUs); everything else is one task per node.
	tasksPerNode := 1
	if launcher != nil && launcher.Topology == LauncherTopologyCollective && gpusPerNode > 0 {
		tasksPerNode = gpusPerNode
	}

	var hdr strings.Builder
	hdr.WriteString("#!" + config.BashPath + "\n")
	hdr.WriteString("#SBATCH --job-name=" + entry.Name + "\n")
	hdr.WriteString("#SBATCH --nodes=" + strconv.Itoa(n) + "\n")
	hdr.WriteString("#SBATCH --ntasks-per-node=" + strconv.Itoa(tasksPerNode) + "\n")
	// Batch-body stdout/stderr (driver output) lands in the head member's dir so
	// the plugin harvests it as the head pod's job.out (per-pod-dir log model).
	hdr.WriteString("#SBATCH --output=" + headPath + "/job.out\n")
	for _, f := range flags {
		if strings.TrimSpace(f) == "" {
			continue
		}
		// Skip a stray --nodes/--ntasks-per-node from flavor/annotations: the gang
		// size is authoritative and already emitted above.
		key := slurmFlagKey(f)
		if key == "--nodes" || key == "--ntasks-per-node" {
			continue
		}
		hdr.WriteString("#SBATCH " + f + "\n")
	}

	// --- body ------------------------------------------------------------------
	var body strings.Builder
	body.WriteString("set -uo pipefail\n\n")
	body.WriteString("# interLink gang-scheduling job (interlink.eu/gang-name=" + entry.Name + ")\n")
	body.WriteString("# One sbatch --nodes=" + strconv.Itoa(n) + " co-allocates the whole gang; each rank runs on one node.\n\n")

	// Site setup (e.g. `module load singularity`) at the BATCH level, mirroring the
	// single-pod path in produceSLURMScript. The single-pod script runs the
	// CommandPrefix in the batch shell, so its PATH changes are visible to job.sh.
	// The gang used to run the CommandPrefix inside each per-rank `srun bash -c`, a
	// FRESH sub-shell where the `module` shell-function is often NOT defined (the
	// interLink sbatch is submitted via a non-login `ssh <host> sbatch`), so
	// `module load singularity` silently no-oped, `singularity` was off PATH, and
	// job.sh's bare `singularity` exited 127. Running it ONCE here executes
	// `module load` in the batch shell (where `module` IS defined) and the resulting
	// PATH propagates to every rank via `srun --export=ALL`. It also writes the
	// compute-node file exactly once (from the head/batch node) instead of the old
	// racing per-rank writes.
	if strings.TrimSpace(config.Commandprefix) != "" {
		body.WriteString("# --- site CommandPrefix (batch level; PATH propagates to ranks via --export=ALL) ---\n")
		body.WriteString(config.Commandprefix + "\n\n")
	}

	// HPC-tuning hook (BOTH modes): the head pod's slurm-job.vk.io/mpi-env
	// annotation ("K=V;K=V") is materialized as `export SINGULARITYENV_<K>=<V>`
	// in the batch shell BEFORE the launch section. Singularity's SINGULARITYENV_
	// prefix mechanism MERGES these into every rank's container env (it does NOT
	// clobber the per-rank --env-file that carries MASTER_ADDR/RANK/WORLD_SIZE, so
	// torch can inject NCCL_* / UCX_* env here without breaking rendezvous). The
	// exports also stay visible to the host launcher (UCX/PMIx server side) for the
	// MPI path. Empty/malformed pairs (no '=', empty key) are skipped.
	if exports := gangMPIEnvExports(head.Pod.ObjectMeta); len(exports) > 0 {
		body.WriteString("# --- HPC tuning env (slurm-job.vk.io/mpi-env -> SINGULARITYENV_*; merged into every rank) ---\n")
		for _, e := range exports {
			body.WriteString(e + "\n")
		}
		body.WriteString("\n")
	}

	// Deterministic head-node discovery for the shadow/tunnel pods. The batch
	// script executes on nodes[0], which IS rank 0's (the head's) node, so
	// `hostname -f` here is the head node. Publish it under a well-known,
	// gang-keyed path: the VK's wstunnel template exposes only name / namespace /
	// labels / annotations (NO pod UID, verified against interLink 0.6.1), so a
	// shadow pod cannot resolve the head member's per-UID dir - but it CAN read
	// its own interlink.eu/gang-name annotation and fetch this file. A recreated
	// gang generation simply overwrites the file (latest wins). Best-effort:
	// discovery must never kill the workload.
	headDiscovery := config.DataRootFolder + gangHeadsDirName + "/" + head.Namespace + "-" + entry.Name
	body.WriteString("# --- publish head node for shadow/tunnel discovery (batch node == head node) ---\n")
	body.WriteString("mkdir -p " + shellescape.Quote(config.DataRootFolder+gangHeadsDirName) +
		" && hostname -f > " + shellescape.Quote(headDiscovery) + " || true\n\n")

	port := gangCoordinationPort

	// Resolve the allocation's nodes and the head IP (copied from the twin).
	body.WriteString(`mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")` + "\n")
	body.WriteString(`head_node="${nodes[0]}"` + "\n")
	body.WriteString(`head_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address | awk '{print $1}')` + "\n")
	body.WriteString("export MASTER_ADDR=\"$head_ip\"\n")
	body.WriteString("export MASTER_PORT=" + strconv.Itoa(port) + "\n")
	body.WriteString("export WORLD_SIZE=" + strconv.Itoa(n) + "\n")
	body.WriteString("export RAY_ADDRESS=\"$head_ip:" + strconv.Itoa(port) + "\"\n")
	body.WriteString(`echo "gang " ` + shellescape.Quote(entry.Name) + ` " head_node=$head_node head_ip=$head_ip world_size=` + strconv.Itoa(n) + `"` + "\n")
	// Gang topology exported for injected launchers (launcher_main reads these).
	body.WriteString("export GANG_NNODES=" + strconv.Itoa(n) + "\n")
	body.WriteString("export GANG_GPUS_PER_NODE=" + strconv.Itoa(gpusPerNode) + "\n\n")

	// --- launch section --------------------------------------------------------
	// Selection order (see gang CLAUDE.md):
	//   1. interlink.eu/launcher present -> injected launcher; its declared topology
	//      picks per-rank (emitLauncherPerRank) vs collective (emitLauncherCollective).
	//   2. else interlink.eu/gang-mode=mpi (DEPRECATED alias) -> built-in collective MPI.
	//   3. else (DEFAULT) -> built-in per-rank model (Ray / torch-distributed): one
	//      `srun --overlap --nodes=1 --ntasks=1` per member + readiness barrier +
	//      DRIVER_RC = rank-0's rc.
	switch {
	case launcher != nil:
		launcherPath, werr := writeLauncherFile(headPath, launcher)
		if werr != nil {
			return "", fmt.Errorf("gang %s: %w", entry.Name, werr)
		}
		if launcher.Topology == LauncherTopologyCollective {
			emitLauncherCollective(&body, head, launcher, launcherPath)
		} else {
			emitLauncherPerRank(&body, ordered, launcher, launcherPath, n, port)
		}
	case strings.EqualFold(strings.TrimSpace(head.Pod.ObjectMeta.Annotations[GangModeAnnotation]), GangModeMPI):
		log.G(Ctx).Warningf("gang %s: interlink.eu/gang-mode=mpi is DEPRECATED; migrate to interlink.eu/launcher (e.g. mpi-openmpi). Using the built-in collective-MPI shape.", entry.Name)
		emitGangMPILauncher(&body, head, n)
	default:
		emitGangPerRankLaunch(&body, ordered, n, port)
	}

	// --- write job.slurm -------------------------------------------------------
	scriptPath := headPath + "/job.slurm"
	full := hdr.String() + "\n" + body.String()
	if err := os.WriteFile(scriptPath, []byte(full), 0o774); err != nil {
		log.G(Ctx).Error("Unable to write gang job.slurm ", scriptPath, ": ", err)
		return "", err
	}
	log.G(Ctx).Infof("Rendered gang sbatch for '%s' (%d nodes) at %s", entry.Name, n, scriptPath)
	return scriptPath, nil
}

// emitGangPerRankLaunch renders the DEFAULT per-rank launch section (Ray/torch):
// one `srun --overlap --nodes=1 --ntasks=1` per member, a readiness barrier
// before the workers, and DRIVER_RC = rank-0's rc as the whole-gang result. This
// is the original, unchanged behavior; the MPI path lives in
// emitGangMPILauncher.
func emitGangPerRankLaunch(bodyPtr *strings.Builder, ordered []*BufferedMember, n, port int) {
	body := bodyPtr
	// Track background PIDs so we can wait/teardown.
	body.WriteString("declare -a RANK_PIDS=()\n\n")

	// Emit one srun per rank. Rank 0 (head) launches first, then a readiness
	// barrier, then the remaining ranks. Each srun runs the member's OWN job.sh
	// (already written by produceSLURMScript into m.FilesPath), so the member's
	// containers run with all the usual per-pod bookkeeping (run-<ctn>.out /
	// run-<ctn>.status) landing in that member's dir -> isolated per-pod logs and
	// status. The gang coordination env is exported before job.sh runs.
	emitRank := func(m *BufferedMember) {
		node := "${nodes[" + strconv.Itoa(m.Rank) + "]}"
		out := m.FilesPath + "/job.out"
		errOut := m.FilesPath + "/job.err"
		memberScript := m.FilesPath + "/job.sh"
		// Record THIS rank's compute node into THIS member's own dir, then exec the
		// member's job.sh. The write runs inside the rank's srun (hostname -f on the
		// rank's node - in a --nodes=N gang the worker is on a DIFFERENT node than
		// the head), giving true per-member node discovery for the shadow/tunnel
		// pods. Best-effort (|| true): a failed write must not kill the rank.
		//
		// The site CommandPrefix (module loads etc.) is emitted ONCE at the batch
		// level above; its PATH changes reach this rank via `srun --export=ALL`, so
		// the member's job.sh finds its container runtime. We deliberately do NOT
		// re-run the CommandPrefix here: a per-rank `srun bash -c` is a fresh
		// sub-shell where the `module` shell-function is often undefined, so
		// `module load singularity` would no-op and job.sh's bare `singularity`
		// would exit 127 (the original bug). Coordination env is passed via srun
		// --export below (so $head_ip resolves in the batch shell).
		inner := "hostname -f > " + shellescape.Quote(m.FilesPath+"/compute-node") + " || true\n" +
			"exec " + shellescape.Quote(memberScript)
		body.WriteString("# rank " + strconv.Itoa(m.Rank) + " (" + m.Role + ") pod " + m.PodUID + "\n")
		body.WriteString("srun --overlap --nodes=1 --ntasks=1 -w \"" + node + "\" \\\n")
		// Coordination env via srun --export: $head_ip resolves in the batch shell
		// (double-quoted here), so the value is baked into the srun command line and
		// reaches the task regardless of the site's default SLURM export policy.
		body.WriteString("     --export=ALL,RANK=" + strconv.Itoa(m.Rank) + ",WORLD_SIZE=" + strconv.Itoa(n) +
			",MASTER_ADDR=\"$head_ip\",MASTER_PORT=" + strconv.Itoa(port) +
			",RAY_ADDRESS=\"$head_ip:" + strconv.Itoa(port) + "\" \\\n")
		body.WriteString("     -o " + shellescape.Quote(out) + " -e " + shellescape.Quote(errOut) + " --open-mode=truncate \\\n")
		body.WriteString("  bash -c " + shellescape.Quote(inner) + " &\n")
		body.WriteString("RANK_PIDS+=($!)\n\n")
	}

	// Head first.
	emitRank(ordered[0])

	// Readiness barrier before workers: poll `ray status` against the head GCS,
	// mirroring the twin's probe loop. Best-effort: if `ray` is not on PATH (a
	// non-Ray workload), the probe simply times out and we proceed anyway so
	// non-Ray gangs are not blocked.
	if n > 1 {
		body.WriteString("# readiness barrier: wait for the head coordinator before launching workers\n")
		body.WriteString("head_ready=0\n")
		body.WriteString("for i in $(seq 1 60); do\n")
		body.WriteString("  # Generic readiness: the head (rank 0) opens MASTER_PORT (Ray GCS / torch\n")
		body.WriteString("  # rendezvous). Probe the port from the head node ($head_ip/$MASTER_PORT\n")
		body.WriteString("  # expand in the batch shell). The loop proceeds anyway after the timeout so\n")
		body.WriteString("  # a workload that never opens the port is not blocked forever.\n")
		body.WriteString("  if srun --overlap --nodes=1 --ntasks=1 -w \"$head_node\" bash -c \"exec 3<>/dev/tcp/$head_ip/$MASTER_PORT\" >/dev/null 2>&1; then head_ready=1; break; fi\n")
		body.WriteString("  sleep 2\n")
		body.WriteString("done\n")
		body.WriteString("echo \"gang head_ready=$head_ready\"\n\n")

		// Then the workers.
		for _, m := range ordered[1:] {
			emitRank(m)
		}
	}

	// Wait for all ranks; capture the head/driver rc from rank 0 so the pod exit
	// status reflects the workload, not a blocking daemon (twin's DRIVER_RC).
	body.WriteString("# Wait for the head (rank 0 / driver) and capture its rc as the gang result.\n")
	body.WriteString("DRIVER_RC=0\n")
	body.WriteString("if [ \"${#RANK_PIDS[@]}\" -gt 0 ]; then\n")
	body.WriteString("  wait \"${RANK_PIDS[0]}\"; DRIVER_RC=$?\n")
	body.WriteString("fi\n")
	body.WriteString("# Reap remaining ranks so the allocation frees.\n")
	body.WriteString("for pid in \"${RANK_PIDS[@]:1}\"; do kill \"$pid\" 2>/dev/null || true; done\n")
	body.WriteString("wait 2>/dev/null || true\n")
	body.WriteString("echo \"DRIVER_RC=$DRIVER_RC\"\n")
	body.WriteString("exit \"$DRIVER_RC\"\n")
}

// emitGangMPILauncher renders the classic-MPI launch section: ONE collective
// srun spanning the whole allocation, one MPI rank per node. It is selected only
// when the head pod carries interlink.eu/gang-mode=mpi. There is no per-rank
// loop, no readiness barrier, and no DRIVER_RC bookkeeping - the single srun's rc
// IS the whole-job rc (the batch shell exits with it because `srun` is the last
// command and `set -uo pipefail` leaves ${?} unmasked). Non-head members render
// no srun at all; their pods still get the shared JID back-filled by stampJID for
// Status / Delete.
//
// The container invocation is reconstructed EXACTLY as runCtn does (prepare.go):
// strings.Join(rc.runtimeCommand, " ") (already includes the singularity exec
// prefix, mounts, options and the SIF path as its last element) followed by each
// rc.containerCommand and rc.containerArgs entry, shellescape-quoted.
//
// Launcher shape (default): `srun` is the launcher AND the PMIx server; each task
// == one `singularity exec` == one MPI rank. `--ntasks-per-node=1` puts one rank
// per node for the first milestone; PMIx does the wireup so no MASTER_ADDR is
// needed. SRUN_CPUS_PER_TASK is exported because MN5's srun no longer propagates
// cpus-per-task.
//
// TODO(nvhpc/mpirun alternative): the NVIDIA HPC SDK OpenMPI on MN5 ACC must be
// launched with `mpirun` INSIDE a single-node srun (not `srun --mpi=pmix`), and
// requires `export SLURM_CPU_BIND=none`. A future gang-mode value (e.g.
// "mpi-mpirun") can select that second shape: `srun --nodes=1 --ntasks=1 -w
// "$head_node" singularity exec $SIF mpirun -np $WORLD_SIZE <app>` with
// SLURM_CPU_BIND=none exported. Left as a documented TODO to avoid overbuilding;
// the pmix path is the clean HPC-native default.
func emitGangMPILauncher(bodyPtr *strings.Builder, head *BufferedMember, n int) {
	body := bodyPtr
	headOut := head.FilesPath + "/job.out"
	headErr := head.FilesPath + "/job.err"

	// Reconstruct the container invocation from the head's first (non-init)
	// runtime command, mirroring runCtn's assembly.
	invocation := gangReconstructContainerInvocation(head.RuntimeCommands)

	// Guard: a head with no non-init workload container would otherwise emit a
	// bare `srun ... \` with an empty command. Can't happen on the normal path
	// (the head always has a workload), but fail loudly rather than silently.
	if strings.TrimSpace(invocation) == "" {
		body.WriteString("echo 'gang MPI mode: head has no workload container to launch' >&2\n")
		body.WriteString("exit 1\n")
		return
	}

	body.WriteString("# --- classic-MPI launcher (interlink.eu/gang-mode=mpi): ONE srun over the whole allocation ---\n")
	body.WriteString("# One srun task per node == one MPI rank; PMIx does the rendezvous (no MASTER_ADDR needed).\n")
	// MN5: srun no longer propagates cpus-per-task; forward it explicitly.
	body.WriteString(`export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK}"` + "\n")
	body.WriteString("# Record the head compute node for shadow/tunnel discovery (best-effort).\n")
	body.WriteString("hostname -f > " + shellescape.Quote(head.FilesPath+"/compute-node") + " || true\n")
	body.WriteString(`srun --mpi="${SLURM_MPI_TYPE:-pmix}" --nodes="$WORLD_SIZE" --ntasks="$WORLD_SIZE" --ntasks-per-node=1 \` + "\n")
	body.WriteString("     -o " + shellescape.Quote(headOut) + " -e " + shellescape.Quote(headErr) + " --open-mode=truncate \\\n")
	body.WriteString("  " + invocation + "\n")
}

// emitLauncherCollective renders the COLLECTIVE injected-launcher section:
// launcher_main runs ONCE from the batch script (on node[0]) and itself fans out
// over the whole allocation (classic MPI openmpi/mpich, or the Ray cluster
// bootstrap). It mirrors emitGangMPILauncher's contract (the single launch's rc is
// the job rc; non-head members render no srun) but the launch SHAPE comes from the
// injected launcher_main instead of a hardcoded srun. The head's container-exec
// prefix is exported as GANG_CONTAINER and its app argv passed to launcher_main.
func emitLauncherCollective(bodyPtr *strings.Builder, head *BufferedMember, def *launcherDef, launcherPath string) {
	body := bodyPtr
	prefix, appArgs, _, ok := launcherWorkload(head.RuntimeCommands)
	if !ok {
		body.WriteString("echo 'gang launcher (collective): head has no workload container to launch' >&2\n")
		body.WriteString("exit 1\n")
		return
	}
	body.WriteString("# --- injected launcher '" + def.Name + "' (collective): launcher_main runs ONCE over the allocation ---\n")
	// MN5: srun no longer propagates cpus-per-task; forward it explicitly.
	body.WriteString(`export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK}"` + "\n")
	// GANG_CONTAINER = the singularity/enroot exec prefix (opts + SIF); the launcher
	// composes it with the app, e.g. `srun ... $GANG_CONTAINER "$@"` or
	// `mpirun ... $GANG_CONTAINER "$@"`. Used unquoted so it word-splits into argv.
	body.WriteString("export GANG_CONTAINER=" + shellescape.Quote(prefix) + "\n")
	body.WriteString("# Record the head compute node for shadow/tunnel discovery (best-effort).\n")
	body.WriteString("hostname -f > " + shellescape.Quote(head.FilesPath+"/compute-node") + " || true\n")
	body.WriteString("source " + shellescape.Quote(launcherPath) + "\n")
	body.WriteString("launcher_main " + shellescapeArgs(appArgs) + "\n")
	body.WriteString("LAUNCHER_RC=$?\n")
	body.WriteString("echo \"gang launcher '" + def.Name + "' exited rc=$LAUNCHER_RC\"\n")
	body.WriteString("exit \"$LAUNCHER_RC\"\n")
}

// emitLauncherPerRank renders the PER-RANK injected-launcher section: like
// emitGangPerRankLaunch (one `srun --overlap` per member, readiness barrier,
// DRIVER_RC = rank-0's rc), but each rank runs `source launcher.sh; launcher_main
// <app>` instead of the member's job.sh. Per-pod logs come from the srun -o/-e;
// per-pod status is written as run-<container>.status from launcher_main's rc so
// Status.go's gang branch still diverges per pod. GANG_NNODES/GANG_GPUS_PER_NODE and
// RANK/WORLD_SIZE/MASTER_* reach each rank via `srun --export` from the batch shell.
// NOTE: this path targets single-workload-container pods (the norm for distributed
// jobs); init containers / probes / multi-container pods should use the default
// (no-launcher) path, which runs the full job.sh.
func emitLauncherPerRank(bodyPtr *strings.Builder, ordered []*BufferedMember, def *launcherDef, launcherPath string, n, port int) {
	body := bodyPtr
	body.WriteString("declare -a RANK_PIDS=()\n\n")

	emitRank := func(m *BufferedMember) {
		node := "${nodes[" + strconv.Itoa(m.Rank) + "]}"
		out := m.FilesPath + "/job.out"
		errOut := m.FilesPath + "/job.err"
		prefix, appArgs, ctnName, ok := launcherWorkload(m.RuntimeCommands)
		if !ok {
			body.WriteString("# rank " + strconv.Itoa(m.Rank) + " (" + m.Role + ") pod " + m.PodUID + ": no workload container; skipping\n\n")
			return
		}
		statusFile := m.FilesPath + "/run-" + ctnName + ".status"
		// Inner (fresh srun sub-shell): set THIS member's container prefix, record its
		// compute node, source the launcher, run launcher_main with the app argv, and
		// persist rc as this rank's per-pod status file.
		inner := "export GANG_CONTAINER=" + shellescape.Quote(prefix) + "\n" +
			"hostname -f > " + shellescape.Quote(m.FilesPath+"/compute-node") + " || true\n" +
			"source " + shellescape.Quote(launcherPath) + "\n" +
			"launcher_main " + shellescapeArgs(appArgs) + "\n" +
			"RC=$?\n" +
			"echo \"$RC\" > " + shellescape.Quote(statusFile) + " || true\n" +
			"exit \"$RC\""
		body.WriteString("# rank " + strconv.Itoa(m.Rank) + " (" + m.Role + ") pod " + m.PodUID + " via launcher '" + def.Name + "'\n")
		body.WriteString("srun --overlap --nodes=1 --ntasks=1 -w \"" + node + "\" \\\n")
		body.WriteString("     --export=ALL,RANK=" + strconv.Itoa(m.Rank) + ",WORLD_SIZE=" + strconv.Itoa(n) +
			",MASTER_ADDR=\"$head_ip\",MASTER_PORT=" + strconv.Itoa(port) +
			",RAY_ADDRESS=\"$head_ip:" + strconv.Itoa(port) + "\" \\\n")
		body.WriteString("     -o " + shellescape.Quote(out) + " -e " + shellescape.Quote(errOut) + " --open-mode=truncate \\\n")
		body.WriteString("  bash -c " + shellescape.Quote(inner) + " &\n")
		body.WriteString("RANK_PIDS+=($!)\n\n")
	}

	// Head first, then a readiness barrier, then the workers (mirrors emitGangPerRankLaunch).
	emitRank(ordered[0])
	if n > 1 {
		body.WriteString("# readiness barrier: wait for the head coordinator before launching workers\n")
		body.WriteString("head_ready=0\n")
		body.WriteString("for i in $(seq 1 60); do\n")
		body.WriteString("  if srun --overlap --nodes=1 --ntasks=1 -w \"$head_node\" bash -c \"exec 3<>/dev/tcp/$head_ip/$MASTER_PORT\" >/dev/null 2>&1; then head_ready=1; break; fi\n")
		body.WriteString("  sleep 2\n")
		body.WriteString("done\n")
		body.WriteString("echo \"gang head_ready=$head_ready\"\n\n")
		for _, m := range ordered[1:] {
			emitRank(m)
		}
	}

	body.WriteString("# Wait for the head (rank 0 / driver) and capture its rc as the gang result.\n")
	body.WriteString("DRIVER_RC=0\n")
	body.WriteString("if [ \"${#RANK_PIDS[@]}\" -gt 0 ]; then\n")
	body.WriteString("  wait \"${RANK_PIDS[0]}\"; DRIVER_RC=$?\n")
	body.WriteString("fi\n")
	body.WriteString("for pid in \"${RANK_PIDS[@]:1}\"; do kill \"$pid\" 2>/dev/null || true; done\n")
	body.WriteString("wait 2>/dev/null || true\n")
	body.WriteString("echo \"DRIVER_RC=$DRIVER_RC\"\n")
	body.WriteString("exit \"$DRIVER_RC\"\n")
}

// gangReconstructContainerInvocation rebuilds the container command line the same
// way runCtn does in prepare.go: join rc.runtimeCommand with spaces (it already
// includes the singularity exec prefix + mounts/options + the SIF path as its
// last element), then append each containerCommand and containerArgs entry
// shellescape-quoted. It uses the FIRST non-init container command (the head's
// workload). Returns an empty string if there is no usable runtime command.
func gangReconstructContainerInvocation(rcs []ContainerCommand) string {
	var rc *ContainerCommand
	for i := range rcs {
		if !rcs[i].isInitContainer {
			rc = &rcs[i]
			break
		}
	}
	if rc == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(strings.Join(rc.runtimeCommand, " "))
	for _, c := range rc.containerCommand {
		b.WriteString(" ")
		b.WriteString(shellescape.Quote(c))
	}
	for _, a := range rc.containerArgs {
		b.WriteString(" ")
		b.WriteString(shellescape.Quote(a))
	}
	return b.String()
}

// gangMPIEnvExports parses the head pod's slurm-job.vk.io/mpi-env annotation
// ("K=V;K=V") into a slice of `export SINGULARITYENV_<K>=<shellescaped V>` lines.
// Empty or malformed pairs (no '=', empty key) are skipped. Returns nil when the
// annotation is absent or yields no valid pairs.
// isShellEnvName reports whether s is a valid POSIX shell/env variable name:
// [A-Za-z_][A-Za-z0-9_]*. Used to reject unsafe mpi-env keys before they are
// interpolated (unquoted) into an `export` statement.
func isShellEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
			// always allowed
		case i > 0 && r >= '0' && r <= '9':
			// digits allowed after the first char
		default:
			return false
		}
	}
	return true
}

func gangMPIEnvExports(metadata metav1.ObjectMeta) []string {
	raw := strings.TrimSpace(metadata.Annotations["slurm-job.vk.io/mpi-env"])
	if raw == "" {
		return nil
	}
	var out []string
	for _, pair := range strings.Split(raw, ";") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			continue
		}
		// The KEY is interpolated unquoted into `export SINGULARITYENV_<K>=...`, so
		// only accept valid shell env-var names ([A-Za-z_][A-Za-z0-9_]*). This drops
		// a hostile key like `X;rm -rf /` before it can break out of the export line
		// (the value is already shellescaped; this hardens the key side too).
		if !isShellEnvName(k) {
			continue
		}
		out = append(out, "export SINGULARITYENV_"+k+"="+shellescape.Quote(v))
	}
	return out
}

// slurmJobGone classifies one squeue probe of a submitted gang's JID and
// reports whether the job is DEFINITIVELY gone from SLURM. The decision is
// deliberately asymmetric: resubmitting over a live job is the worse failure
// mode (two allocations writing the same member dirs), so anything ambiguous
// reports NOT gone.
//   - stderr "Invalid job id" -> gone: the controller no longer knows the job
//     (it ran and aged out, exactly what a dead gang looks like after MinJobAge).
//   - any OTHER stderr (e.g. a transient SSH failure through the site's squeue
//     shim) -> NOT gone: indeterminate, never risk a double submit.
//   - clean run, empty output -> gone (with --states=all a live job always prints).
//   - terminal StateCompact code -> gone; live or unrecognized codes -> NOT gone.
func slurmJobGone(stdout, stderr string, exitCode int) bool {
	errS := strings.TrimSpace(stderr)
	if errS != "" {
		// rc-independent: squeue exits 1 alongside this message.
		return strings.Contains(errS, "Invalid job id")
	}
	// go-execute v1 returns err=nil for ANY nonzero exit (the failure lives only
	// in ExitCode), so a signal-killed ssh shim surfaces here as nonzero rc with
	// EMPTY stdout and stderr - maximally ambiguous. Without this gate the empty-
	// output branch below would declare a live gang gone and double-submit
	// (found by adversarial review, reproduced against go-execute v0.6.0). On a
	// nonzero exit, distrust stdout too (could be partial pipe output): only the
	// explicit Invalid-job-id marker above may declare gone.
	if exitCode != 0 {
		return false
	}
	out := strings.TrimSpace(stdout)
	if out == "" {
		return true
	}
	switch strings.Fields(out)[0] {
	// Unambiguously terminal squeue StateCompact codes. PR (preempted) is
	// deliberately absent: a preempt-requeued job returns to PD.
	case "CD", "CA", "F", "TO", "NF", "BF", "DL", "OOM", "SE":
		return true
	}
	return false
}

// gangJobIsGone asks SLURM whether the submitted gang JID still exists, using
// the same squeue invocation shape as Status.go. A failure to even run squeue
// reports NOT gone (see slurmJobGone's asymmetry rationale).
func gangJobIsGone(Ctx context.Context, config SlurmConfig, jid string) bool {
	shell := exec.ExecTask{
		Command: config.Squeuepath,
		Args:    []string{"--noheader", "-a", "--states=all", "-O", "StateCompact", "-j ", jid},
		Shell:   true,
	}
	execReturn, err := shell.Execute()
	if err != nil {
		log.G(Ctx).Warning("Gang staleness probe: could not run squeue for JID "+jid+": ", err, " (treating as alive)")
		return false
	}
	return slurmJobGone(execReturn.Stdout, execReturn.Stderr, execReturn.ExitCode)
}

// gangHeadsDirName is the well-known directory (under DataRootFolder) where a
// gang's batch script publishes the head node's hostname, keyed
// <namespace>-<gangname>. Shadow/tunnel pods resolve the head through it (they
// know their gang-name annotation but not any pod UID).
const gangHeadsDirName = ".gang-heads"

// gangMarkerFile is the per-member marker file that flags a pod dir as belonging
// to a co-scheduled gang. Its presence (checked by LoadJIDs and Status) is what
// enables the gang-only per-rank status divergence; single-pod jobs never write
// it, so their status behaviour is unchanged.
const gangMarkerFile = "gang.marker"

// readMemberContainerExitCode reads a gang member's OWN per-container exit code
// from run-<ctn>.status (or init-<ctn>.status) WITHOUT ever writing anything.
// It returns (exitCode, found, err):
//   - found=false, err=nil  : no status file yet -> this rank is still running.
//   - found=true            : the rank's container has terminated with exitCode.
//
// It deliberately does NOT reuse getExitCode: that helper, on a missing file,
// synthesises a status file from the shared squeue exit code, which for a still-
// Running gang would wrongly record a terminal state. This read-only variant is
// what lets a single failed rank surface as Failed on its own pod while the
// shared SLURM job (and the other ranks) keep running.
func readMemberContainerExitCode(path, ctName string) (int32, bool, error) {
	for _, p := range []string{path + "/run-" + ctName + ".status", path + "/init-" + ctName + ".status"} {
		raw, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, false, err
		}
		code, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			return 0, false, fmt.Errorf("parse %s: %w", p, err)
		}
		return int32(code), true, nil
	}
	return 0, false, nil
}

// stampJID mirrors handleJidAndPodUid's on-disk bookkeeping for one member so
// LoadJIDs can rehydrate every gang member on plugin restart. It writes
// JobID.jid / PodUID.uid / PodNamespace.ns into the member's OWN dir, writes the
// gang.marker (so this member is recognised as a gang member across restarts),
// and registers the JID in the shared JIDs map with Gang=true. The caller MUST
// hold h.GangMu.
func stampJID(Ctx context.Context, JIDs *map[string]*JidStruct, uid, ns, jid, dir string) error {
	if err := os.WriteFile(dir+"/JobID.jid", []byte(jid), 0o644); err != nil {
		log.G(Ctx).Error("Can't write JobID.jid for ", uid, ": ", err)
		return err
	}
	if err := os.WriteFile(dir+"/PodNamespace.ns", []byte(ns), 0o644); err != nil {
		log.G(Ctx).Error("Can't write PodNamespace.ns for ", uid, ": ", err)
		return err
	}
	if err := os.WriteFile(dir+"/PodUID.uid", []byte(uid), 0o644); err != nil {
		log.G(Ctx).Error("Can't write PodUID.uid for ", uid, ": ", err)
		return err
	}
	if err := os.WriteFile(dir+"/"+gangMarkerFile, []byte(jid), 0o644); err != nil {
		log.G(Ctx).Error("Can't write gang.marker for ", uid, ": ", err)
		return err
	}
	(*JIDs)[uid] = &JidStruct{PodUID: uid, PodNamespace: ns, JID: jid, Gang: true}
	return nil
}

// bufferGangMember buffers one rendered member and, if this arrival completes
// the gang, submits ONE sbatch for the whole group and back-fills every member's
// JID. It returns (jid, submitted, err):
//   - submitted=false, jid="" : buffered, gang not yet complete -> caller responds
//     200 with an empty PodJID (pod stays Pending until Status sees its JID).
//   - submitted=true,  jid=<gangJID> : gang complete and submitted; caller responds
//     200 with PodJID=jid for THIS pod (siblings reconcile via Status re-poll).
//
// The whole critical section is under h.GangMu, which also guards the JIDs writes.
func (h *SidecarHandler) bufferGangMember(Ctx context.Context, member *BufferedMember, size int) (string, bool, error) {
	gangName := member.Pod.ObjectMeta.Annotations[GangNameAnnotation]

	h.GangMu.Lock()
	defer h.GangMu.Unlock()

	// Opportunistically clear any timed-out incomplete gangs while we hold the lock.
	h.sweepGangsLocked(Ctx)

	if h.GangTable == nil {
		h.GangTable = make(map[string]*GangEntry)
	}
	entry, ok := h.GangTable[gangName]
	if !ok {
		entry = &GangEntry{
			Name:      gangName,
			Size:      size,
			Members:   make(map[string]*BufferedMember),
			CreatedAt: time.Now(),
		}
		h.GangTable[gangName] = entry
		log.G(Ctx).Infof("Gang '%s': created buffer, size=%d", gangName, size)
	}

	// Idempotency / re-Create safety: if this UID is already buffered, or the gang
	// is already submitted, do not double-count.
	if entry.Submitted {
		// A re-Create of a UID that WAS part of the submitted gang: idempotently
		// re-stamp and report the shared JID. No squeue probe here - the pod
		// belongs to THIS generation whatever state its job is in (Status.go owns
		// the terminal reporting).
		if _, known := entry.Members[member.PodUID]; known {
			if err := stampJID(Ctx, h.JIDs, member.PodUID, member.Namespace, entry.JID, member.FilesPath); err != nil {
				return "", false, err
			}
			log.G(Ctx).Infof("Gang '%s': already submitted, returning shared JID %s for re-Created member %s", gangName, entry.JID, member.PodUID)
			return entry.JID, true, nil
		}

		// A pod UID we have NEVER seen arrives for an already-submitted gang. Two
		// legitimate causes, distinguished by asking SLURM about the entry's JID:
		//   - the job is still ALIVE -> this is a replacement pod (the operator
		//     recreated one member while the allocation lives on); bind it to the
		//     live shared JID, never resubmit over a running gang;
		//   - the job is GONE (ran and aged out of the controller - the squeue
		//     probe echoes "Invalid job id") -> the whole workload was deleted and
		//     recreated under the same gang-name; the entry is STALE. Handing out
		//     the dead JID would pin every new pod Pending forever (field failure:
		//     BSC job 42973171). Drop the entry and buffer this member as the
		//     first of a FRESH generation.
		// The probe costs one squeue round-trip under GangMu, only on this rare
		// path (never on first-generation Creates), which also serializes
		// concurrent new-generation arrivals so exactly one of them rebuilds the
		// entry.
		if !gangJobIsGone(Ctx, h.Config, entry.JID) {
			entry.Members[member.PodUID] = member
			if err := stampJID(Ctx, h.JIDs, member.PodUID, member.Namespace, entry.JID, member.FilesPath); err != nil {
				return "", false, err
			}
			log.G(Ctx).Warningf("Gang '%s': new pod %s joined an already-submitted LIVE gang (JID %s); binding as replacement", gangName, member.PodUID, entry.JID)
			return entry.JID, true, nil
		}
		log.G(Ctx).Warningf("Gang '%s': submitted JID %s is gone from SLURM; dropping the stale entry and starting a fresh generation with pod %s", gangName, entry.JID, member.PodUID)
		entry = &GangEntry{
			Name:      gangName,
			Size:      size,
			Members:   make(map[string]*BufferedMember),
			CreatedAt: time.Now(),
		}
		h.GangTable[gangName] = entry
	}
	entry.Members[member.PodUID] = member

	if len(entry.Members) < entry.Size {
		log.G(Ctx).Infof("Gang '%s': buffered pod %s (%d/%d), waiting for quorum", gangName, member.PodUID, len(entry.Members), entry.Size)
		return "", false, nil
	}

	// Quorum reached: this is the last arrival. Submit ONE sbatch for the group.
	log.G(Ctx).Infof("Gang '%s': quorum reached (%d/%d), submitting single co-scheduled sbatch", gangName, len(entry.Members), entry.Size)

	ordered := assignRanks(entry.Members)
	scriptPath, err := produceGangSLURMScript(Ctx, h.Config, entry, ordered)
	if err != nil {
		return "", false, err
	}

	out, err := SLURMBatchSubmit(h.Ctx, h.Config, scriptPath)
	if err != nil {
		log.G(Ctx).Error("Gang '"+gangName+"': sbatch failed: ", err)
		return "", false, err
	}
	log.G(Ctx).Info(out)

	jid, err := parseJIDFromSubmit(out)
	if err != nil {
		return "", false, fmt.Errorf("gang %s: %w", gangName, err)
	}

	// Back-fill every member: register the shared JID in JIDs and persist the
	// per-pod bookkeeping files so LoadJIDs rehydrates all N on restart.
	for _, m := range ordered {
		if err := stampJID(Ctx, h.JIDs, m.PodUID, m.Namespace, jid, m.FilesPath); err != nil {
			return "", false, err
		}
	}
	entry.JID = jid
	entry.Submitted = true
	log.G(Ctx).Infof("Gang '%s': submitted as SLURM job %s (%d nodes)", gangName, jid, len(ordered))

	return jid, true, nil
}

// removeGangMemberOnDelete drops a deleted pod's UID from its gang entry and
// removes the entry once empty. Safe to call for non-gang pods (no-op if the UID
// is not tracked). Takes h.GangMu.
func (h *SidecarHandler) removeGangMemberOnDelete(Ctx context.Context, uid string) {
	h.GangMu.Lock()
	defer h.GangMu.Unlock()
	if h.GangTable == nil {
		return
	}
	for name, entry := range h.GangTable {
		if _, ok := entry.Members[uid]; ok {
			delete(entry.Members, uid)
			log.G(Ctx).Infof("Gang '%s': removed member %s on delete (%d remaining)", name, uid, len(entry.Members))
		}
		if len(entry.Members) == 0 {
			delete(h.GangTable, name)
			log.G(Ctx).Infof("Gang '%s': entry dropped (empty)", name)
		}
	}
}

// sweepGangsLocked abandons any incomplete gang whose buffering has exceeded the
// configured timeout: it removes every buffered member's dir and drops the entry.
// Caller MUST hold h.GangMu.
func (h *SidecarHandler) sweepGangsLocked(Ctx context.Context) {
	if h.GangTable == nil {
		return
	}
	timeout := gangGuaranteeTimeout(h.Config)
	for name, entry := range h.GangTable {
		if entry.Submitted {
			continue
		}
		if time.Since(entry.CreatedAt) <= timeout {
			continue
		}
		log.G(Ctx).Warningf("Gang '%s' timed out %d/%d after %s; abandoning buffered members", name, len(entry.Members), entry.Size, timeout)
		for _, m := range entry.Members {
			if err := os.RemoveAll(m.FilesPath); err != nil {
				log.G(Ctx).Debugf("Gang '%s': failed to remove %s: %v", name, m.FilesPath, err)
			}
		}
		delete(h.GangTable, name)
	}
}

// StartGangSweeper launches a background goroutine that periodically abandons
// timed-out incomplete gangs. Started from main.go only when gang scheduling is
// enabled. It exits when Ctx is cancelled.
func (h *SidecarHandler) StartGangSweeper(Ctx context.Context) {
	timeout := gangGuaranteeTimeout(h.Config)
	// Sweep a few times per timeout window; never faster than every 30s.
	interval := timeout / 4
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		log.G(Ctx).Infof("Gang sweeper started (timeout=%s, interval=%s)", timeout, interval)
		for {
			select {
			case <-Ctx.Done():
				log.G(Ctx).Info("Gang sweeper stopping")
				return
			case <-ticker.C:
				h.GangMu.Lock()
				h.sweepGangsLocked(Ctx)
				h.GangMu.Unlock()
			}
		}
	}()
}

// parseJIDFromSubmit extracts the numeric SLURM job ID from sbatch output.
// Mirrors the regex in handleJidAndPodUid.
func parseJIDFromSubmit(output string) (string, error) {
	m := jidSubmitRe.FindStringSubmatch(output)
	if len(m) < 2 {
		return "", fmt.Errorf("could not parse SLURM job ID from sbatch output %q", output)
	}
	return m[1], nil
}

// gangMemberFromCreate packages the data Create.go already rendered for one pod
// into a BufferedMember for the gang buffer.
func gangMemberFromCreate(
	pod v1.Pod,
	filesPath string,
	runtimeCommands []ContainerCommand,
	resourceLimits ResourceLimits,
	isDefaultCPU bool,
	isDefaultRam bool,
	flavor *FlavorResolution,
) *BufferedMember {
	return &BufferedMember{
		PodUID:          string(pod.UID),
		Namespace:       pod.Namespace,
		FilesPath:       filesPath,
		Role:            gangRoleFromMeta(pod.ObjectMeta),
		Pod:             pod,
		RuntimeCommands: runtimeCommands,
		ResourceLimits:  resourceLimits,
		IsDefaultCPU:    isDefaultCPU,
		IsDefaultRam:    isDefaultRam,
		Flavor:          flavor,
	}
}
