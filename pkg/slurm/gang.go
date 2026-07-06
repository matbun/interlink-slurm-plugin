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

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"al.essio.dev/pkg/shellescape"
)

// gangCoordinationPort is the coordination port exported to every rank as
// MASTER_PORT / the Ray GCS port. It mirrors PORT=7639 from the validated
// 03-nodesN-ray.slurm twin. It is fixed for now; a future change can make it
// per-gang / annotation-driven.
const gangCoordinationPort = 7639

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

	var hdr strings.Builder
	hdr.WriteString("#!" + config.BashPath + "\n")
	hdr.WriteString("#SBATCH --job-name=" + entry.Name + "\n")
	hdr.WriteString("#SBATCH --nodes=" + strconv.Itoa(n) + "\n")
	hdr.WriteString("#SBATCH --ntasks-per-node=1\n")
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

	port := gangCoordinationPort

	// Resolve the allocation's nodes and the head IP (copied from the twin).
	body.WriteString(`mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")` + "\n")
	body.WriteString(`head_node="${nodes[0]}"` + "\n")
	body.WriteString(`head_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address | awk '{print $1}')` + "\n")
	body.WriteString("export MASTER_ADDR=\"$head_ip\"\n")
	body.WriteString("export MASTER_PORT=" + strconv.Itoa(port) + "\n")
	body.WriteString("export WORLD_SIZE=" + strconv.Itoa(n) + "\n")
	body.WriteString("export RAY_ADDRESS=\"$head_ip:" + strconv.Itoa(port) + "\"\n")
	body.WriteString(`echo "gang " ` + shellescape.Quote(entry.Name) + ` " head_node=$head_node head_ip=$head_ip world_size=` + strconv.Itoa(n) + `"` + "\n\n")

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
		inner := fmt.Sprintf(
			"export RANK=%d WORLD_SIZE=%d MASTER_ADDR=\"$head_ip\" MASTER_PORT=%d RAY_ADDRESS=\"$head_ip:%d\"; exec %s",
			m.Rank, n, port, port, shellescape.Quote(memberScript),
		)
		body.WriteString("# rank " + strconv.Itoa(m.Rank) + " (" + m.Role + ") pod " + m.PodUID + "\n")
		body.WriteString("srun --overlap --nodes=1 --ntasks=1 -w \"" + node + "\" \\\n")
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
		body.WriteString("  if srun --overlap --nodes=1 --ntasks=1 -w \"$head_node\" bash -c 'command -v ray >/dev/null 2>&1 && ray status --address \"$RAY_ADDRESS\"' >/dev/null 2>&1; then head_ready=1; break; fi\n")
		body.WriteString("  # If ray is unavailable, do not block non-Ray gangs.\n")
		body.WriteString("  if ! srun --overlap --nodes=1 --ntasks=1 -w \"$head_node\" bash -c 'command -v ray >/dev/null 2>&1' >/dev/null 2>&1; then head_ready=1; break; fi\n")
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
		// The gang already went out. Register this UID against the shared JID (in
		// case a re-Create arrives for an already-submitted gang) and report it.
		if err := stampJID(Ctx, h.JIDs, member.PodUID, member.Namespace, entry.JID, member.FilesPath); err != nil {
			return "", false, err
		}
		log.G(Ctx).Infof("Gang '%s': already submitted, returning shared JID %s for pod %s", gangName, entry.JID, member.PodUID)
		return entry.JID, true, nil
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
