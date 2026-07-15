package slurm

// Launcher injection.
//
// A gang pod may carry `interlink.eu/launcher: <name>`. When present, the gang
// plugin resolves `<name>.sh` from the launcher registry (SlurmConfig.
// LauncherRegistryPath, a mounted ConfigMap), validates it, materialises it next
// to job.slurm, and drives the launch through the shell function `launcher_main`
// the file defines. The launch shape (torchrun / mpirun / srun / ray-bootstrap +
// module loads + fabric/NUMA env) thus lives in a hot-reloadable ConfigMap instead
// of the plugin binary. Topology (per-rank vs collective) is declared IN the file.
//
// CRASH, DO NOT FALL BACK: when a launcher is explicitly requested, any problem
// (bad name, missing file, missing/ambiguous topology directive, missing
// launcher_main, `bash -n` failure) makes the gang submit fail loudly rather than
// silently reverting to a built-in default. A silent fallback would hide that the
// operator's launcher never ran. See interlink-vk/in-cluster-bsc/gang/CLAUDE.md.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/containerd/containerd/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"al.essio.dev/pkg/shellescape"
)

// Launcher topology values (declared per launcher via the topology directive).
const (
	// LauncherTopologyPerRank: launcher_main runs once PER RANK inside each
	// member's `srun --overlap` (Ray/torch env:///TF). Preserves per-pod logs+status.
	LauncherTopologyPerRank = "per-rank"
	// LauncherTopologyCollective: launcher_main runs ONCE from the batch script and
	// itself fans out over the whole allocation (classic MPI: openmpi/mpich, or the
	// Ray cluster bootstrap). Non-head members render no srun.
	LauncherTopologyCollective = "collective"
)

// launcherEntrypoint is the fixed shell function name every launcher file must define.
const launcherEntrypoint = "launcher_main"

// launcherFileName is written next to job.slurm (on the sshfs-mirrored jobs dir, so
// the HPC sees it at the same path) and `source`d by the launch section.
const launcherFileName = "launcher.sh"

var (
	// topologyDirectiveRe matches the required "# interlink-topology: <value>" line.
	topologyDirectiveRe = regexp.MustCompile(`(?m)^[ \t]*#[ \t]*interlink-topology:[ \t]*(per-rank|collective)[ \t]*$`)
	// launcherMainRe matches a bash definition of launcher_main
	// (`launcher_main() {` or `function launcher_main`).
	launcherMainRe = regexp.MustCompile(`(?m)^[ \t]*(function[ \t]+launcher_main\b|launcher_main[ \t]*\([ \t]*\))`)
	// launcherNameRe restricts launcher names to a safe filename token (no path sep,
	// no dot-dot). The <name>.sh file must live directly under the registry dir.
	launcherNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	// gresGPURe extracts N from --gres=gpu:N or --gres=gpu:<type>:N.
	gresGPURe = regexp.MustCompile(`^--gres=gpu(?::[A-Za-z0-9_.-]+)*:(\d+)$`)
	// gpusPerNodeFlagRe extracts N from --gpus-per-node=N.
	gpusPerNodeFlagRe = regexp.MustCompile(`^--gpus-per-node=(\d+)$`)
)

// launcherDef is a validated launcher ready to be materialised.
type launcherDef struct {
	Name     string
	Topology string // LauncherTopologyPerRank | LauncherTopologyCollective
	Body     string // full file contents, written verbatim to launcher.sh
}

// launcherRegistryDir returns the configured registry dir (never empty: func.go
// defaults it to /etc/interlink/launchers).
func launcherRegistryDir(config SlurmConfig) string {
	if d := strings.TrimSpace(config.LauncherRegistryPath); d != "" {
		return d
	}
	return "/etc/interlink/launchers"
}

// selectedLauncherName returns the launcher name requested by the pod via
// interlink.eu/launcher, or "" if none. The deprecated gang-mode alias is handled
// by the caller, not here.
func selectedLauncherName(metadata metav1.ObjectMeta) string {
	return strings.TrimSpace(metadata.Annotations[LauncherAnnotation])
}

// loadLauncher reads, validates, and returns the requested launcher. Read FRESH on
// every call (no caching) so a ConfigMap edit hot-reloads without a plugin restart.
// Returns an error on ANY problem (crash-no-fallback).
func loadLauncher(Ctx context.Context, config SlurmConfig, name string) (*launcherDef, error) {
	name = strings.TrimSpace(name)
	if !launcherNameRe.MatchString(name) {
		return nil, fmt.Errorf("invalid launcher name %q (allowed pattern %s)", name, launcherNameRe.String())
	}
	dir := filepath.Clean(launcherRegistryDir(config))
	path := filepath.Join(dir, name+".sh")
	// Defence-in-depth against traversal even though launcherNameRe forbids '/'.
	if path != filepath.Join(dir, name+".sh") || !strings.HasPrefix(path, dir+string(os.PathSeparator)) {
		return nil, fmt.Errorf("launcher %q resolves outside the registry dir %q", name, dir)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read launcher %q at %s: %w", name, path, err)
	}
	body := string(raw)

	m := topologyDirectiveRe.FindAllStringSubmatch(body, -1)
	if len(m) == 0 {
		return nil, fmt.Errorf("launcher %q (%s): missing '# interlink-topology: per-rank|collective' directive", name, path)
	}
	topology := m[0][1]
	for _, mm := range m[1:] {
		if mm[1] != topology {
			return nil, fmt.Errorf("launcher %q (%s): conflicting interlink-topology directives (%s vs %s)", name, path, topology, mm[1])
		}
	}
	if !launcherMainRe.MatchString(body) {
		return nil, fmt.Errorf("launcher %q (%s): does not define a %s function", name, path, launcherEntrypoint)
	}
	if err := bashSyntaxCheck(Ctx, path); err != nil {
		return nil, fmt.Errorf("launcher %q (%s): bash -n failed: %w", name, path, err)
	}

	log.G(Ctx).Infof("Gang launcher %q loaded (topology=%s) from %s", name, topology, path)
	return &launcherDef{Name: name, Topology: topology, Body: body}, nil
}

// bashSyntaxCheck runs `bash -n <path>` in the plugin container to reject a broken
// launcher before it ever reaches the HPC. A real syntax error is fatal
// (crash-no-fallback). If no bash binary can be found in the plugin container the
// check is SKIPPED with a loud warning (tooling gap, not a launcher defect): the
// launcher will still be re-parsed by the HPC shell via job.slurm's shebang.
func bashSyntaxCheck(Ctx context.Context, path string) error {
	var bin string
	for _, c := range []string{"bash", "/bin/bash", "/usr/bin/bash"} {
		if p, err := exec.LookPath(c); err == nil {
			bin = p
			break
		}
	}
	if bin == "" {
		log.G(Ctx).Warningf("launcher syntax check skipped: no bash found in plugin container; relying on HPC-side shebang for %s", path)
		return nil
	}
	out, err := exec.CommandContext(Ctx, bin, "-n", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// writeLauncherFile materialises def.Body to <headPath>/launcher.sh (0644) so both
// the batch script (collective) and each rank's `srun bash -c` (per-rank) can
// `source` it. It lives on the sshfs-mirrored jobs dir, so the path is identical on
// the HPC. Returns the absolute path.
func writeLauncherFile(headPath string, def *launcherDef) (string, error) {
	p := headPath + "/" + launcherFileName
	if err := os.WriteFile(p, []byte(def.Body), 0o644); err != nil {
		return "", fmt.Errorf("write launcher.sh: %w", err)
	}
	return p, nil
}

// gpusPerNodeFromFlags extracts the GPUs-per-node count the allocation grants, from
// the resolved sbatch flags: --gpus-per-node=N wins, else --gres=gpu[:type]:N.
// Returns 0 when no GPU flag is present. Used for the one-rank-per-GPU (gang6)
// ntasks-per-node and exported as GANG_GPUS_PER_NODE for the launcher.
func gpusPerNodeFromFlags(flags []string) int {
	for _, f := range flags {
		if m := gpusPerNodeFlagRe.FindStringSubmatch(strings.TrimSpace(f)); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
				return n
			}
		}
	}
	for _, f := range flags {
		if m := gresGPURe.FindStringSubmatch(strings.TrimSpace(f)); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// launcherWorkload extracts, from a member's rendered container commands, the pieces
// launcher_main is invoked with: the container-exec PREFIX (runtimeCommand joined,
// e.g. `singularity exec <opts> <SIF>`, exported as GANG_CONTAINER) and the APP
// argv (containerCommand + containerArgs, passed as launcher_main's positional args).
// It uses the FIRST non-init container. ok=false when there is no workload container.
func launcherWorkload(rcs []ContainerCommand) (prefix string, appArgs []string, ctnName string, ok bool) {
	for i := range rcs {
		if rcs[i].isInitContainer {
			continue
		}
		rc := &rcs[i]
		app := append([]string{}, rc.containerCommand...)
		app = append(app, rc.containerArgs...)
		return strings.Join(rc.runtimeCommand, " "), app, rc.containerName, true
	}
	return "", nil, "", false
}

// shellescapeArgs joins argv into a single shell-safe, space-separated string
// (each element quoted), for interpolation after `launcher_main`.
func shellescapeArgs(args []string) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = shellescape.Quote(a)
	}
	return strings.Join(parts, " ")
}
