package slurm

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// writeTestLauncher writes a minimal valid launcher (topology directive +
// launcher_main) into dir, returning nothing (fatal on error).
func writeTestLauncher(t *testing.T, dir, name, topology, body string) {
	t.Helper()
	content := "# interlink-topology: " + topology + "\nlauncher_main() {\n" + body + "\n}\n"
	if err := os.WriteFile(dir+"/"+name+".sh", []byte(content), 0o644); err != nil {
		t.Fatalf("write launcher %s: %v", name, err)
	}
}

func TestGpusPerNodeFromFlags(t *testing.T) {
	cases := []struct {
		flags []string
		want  int
	}{
		{[]string{"--gres=gpu:2"}, 2},
		{[]string{"--gres=gpu:h100:4"}, 4},
		{[]string{"--gpus-per-node=3", "--gres=gpu:1"}, 3}, // --gpus-per-node wins
		{[]string{"-A", "acct", "--gres=gpu:8", "-t", "01:00:00"}, 8},
		{[]string{"--cpus-per-task=40"}, 0}, // no GPU flag
		{nil, 0},
	}
	for _, c := range cases {
		if got := gpusPerNodeFromFlags(c.flags); got != c.want {
			t.Errorf("gpusPerNodeFromFlags(%v)=%d want %d", c.flags, got, c.want)
		}
	}
}

func TestLoadLauncherValidatesAndCrashesNoFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := SlurmConfig{LauncherRegistryPath: dir}
	ctx := context.Background()

	// valid collective launcher
	writeTestLauncher(t, dir, "good-col", "collective", "  echo hi")
	// valid per-rank launcher
	writeTestLauncher(t, dir, "good-pr", "per-rank", "  echo hi")
	// missing topology directive
	if err := os.WriteFile(dir+"/no-topo.sh", []byte("launcher_main() { echo x; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// missing launcher_main
	if err := os.WriteFile(dir+"/no-main.sh", []byte("# interlink-topology: per-rank\necho x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// broken bash
	if err := os.WriteFile(dir+"/broken.sh", []byte("# interlink-topology: per-rank\nlauncher_main() {\n  if [ ; then\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// conflicting topology directives
	if err := os.WriteFile(dir+"/conflict.sh", []byte("# interlink-topology: per-rank\n# interlink-topology: collective\nlauncher_main() { :; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if d, err := loadLauncher(ctx, cfg, "good-col"); err != nil || d.Topology != LauncherTopologyCollective {
		t.Errorf("good-col: got def=%+v err=%v; want collective, nil", d, err)
	}
	if d, err := loadLauncher(ctx, cfg, "good-pr"); err != nil || d.Topology != LauncherTopologyPerRank {
		t.Errorf("good-pr: got def=%+v err=%v; want per-rank, nil", d, err)
	}
	for _, bad := range []string{"no-topo", "no-main", "conflict", "does-not-exist", "../etc/passwd", "bad name", ""} {
		if _, err := loadLauncher(ctx, cfg, bad); err == nil {
			t.Errorf("loadLauncher(%q) must error (crash-no-fallback), got nil", bad)
		}
	}
	// broken bash: only fails when a bash binary is present to catch it (LookPath).
	if _, err := loadLauncher(ctx, cfg, "broken"); err == nil {
		if _, lp := lookBash(); lp {
			t.Errorf("broken.sh must fail bash -n when bash is available")
		}
	}
}

// lookBash reports whether a bash binary is on PATH (mirrors bashSyntaxCheck).
func lookBash() (string, bool) {
	for _, c := range []string{"bash", "/bin/bash", "/usr/bin/bash"} {
		if _, err := exec.LookPath(c); err == nil {
			return c, true
		}
	}
	return "", false
}

func TestProduceGangSLURMScriptLauncherCollective(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	reg := t.TempDir()
	writeTestLauncher(t, reg, "mpi-openmpi", "collective",
		`  local n=$(( GANG_NNODES * GANG_GPUS_PER_NODE ))`+"\n"+`  mpirun -n "$n" ${GANG_CONTAINER} "$@"`)
	config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity", LauncherRegistryPath: reg}
	extra := map[string]string{LauncherAnnotation: "mpi-openmpi", "slurm-job.vk.io/flags": "--gres=gpu:2"}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, extra, []string{"python"}, []string{"mpi_cpu.py"}),
		"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, extra, []string{"python"}, []string{"mpi_cpu.py"}),
	}}
	ordered := assignRanks(entry.Members)
	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(b)

	// launcher.sh materialised in the head dir with the body.
	lp := dataRoot + "default-h/" + launcherFileName
	if lb, err := os.ReadFile(lp); err != nil || !strings.Contains(string(lb), "launcher_main") {
		t.Errorf("launcher.sh not materialised at %s (err=%v)", lp, err)
	}
	for _, want := range []string{
		"source " + lp,               // sourced
		"launcher_main python mpi_cpu.py", // called with the app argv
		"export GANG_CONTAINER=",     // container prefix exported
		"export GANG_NNODES=2",
		"export GANG_GPUS_PER_NODE=2",
		"#SBATCH --ntasks-per-node=2", // gang6: one task per GPU for collective
		`export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK}"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("collective launcher script missing %q\n---\n%s", want, s)
		}
	}
	// collective: no per-rank loop, no readiness barrier, no built-in --mpi= launcher.
	for _, unwanted := range []string{"srun --overlap --nodes=1 --ntasks=1", "/dev/tcp/$head_ip/$MASTER_PORT", `--mpi="${SLURM_MPI_TYPE`} {
		if strings.Contains(s, unwanted) {
			t.Errorf("collective launcher must NOT contain %q\n---\n%s", unwanted, s)
		}
	}
}

func TestProduceGangSLURMScriptLauncherPerRank(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	reg := t.TempDir()
	writeTestLauncher(t, reg, "torchrun", "per-rank",
		`  ${GANG_CONTAINER} torchrun --node-rank="$RANK" --no-python "$@"`)
	config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity", LauncherRegistryPath: reg}
	// --gres=gpu:2 is present but per-rank must still pin --ntasks-per-node=1.
	extra := map[string]string{LauncherAnnotation: "torchrun", "slurm-job.vk.io/flags": "--gres=gpu:2"}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, extra, []string{"python"}, []string{"train.py"}),
		"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, extra, []string{"python"}, []string{"train.py"}),
	}}
	ordered := assignRanks(entry.Members)
	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, _ := os.ReadFile(path)
	s := string(b)

	// Per-rank loop present (2 members + barrier probe = 3 overlap sruns), header pins 1/node.
	if got := strings.Count(s, "srun --overlap --nodes=1 --ntasks=1"); got != 3 {
		t.Errorf("per-rank launcher must emit 3 overlap sruns (2 members + barrier), got %d\n---\n%s", got, s)
	}
	for _, want := range []string{
		"#SBATCH --ntasks-per-node=1", // per-rank ignores GPUs for tasks/node
		"source " + dataRoot + "default-h/" + launcherFileName,
		"launcher_main python train.py",
		"default-h/run-main.status", // per-pod status write
		"default-w/run-main.status",
		"/dev/tcp/$head_ip/$MASTER_PORT", // readiness barrier kept
		"exit \"$DRIVER_RC\"",            // DRIVER_RC tail kept
	} {
		if !strings.Contains(s, want) {
			t.Errorf("per-rank launcher script missing %q\n---\n%s", want, s)
		}
	}
}

func TestLauncherMissingRegistryCrashes(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	config := SlurmConfig{BashPath: "/bin/bash", LauncherRegistryPath: t.TempDir()} // empty registry
	extra := map[string]string{LauncherAnnotation: "does-not-exist"}
	entry := &GangEntry{Name: "g1", Size: 1, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 1, extra, []string{"python"}, []string{"x.py"}),
	}}
	ordered := assignRanks(entry.Members)
	if _, err := produceGangSLURMScript(context.Background(), config, entry, ordered); err == nil {
		t.Fatalf("a missing launcher must FAIL the submit (crash-no-fallback), got nil error")
	}
}

func TestLauncherAnnotationOverridesGangMode(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	reg := t.TempDir()
	writeTestLauncher(t, reg, "mpi-openmpi", "collective", `  mpirun ${GANG_CONTAINER} "$@"`)
	config := SlurmConfig{BashPath: "/bin/bash", LauncherRegistryPath: reg}
	// BOTH annotations set: launcher must win, gang-mode built-in must NOT fire.
	extra := map[string]string{LauncherAnnotation: "mpi-openmpi", GangModeAnnotation: GangModeMPI}
	entry := &GangEntry{Name: "g1", Size: 1, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 1, extra, []string{"python"}, []string{"x.py"}),
	}}
	ordered := assignRanks(entry.Members)
	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, _ := os.ReadFile(path)
	s := string(b)
	if !strings.Contains(s, "source "+dataRoot+"default-h/"+launcherFileName) {
		t.Errorf("launcher must win over gang-mode (expected the launcher to be sourced)\n---\n%s", s)
	}
	if strings.Contains(s, `--mpi="${SLURM_MPI_TYPE`) {
		t.Errorf("built-in gang-mode MPI launcher must NOT fire when interlink.eu/launcher is set\n---\n%s", s)
	}
}
