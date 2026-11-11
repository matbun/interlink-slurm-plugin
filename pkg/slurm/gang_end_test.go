package slurm

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeSrun runs a rank's `srun ... -o <out> -e <err> ... bash -c <inner>` on the
// local host, with the rank's output in its files.
const fakeSrun = `#!/bin/bash
out=/dev/null err=/dev/null
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift 2 ;;
    -e) err=$2; shift 2 ;;
    bash) break ;;
    *) shift ;;
  esac
done
exec "$@" >"$out" 2>"$err"
`

// With interlink.eu/gang-end: all, a gang without a launcher starts every rank
// at once, waits for each to end and fails with the first failure: a worker that
// outlives the head runs to its end instead of being stopped with it.
func TestGangEndAll(t *testing.T) {
	requireBash(t)
	dataRoot := t.TempDir() + string(os.PathSeparator)
	head := mkMember(t, dataRoot, "h", map[string]string{GangEndAnnotation: GangEndAll})
	worker := mkMember(t, dataRoot, "w", nil)
	_, ordered := gangOf("g1", head, worker)
	scripts := map[*BufferedMember]string{
		head:   "#!/bin/bash\necho head done\n",
		worker: "#!/bin/bash\nsleep 1\necho worker done\nexit 3\n",
	}
	for m, s := range scripts {
		if err := os.WriteFile(filepath.Join(m.WorkDir, "job.sh"), []byte(s), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "srun"), []byte(fakeSrun), 0o755); err != nil {
		t.Fatal(err)
	}

	var body strings.Builder
	body.WriteString("#!/bin/bash\nnodes=(n0 n1)\nhead_node=n0\nhead_ip=127.0.0.1\n")
	emitGangPerRankLaunch(&body, ordered, 2, 29500, "", true)
	if strings.Contains(body.String(), "readiness barrier") {
		t.Errorf("gang-end all waits for the head:\n%s", body.String())
	}
	script := filepath.Join(t.TempDir(), "launch.sh")
	if err := os.WriteFile(script, []byte(body.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("gang exit %v, want 3 (the worker's)\n%s", err, out)
	}
	for m, want := range map[*BufferedMember]string{head: "head done", worker: "worker done"} {
		b, _ := os.ReadFile(filepath.Join(m.WorkDir, "job.out"))
		if !strings.Contains(string(b), want) {
			t.Errorf("%s's job.out %q, want %q", m.PodUID, b, want)
		}
	}

	// The whole script takes the mode from the head's annotation
	entry, ordered := gangOf("g1", head, worker)
	if s := gangScript(t, SlurmConfig{BashPath: testBashPath}, entry, ordered); strings.Contains(s, "readiness barrier") || !strings.Contains(s, "GANG_RC") {
		t.Errorf("the gang script of a head with gang-end all:\n%s", s)
	}

	// Without the annotation the head drives: workers wait for it, then stop with it.
	body.Reset()
	emitGangPerRankLaunch(&body, ordered, 2, 29500, "", false)
	if s := body.String(); !strings.Contains(s, "readiness barrier") || !strings.Contains(s, "DRIVER_RC") {
		t.Errorf("the default launch lost its barrier or driver:\n%s", s)
	}
}
