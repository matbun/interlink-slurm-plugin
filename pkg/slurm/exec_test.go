package slurm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"
)

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

type fakeAttach struct {
	in     io.Reader
	out    *bytes.Buffer
	tty    bool
	resize chan vkapi.TermSize
}

func (f *fakeAttach) Stdin() io.Reader              { return f.in }
func (f *fakeAttach) Stdout() io.WriteCloser        { return nopCloser{f.out} }
func (f *fakeAttach) Stderr() io.WriteCloser        { return nopCloser{f.out} }
func (f *fakeAttach) TTY() bool                     { return f.tty }
func (f *fakeAttach) Resize() <-chan vkapi.TermSize { return f.resize }

// The exec script runs the container's runtime command in the job's
// directory, with exec in place of run and the command given last.
func TestWriteExecScript(t *testing.T) {
	dir := t.TempDir()
	cfg := SlurmConfig{ContainerRuntime: "singularity", ExecPrefix: []string{"/opt/shims/node-exec"}, Commandprefix: "module load singularity"}
	rc := []string{"singularity", "run", "--containall", "", "--env-file /w/main.env", "--bind /w/a:/a", "docker://python:3.11"}
	if err := writeExecScript(cfg, dir, "/w", "main", rc); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "exec-main.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"\ncd /w || exit 1\nmodule load singularity\nexec singularity exec --containall  --env-file /w/main.env --bind /w/a:/a docker://python:3.11 \"$@\"\n"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("script %q lacks %q", b, want)
		}
	}
	// no ExecPrefix: no script
	other := t.TempDir()
	if err := writeExecScript(SlurmConfig{}, other, "/w", "main", rc); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(other, "exec-main.sh")); err == nil {
		t.Error("a script without ExecPrefix")
	}
}

// The prefix gets the node and one shell line that runs the command through
// the script; the exec's stdin reaches it, and its exit code is the command's.
func TestRunExec(t *testing.T) {
	h := &SidecarHandler{Config: SlurmConfig{ExecPrefix: []string{"/bin/sh", "-c", `echo "node=$1 tty=$EXEC_TTY"; echo "$2"; cat`, "sh"}}}
	f := &fakeAttach{in: strings.NewReader("from stdin\n"), out: &bytes.Buffer{}}
	if err := h.runExec(context.Background(), "as01r1b16", "/d/ns-uid/exec-main.sh", []string{"echo", "hi there"}, f); err != nil {
		t.Fatal(err)
	}
	if want := "node=as01r1b16 tty=0\nexec bash /d/ns-uid/exec-main.sh echo 'hi there'\nfrom stdin\n"; f.out.String() != want {
		t.Errorf("output %q, want %q", f.out.String(), want)
	}
	// a terminal: EXEC_TTY=1 and its first size set before the command
	f = &fakeAttach{out: &bytes.Buffer{}, tty: true, resize: make(chan vkapi.TermSize, 1)}
	f.resize <- vkapi.TermSize{Width: 120, Height: 40}
	if err := h.runExec(context.Background(), "n1", "/s.sh", []string{"bash"}, f); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.out.String(), "tty=1\nstty cols 120 rows 40 2>/dev/null; exec bash /s.sh bash\n") {
		t.Errorf("tty output %q", f.out.String())
	}
	h.Config.ExecPrefix = []string{"/bin/sh", "-c", "exit 3", "sh"}
	err := h.runExec(context.Background(), "n1", "/s.sh", []string{"false"}, &fakeAttach{out: &bytes.Buffer{}})
	var code utilexec.CodeExitError
	if !errors.As(err, &code) || code.Code != 3 {
		t.Errorf("err %v, want exit code 3", err)
	}
}

func TestExecHandlerRefuses(t *testing.T) {
	data := t.TempDir() + "/"
	for _, tc := range []struct {
		name   string
		prefix []string
		query  string
		code   int
	}{
		{"no prefix", nil, "namespace=ns&pod=uid-1&container=main&node=n1&command=ls&output=1", http.StatusNotImplemented},
		{"no node", []string{"/bin/true"}, "namespace=ns&pod=uid-1&container=main&command=ls&output=1", http.StatusBadRequest},
		{"a node that is no host name", []string{"/bin/true"}, "namespace=ns&pod=uid-1&container=main&node=n1;rm&command=ls&output=1", http.StatusBadRequest},
		{"no script", []string{"/bin/true"}, "namespace=ns&pod=uid-1&container=main&node=n1&command=ls&output=1", http.StatusNotFound},
	} {
		h := &SidecarHandler{Config: SlurmConfig{ExecPrefix: tc.prefix, DataRootFolder: data}}
		w := httptest.NewRecorder()
		h.ExecHandler(w, httptest.NewRequest(http.MethodPost, "/exec?"+tc.query, nil))
		if w.Code != tc.code {
			t.Errorf("%s: %d %q, want %d", tc.name, w.Code, w.Body.String(), tc.code)
		}
	}
}

func TestPluginCapabilitiesExec(t *testing.T) {
	if caps := pluginCapabilities(SlurmConfig{}); strings.Contains(strings.Join(caps, ","), CapabilityExec) {
		t.Errorf("caps %v without ExecPrefix", caps)
	}
	if caps := pluginCapabilities(SlurmConfig{ExecPrefix: []string{"x"}, GangSchedulingEnabled: true}); caps[len(caps)-1] != CapabilityExec || len(caps) != 4 {
		t.Errorf("caps %v, want the gang ones and exec", caps)
	}
}
