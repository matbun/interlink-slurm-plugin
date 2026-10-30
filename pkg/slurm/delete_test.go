package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Delete removes the socket directory the job's EXIT trap left behind, for example
// after a SIGKILL. It recomputes the path from the UID, and a directory that is
// already gone is not an error.
func TestStopHandlerRemovesSocketDir(t *testing.T) {
	for _, exists := range []bool{true, false} {
		root := t.TempDir()
		socketDir := socketDirFor(root, socketTestUID)
		if exists {
			if err := os.MkdirAll(socketDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(socketDir, "fwd-8888.sock"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}

		pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: socketTestPod, Namespace: metav1.NamespaceDefault, UID: socketTestUID}}
		body, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}

		jids := map[string]*JidStruct{}
		h := &SidecarHandler{
			Config: SlurmConfig{DataRootFolder: t.TempDir() + "/", SocketDirRoot: root},
			JIDs:   &jids,
			Ctx:    context.Background(),
		}
		rec := httptest.NewRecorder()
		h.StopHandler(rec, httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body)))

		if rec.Code != http.StatusOK {
			t.Errorf("StopHandler() with the directory present=%v: code %d, body %q", exists, rec.Code, rec.Body.String())
		}
		if _, err := os.Stat(socketDir); !os.IsNotExist(err) {
			t.Errorf("socket directory %s still there after delete (stat: %v)", socketDir, err)
		}
	}
}

// /delete of a gang member still in the buffer drops it: the group forgets it, its
// directory goes, nothing is cancelled, and the next members form the group afresh.
func TestStopHandlerDropsBufferedGangMember(t *testing.T) {
	dir := t.TempDir()
	scripts := filepath.Join(dir, "scripts")
	h := createHandler(t, writeStubScript(t, dir, "sbatch", "echo x >> "+scripts+"; echo 'Submitted batch job 42'"))
	h.Config.GangSchedulingEnabled = true
	h.Config.Scancelpath = writeStubScript(t, dir, "scancel", "echo \"$@\" >> "+filepath.Join(dir, "scancel.calls"))
	member := func(uid, rank string) v1.Pod {
		pod := plainPod(uid, map[string]string{PodGroupTotalCountAnnotation: "2"})
		pod.Labels = map[string]string{PodGroupNameLabel: "group", JobCompletionIndexLabel: rank}
		return pod
	}
	first := member("uid-g0", "0")
	rec := submitPod(t, h, first)
	var created CreateStruct
	if err := json.Unmarshal(rec.Body.Bytes(), &created); rec.Code != http.StatusOK || err != nil || created.PodJID != "" {
		t.Fatalf("buffered member: %d %q", rec.Code, rec.Body.String())
	}

	body, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	h.StopHandler(rec, httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %q", rec.Code, rec.Body.String())
	}
	if _, ok := h.GangTable["group"]; ok {
		t.Fatalf("the group still holds the deleted member: %+v", h.GangTable["group"].Members)
	}
	if _, err := os.Stat(h.Config.DataRootFolder + metav1.NamespaceDefault + "-uid-g0"); !os.IsNotExist(err) {
		t.Fatalf("the deleted member's directory is left: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "scancel.calls")); !os.IsNotExist(err) {
		t.Fatal("scancel ran for a member that had no job")
	}

	// The members sent again make one job of two, not one of three.
	for _, pod := range []v1.Pod{member("uid-h0", "0"), member("uid-h1", "1")} {
		if rec := submitPod(t, h, pod); rec.Code != http.StatusOK {
			t.Fatalf("member %s: %d %q", pod.UID, rec.Code, rec.Body.String())
		}
	}
	if raw, _ := os.ReadFile(scripts); strings.Count(string(raw), "x") != 1 {
		t.Fatalf("sbatch ran %d times, want once", strings.Count(string(raw), "x"))
	}
	if n := len(h.GangTable["group"].Members); n != 2 {
		t.Fatalf("the job holds %d members, want 2", n)
	}
}
