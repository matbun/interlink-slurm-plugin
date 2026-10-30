package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
)

// SubmitHandler is what binds the socket directory into every container, outside
// prepareMounts and the env file; job.sh is where the runtime commands end up.
func TestSubmitHandlerBindsSocketDir(t *testing.T) {
	sbatch := filepath.Join(t.TempDir(), "sbatch")
	if err := os.WriteFile(sbatch, []byte("#!/bin/sh\necho 'Submitted batch job 42'\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		root string
		want string // empty: job.sh must not mention INTERLINK_SOCKET_DIR
	}{
		{
			name: "with a root",
			root: socketTestRoot,
			want: "--bind " + socketTestDir + ":" + socketTestDir + " --env INTERLINK_SOCKET_DIR=" + socketTestDir + " " + busyboxImage,
		},
		{name: "without a root"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataRoot := t.TempDir() + "/"
			data := commonIL.RetrievedPodData{
				Pod: v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: socketTestPod, Namespace: metav1.NamespaceDefault, UID: socketTestUID},
					Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "workload", Image: busyboxImage}}},
				},
			}
			body, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}

			jids := map[string]*JidStruct{}
			h := &SidecarHandler{
				Config: SlurmConfig{
					Sbatchpath:       sbatch,
					ContainerRuntime: singularityRuntime,
					SingularityPath:  singularityRuntime,
					DataRootFolder:   dataRoot,
					SocketDirRoot:    tt.root,
				},
				JIDs: &jids,
				Ctx:  context.Background(),
			}
			rec := httptest.NewRecorder()
			h.SubmitHandler(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))

			if rec.Code != http.StatusOK {
				t.Fatalf("SubmitHandler() code = %d, body %q", rec.Code, rec.Body.String())
			}
			raw, err := os.ReadFile(dataRoot + metav1.NamespaceDefault + "-" + socketTestUID + "/job.sh")
			if err != nil {
				t.Fatalf("read job.sh: %v", err)
			}
			jobSh := string(raw)
			if tt.want == "" {
				if strings.Contains(jobSh, "INTERLINK_SOCKET_DIR") {
					t.Errorf("job.sh must not bind a socket directory\n---\n%s", jobSh)
				}
				return
			}
			if !strings.Contains(jobSh, tt.want) {
				t.Errorf("job.sh lacks %q\n---\n%s", tt.want, jobSh)
			}
		})
	}
}

// A job script sent by the virtual kubelet is written to jobScript.sh in the job directory;
// the file must be closed, or a FUSE mount keeps it as .fuse_hidden* after the pod is deleted.
func TestSubmitHandlerClosesJobScript(t *testing.T) {
	if _, err := os.ReadDir("/proc/self/fd"); err != nil {
		t.Skip("needs /proc/self/fd")
	}
	sbatch := filepath.Join(t.TempDir(), "sbatch")
	if err := os.WriteFile(sbatch, []byte("#!/bin/sh\necho 'Submitted batch job 42'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dataRoot := t.TempDir() + "/"
	data := commonIL.RetrievedPodData{
		Pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: socketTestPod, Namespace: metav1.NamespaceDefault, UID: socketTestUID},
			Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "workload", Image: busyboxImage}}},
		},
		JobScript: "#!/bin/sh\necho hello\n",
	}
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	jids := map[string]*JidStruct{}
	h := &SidecarHandler{
		Config: SlurmConfig{
			Sbatchpath:       sbatch,
			ContainerRuntime: singularityRuntime,
			SingularityPath:  singularityRuntime,
			DataRootFolder:   dataRoot,
		},
		JIDs: &jids,
		Ctx:  context.Background(),
	}
	rec := httptest.NewRecorder()
	h.SubmitHandler(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("SubmitHandler() code = %d, body %q", rec.Code, rec.Body.String())
	}

	workDir := dataRoot + metav1.NamespaceDefault + "-" + socketTestUID
	if raw, err := os.ReadFile(workDir + "/jobScript.sh"); err != nil || string(raw) != data.JobScript {
		t.Fatalf("jobScript.sh = %q, %v; want %q", raw, err, data.JobScript)
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name())); err == nil && strings.HasPrefix(target, workDir+"/") {
			t.Errorf("left %s open (fd %s)", target, fd.Name())
		}
	}
}

func createHandler(t *testing.T, sbatch string) *SidecarHandler {
	t.Helper()
	jids := map[string]*JidStruct{}
	return &SidecarHandler{
		Config: SlurmConfig{
			Sbatchpath:       sbatch,
			ContainerRuntime: singularityRuntime,
			SingularityPath:  singularityRuntime,
			DataRootFolder:   t.TempDir() + "/",
			GangTimeout:      "10m",
		},
		JIDs:      &jids,
		Ctx:       context.Background(),
		GangTable: map[string]*GangEntry{},
	}
}

func plainPod(uid string, annotations map[string]string) v1.Pod {
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-" + uid, Namespace: metav1.NamespaceDefault, UID: types.UID(uid), Annotations: annotations},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "workload", Image: busyboxImage}}},
	}
}

// /create answers 503 for what trying again later can fix and 400 for a job Slurm
// refuses, whether one pod or the last member of a gang reaches sbatch.
func TestSubmitHandlerAnswersBySbatchRefusal(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		rc     int
		want   int
	}{
		{"ssh failed", "ssh: connect to host login.hpc-a.example.org port 22: Connection timed out", 255, http.StatusServiceUnavailable},
		{"QOS submit limit", "sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)", 1, http.StatusServiceUnavailable},
		{"association submit limit", "sbatch: error: AssocMaxSubmitJobLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)", 1, http.StatusServiceUnavailable},
		{"submit limit", "sbatch: error: MaxSubmitJobsLimit", 1, http.StatusServiceUnavailable},
		{"controller down", "sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)", 1, http.StatusServiceUnavailable},
		{"controller busy", "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation", 1, http.StatusServiceUnavailable},
		{"controller not accepting", "sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying", 1, http.StatusServiceUnavailable},
		{"invalid account", "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified", 1, http.StatusBadRequest},
		{"invalid partition", "sbatch: error: invalid partition specified: nope\nsbatch: error: Batch job submission failed: Invalid partition name specified", 1, http.StatusBadRequest},
		{"invalid QOS", "sbatch: error: Batch job submission failed: Invalid qos specification", 1, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "stderr"), []byte(tt.stderr+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			sbatch := writeStubScript(t, dir, "sbatch", "cat "+filepath.Join(dir, "stderr")+" >&2; exit "+strconv.Itoa(tt.rc))
			firstLine, _, _ := strings.Cut(tt.stderr, "\n")

			h := createHandler(t, sbatch)
			rec := submitPod(t, h, plainPod("uid-one", nil))
			if rec.Code != tt.want || !strings.Contains(rec.Body.String(), firstLine) {
				t.Fatalf("one pod: %d %q, want %d with sbatch's text", rec.Code, rec.Body.String(), tt.want)
			}
			if len(*h.JIDs) != 0 {
				t.Fatalf("a refused pod left a job: %v", *h.JIDs)
			}
			if _, err := os.Stat(h.Config.DataRootFolder + metav1.NamespaceDefault + "-uid-one"); !os.IsNotExist(err) {
				t.Fatalf("a refused pod left its directory: %v", err)
			}

			h = createHandler(t, sbatch)
			h.Config.GangSchedulingEnabled = true
			member := func(uid, rank string) v1.Pod {
				pod := plainPod(uid, map[string]string{PodGroupTotalCountAnnotation: "2"})
				pod.Labels = map[string]string{PodGroupNameLabel: "group", JobCompletionIndexLabel: rank}
				return pod
			}
			if rec := submitPod(t, h, member("uid-g0", "0")); rec.Code != http.StatusOK {
				t.Fatalf("first member: %d %q", rec.Code, rec.Body.String())
			}
			rec = submitPod(t, h, member("uid-g1", "1"))
			if rec.Code != tt.want || !strings.Contains(rec.Body.String(), firstLine) {
				t.Fatalf("last member of a gang: %d %q, want %d with sbatch's text", rec.Code, rec.Body.String(), tt.want)
			}
		})
	}
}

// The account, partition and QOS of the pod's sbatch flags end up in #SBATCH lines,
// so /create refuses what slurmNamePattern does not allow, before anything is written.
func TestSubmitHandlerChecksQueueFlags(t *testing.T) {
	tests := []struct {
		flags string
		want  int
	}{
		{"--account=proj42 --partition=debug --qos=prod --time=30", http.StatusOK},
		{"-A proj42 -p debug,prod -q prod_long --nodes 1", http.StatusOK},
		{"--account proj42.a-b", http.StatusOK},
		{`--account="proj 42"`, http.StatusBadRequest},
		{"--account=proj42;id", http.StatusBadRequest},
		{"-A $(id)", http.StatusBadRequest},
		{`--partition="debug prod"`, http.StatusBadRequest},
		{"-p debug/prod", http.StatusBadRequest},
		{"--qos=prod&x", http.StatusBadRequest},
		{"-q `id`", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.flags, func(t *testing.T) {
			dir := t.TempDir()
			ran := filepath.Join(dir, "ran")
			h := createHandler(t, writeStubScript(t, dir, "sbatch", "touch "+ran+"; echo 'Submitted batch job 42'"))
			rec := submitPod(t, h, plainPod("uid-flags", map[string]string{slurmFlagsAnnotation: tt.flags}))
			if rec.Code != tt.want {
				t.Fatalf("code = %d %q, want %d", rec.Code, rec.Body.String(), tt.want)
			}
			if tt.want == http.StatusOK {
				return
			}
			if _, err := os.Stat(ran); !os.IsNotExist(err) {
				t.Fatal("sbatch ran for refused flags")
			}
			if _, err := os.Stat(h.Config.DataRootFolder + metav1.NamespaceDefault + "-uid-flags"); !os.IsNotExist(err) {
				t.Fatalf("refused flags left the pod's directory: %v", err)
			}
		})
	}
}

// /create answers the last member of a group as /createPodGroup answers a group:
// 503 for what trying again later can fix and 400 for a job Slurm refuses, with
// sbatch's text. The member leaves the group with its directory; the members
// before it stay buffered.
func TestSubmitHandlerAnswersGroupBySbatchRefusal(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		rc     int
		want   int
	}{
		{"ssh failed", "ssh: connect to host login.hpc-a.example.org port 22: Connection timed out", 255, http.StatusServiceUnavailable},
		{"QOS submit limit", "sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)", 1, http.StatusServiceUnavailable},
		{"invalid account", "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified", 1, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "stderr"), []byte(tt.stderr+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			firstLine, _, _ := strings.Cut(tt.stderr, "\n")
			h, dataRoot, _, _ := newPodGroupHandler(t, "cat "+filepath.Join(dir, "stderr")+" >&2; exit "+strconv.Itoa(tt.rc))

			var created CreateStruct
			rec := submitPod(t, h, kueueMemberPod("uid-g0", "group", 0, 2))
			if err := json.Unmarshal(rec.Body.Bytes(), &created); rec.Code != http.StatusOK || err != nil || created.PodJID != "" {
				t.Fatalf("first member: %d %q", rec.Code, rec.Body.String())
			}
			rec = submitPod(t, h, kueueMemberPod("uid-g1", "group", 1, 2))
			if rec.Code != tt.want || !strings.Contains(rec.Body.String(), firstLine) {
				t.Fatalf("last member: %d %q, want %d with sbatch's text", rec.Code, rec.Body.String(), tt.want)
			}
			if _, err := os.Stat(memberDir(dataRoot, "uid-g1")); !os.IsNotExist(err) {
				t.Fatalf("the refused member's directory is left: %v", err)
			}
			if e := h.GangTable[gangKey(testNamespace, "group")]; e == nil || len(e.Members) != 1 || e.Members["uid-g0"] == nil {
				t.Fatalf("gang entry %+v, want uid-g0 still buffered", e)
			}
		})
	}
}

// A pod that names its group without the group's size is refused before anything
// is written; with gang scheduling off the same pod is one job.
func TestSubmitHandlerRefusesGroupWithoutSize(t *testing.T) {
	h, dataRoot, _, sbatchCount := newPodGroupHandler(t, "echo 'Submitted batch job 42'")
	pod := kueueMemberPod("uid-g0", "group", 0, 2)
	delete(pod.Annotations, PodGroupTotalCountAnnotation)

	if rec := submitPod(t, h, pod); rec.Code != http.StatusBadRequest {
		t.Fatalf("code %d %q, want 400", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(memberDir(dataRoot, "uid-g0")); !os.IsNotExist(err) {
		t.Fatalf("a refused pod left its directory: %v", err)
	}
	if n := countLines(t, sbatchCount); n != 0 || len(h.GangTable) != 0 {
		t.Fatalf("sbatch ran %d times, gangs %v", n, h.GangTable)
	}

	h.Config.GangSchedulingEnabled = false
	rec := submitPod(t, h, pod)
	var created CreateStruct
	if err := json.Unmarshal(rec.Body.Bytes(), &created); rec.Code != http.StatusOK || err != nil || created.PodJID != "42" {
		t.Fatalf("gang scheduling off: %d %q, want job 42", rec.Code, rec.Body.String())
	}
}
