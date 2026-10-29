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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// A pod whose app containers set no CPU or memory gets its flavor's CPUDefault and
// MemoryDefault in job.slurm, not the 1 CPU / 1 MB fallback, and not the limits of an init
// container alone.
func TestSubmitHandlerFlavorDefaults(t *testing.T) {
	small := v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}
	for _, tc := range []struct {
		name string
		init []v1.Container
	}{
		{"no limits", nil},
		{"init container limits only", []v1.Container{{Name: "setup", Image: busyboxImage, Resources: v1.ResourceRequirements{Limits: small}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sbatch := filepath.Join(t.TempDir(), "sbatch")
			if err := os.WriteFile(sbatch, []byte("#!/bin/sh\necho 'Submitted batch job 42'\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			dataRoot := t.TempDir() + "/"
			data := commonIL.RetrievedPodData{
				Pod: v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: socketTestPod, Namespace: metav1.NamespaceDefault, UID: socketTestUID},
					Spec: v1.PodSpec{
						InitContainers: tc.init,
						Containers:     []v1.Container{{Name: "workload", Image: busyboxImage}},
					},
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
					Flavors:          map[string]FlavorConfig{"small": {Name: "small", CPUDefault: 4, MemoryDefault: "2G"}},
					DefaultFlavor:    "small",
				},
				JIDs: &jids,
				Ctx:  context.Background(),
			}
			rec := httptest.NewRecorder()
			h.SubmitHandler(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("SubmitHandler() code = %d, body %q", rec.Code, rec.Body.String())
			}

			raw, err := os.ReadFile(dataRoot + metav1.NamespaceDefault + "-" + socketTestUID + "/job.slurm")
			if err != nil {
				t.Fatal(err)
			}
			script := string(raw)
			for _, want := range []string{"#SBATCH --cpus-per-task=4\n", "#SBATCH --mem=2048\n"} {
				if !strings.Contains(script, want) {
					t.Errorf("job.slurm lacks %q:\n%s", want, script)
				}
			}
			for _, bad := range []string{"#SBATCH --cpus-per-task=1\n", "#SBATCH --mem=1\n", "#SBATCH --mem=1024\n"} {
				if strings.Contains(script, bad) {
					t.Errorf("job.slurm has %q:\n%s", bad, script)
				}
			}
		})
	}
}
