package slurm

import (
	"net/http"
	"os"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const workerCtn = "worker"

// twoPodSetRequest is a workload of a PodSet of drivers and one of workers, each
// with its own container.
func twoPodSetRequest(drivers, workers int32) SubmitRequest {
	req := submitRequest(drivers)
	req.PodSets[0].Name = "driver"
	req.PodSets[0].Template.Spec.Containers[0].Command = []string{"sh", "-c", "echo driver"}
	req.PodSets = append(req.PodSets, ExecutorPodSet{
		Name:      "worker",
		Count:     workers,
		Partition: req.PodSets[0].Partition,
		QOS:       req.PodSets[0].QOS,
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Name:    workerCtn,
			Image:   "docker://alpine",
			Command: []string{"sh", "-c", "echo worker"},
		}}}},
	})
	return req
}

func TestExecutorSeveralPodSetsAreOneGang(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(2, 1)
	e.submit(req)

	scripts := e.read("sbatch.scripts")
	if n := strings.Count(scripts, "#--- end of script"); n != 1 {
		t.Fatalf("sbatch ran %d times, want 1\n%s", n, scripts)
	}
	if !strings.Contains(scripts, "#SBATCH --nodes=3\n") {
		t.Fatalf("job script is not one allocation of 3 nodes\n%s", scripts)
	}
	for i, want := range []string{"echo driver", "echo driver", "echo worker"} {
		if !strings.Contains(scripts, e.memberDir(i)+"/job.sh") {
			t.Errorf("the gang script does not run member %d's job.sh", i)
		}
		jobSh, err := os.ReadFile(e.memberDir(i) + "/job.sh")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(jobSh), want) {
			t.Errorf("member %d's job.sh lacks %q\n%s", i, want, jobSh)
		}
	}
	env, err := os.ReadFile(e.memberDir(2) + "/" + workerCtn + "_envfile.properties")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(env), "INTERLINK_RANK=2\n") || !strings.Contains(string(env), "INTERLINK_WORLD_SIZE=3\n") {
		t.Errorf("member 2 env file:\n%s", env)
	}

	for i, want := range []struct{ podSet, role string }{{"driver", GangRoleHead}, {"driver", GangRoleWorker}, {"worker", GangRoleWorker}} {
		m := buildMember(req, i)
		if m.Labels[JobCompletionIndexLabel] != string(rune('0'+i)) || m.Annotations[PodSetAnnotation] != want.podSet ||
			m.Labels[PodGroupNameLabel] != executorWorkload || m.Annotations[PodGroupTotalCountAnnotation] != "3" ||
			m.Annotations[GangRoleAnnotation] != want.role || string(m.UID) != memberUID(executorWorkload, i) {
			t.Errorf("member %d: labels %v annotations %v", i, m.Labels, m.Annotations)
		}
	}

	if st := e.jobStatus(); len(st.Ranks) != 3 || st.Ranks[2].PodUID != memberUID(executorWorkload, 2) {
		t.Fatalf("ranks: %+v", st.Ranks)
	}

	// Each member's log is its own container's.
	e.writeLog(0, "zero\n")
	e.writeLog(1, "one\n")
	if err := os.WriteFile(e.memberDir(2)+"/run-"+workerCtn+".out", []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload, nil)
	if want := "[rank 0] zero\n[rank 1] one\n[rank 2] two\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("logs = %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&container="+workerCtn, nil)
	if want := "[rank 2] two\n"; rec.Body.String() != want {
		t.Fatalf("worker logs = %q, want %q", rec.Body.String(), want)
	}
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&container=nope", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown container = %d", rec.Code)
	}

	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d", rec.Code)
	}
	for i := 0; i < 3; i++ {
		if _, err := os.Stat(e.memberDir(i)); !os.IsNotExist(err) {
			t.Errorf("member %d directory left", i)
		}
	}
}

func TestExecutorPodSetsSizeTheNodeByTheLargest(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(1, 1)
	req.PodSets[0].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"), v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	req.PodSets[1].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("16"), v1.ResourceMemory: resource.MustParse("4Gi"), "nvidia.com/gpu": resource.MustParse("2"),
	}
	e.submit(req)
	scripts := e.read("sbatch.scripts")
	for _, want := range []string{"#SBATCH --cpus-per-task=16\n", "#SBATCH --mem=8192\n", "#SBATCH --gres=gpu:2\n"} {
		if !strings.Contains(scripts, want) {
			t.Errorf("job script lacks %q\n%s", want, scripts)
		}
	}
}

func TestExecutorPodSetsRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SubmitRequest)
		want   string
	}{
		{"different QOS", func(r *SubmitRequest) { r.PodSets[1].QOS = "prod" }, `QOS "prod" differs`},
		{"different partitions", func(r *SubmitRequest) { r.PodSets[1].Partition = "cpu" }, `partition "cpu" differs`},
		{"launcher with different images", func(r *SubmitRequest) {
			r.PodSets[0].Template.Spec.Containers[0].Command = r.PodSets[1].Template.Spec.Containers[0].Command
			r.PodSets[1].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"launcher with different commands", func(r *SubmitRequest) {
			r.PodSets[1].Template.Spec.Containers[0].Image = r.PodSets[0].Template.Spec.Containers[0].Image
			r.PodSets[0].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"too many members", func(r *SubmitRequest) { r.PodSets[0].Count, r.PodSets[1].Count = maxWorkloadMembers, 1 }, "more than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := twoPodSetRequest(1, 1)
			tt.mutate(&req)
			rec := e.do(http.MethodPost, "/submit", req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("code = %d %s", rec.Code, rec.Body.String())
			}
			if resp := decodeError(t, rec); !resp.Permanent || !strings.Contains(resp.Message, tt.want) {
				t.Fatalf("response = %+v, want a permanent %q", resp, tt.want)
			}
			if s := e.read("sbatch.scripts"); s != "" {
				t.Fatalf("sbatch ran")
			}
		})
	}

	// The same container in every PodSet runs under a launcher.
	req := twoPodSetRequest(1, 1)
	req.PodSets[1].Template.Spec.Containers[0] = *req.PodSets[0].Template.Spec.Containers[0].DeepCopy()
	req.PodSets[1].Template.ObjectMeta = metav1.ObjectMeta{Annotations: map[string]string{LauncherAnnotation: "mpi"}}
	if aerr := validatePodSetsShareAllocation(req.PodSets); aerr != nil {
		t.Fatalf("same container refused: %s", aerr.msg)
	}
}

// Each PodSet's references are read and resolved for its own members.
func TestExecutorPodSetsResolveTheirOwnReferences(t *testing.T) {
	e := newExecutorEnv(t, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("s3cr3t")}})
	req := twoPodSetRequest(1, 1)
	ps := &req.PodSets[1]
	ps.SecretRefs = []string{"hf-token"}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "HF_TOKEN", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{
		LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "token",
	}}}}
	e.submit(req)
	env, err := os.ReadFile(e.memberDir(1) + "/" + workerCtn + "_envfile.properties")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(env), "HF_TOKEN=s3cr3t") {
		t.Fatalf("worker env file:\n%s", env)
	}
}
