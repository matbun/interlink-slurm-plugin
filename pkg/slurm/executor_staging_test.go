package slurm

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
)

const (
	stageInJob  = "stage-in-0b6c1d2e"
	stageOutJob = "stage-out-0b6c1d2e"
)

func (e *executorEnv) enableStaging() {
	e.d.Sidecar.Config.Staging = StagingConfig{
		Enabled:             true,
		Image:               "registry.a.example.org/stage:1",
		Command:             defaultStageCommand,
		TransferHost:        "transfer.hpc-a.example.org",
		StageRoot:           "/scratch/proj42/stage",
		KeySecret:           "hpc-a-key",
		KeySecretKey:        "ssh-privatekey",
		KnownHostsConfigMap: "hpc-a-known-hosts",
		Volumes: []StagingVolume{
			{Name: "eos", ClaimName: "eos-claim", MountPath: "/eos", Kerberos: true},
			{Name: "shared", ClaimName: "shared-claim", MountPath: "/shared"},
		},
		Kerberos: &StagingKerberos{KeytabSecret: "alice-keytab", KeytabKey: "keytab", Principal: "alice@A.EXAMPLE.ORG", Krb5ConfigMap: "krb5"},
	}
}

func stagedRequest(annotation, paths string) SubmitRequest {
	req := submitRequest(1)
	req.PodSets[0].Template.Annotations = map[string]string{annotation: paths}
	return req
}

func (e *executorEnv) stageJob(name string) *batchv1.Job {
	e.t.Helper()
	job, err := e.kube.BatchV1().Jobs(executorNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		e.t.Fatal(err)
	}
	return job
}

// endStageJob sets the Job's terminal condition, as the Job controller would.
func (e *executorEnv) endStageJob(name string, cond batchv1.JobConditionType, reason string) {
	e.t.Helper()
	job := e.stageJob(name)
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: cond, Status: v1.ConditionTrue, Reason: reason, Message: "from the test"})
	if _, err := e.kube.BatchV1().Jobs(executorNS).UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *executorEnv) countActions(verb, resource string) int {
	n := 0
	for _, a := range e.kube.Actions() {
		if a.GetVerb() == verb && a.GetResource().Resource == resource {
			n++
		}
	}
	return n
}

func envOf(c v1.Container) map[string]string {
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	return env
}

func TestExecutorStageIn(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	req := stagedRequest(StageInAnnotation, "/shared/alice/data:/eos/user/a/alice/model/")

	for range 2 {
		rec := e.do(http.MethodPost, "/submit", req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("submit while staging = %d %s", rec.Code, rec.Body.String())
		}
		if resp := decodeError(t, rec); resp.Permanent || !strings.Contains(resp.Message, "staging in") {
			t.Fatalf("response = %+v", resp)
		}
	}
	if n := e.countActions("create", "jobs"); n != 1 {
		t.Fatalf("%d stage Jobs created, want 1", n)
	}
	if s := e.read("sbatch.scripts"); s != "" {
		t.Fatalf("sbatch ran before the stage-in ended")
	}

	job := e.stageJob(stageInJob)
	pod := job.Spec.Template.Spec
	if job.Labels[StageOfLabel] != executorWorkload || job.Labels[StageDirectionLabel] != "in" ||
		job.Spec.Template.Labels["sidecar.istio.io/inject"] != "false" || *job.Spec.BackoffLimit != 0 || pod.RestartPolicy != v1.RestartPolicyNever {
		t.Fatalf("stage Job: %+v", job)
	}
	c := pod.Containers[0]
	env := envOf(c)
	for k, want := range map[string]string{
		"STAGE_DIRECTION": "in",
		"STAGE_PATHS":     "/shared/alice/data:/eos/user/a/alice/model/",
		"REMOTE":          "alice@transfer.hpc-a.example.org",
		"STAGE_ROOT":      "/scratch/proj42/stage",
		"STAGE_NAMESPACE": executorNS,
		"SSH_KEY":         "/ssh/ssh-privatekey",
		"KEYTAB":          "/keytab/keytab",
		"PRINCIPAL":       "alice@A.EXAMPLE.ORG",
		"KRB5CCNAME":      "FILE:/tmp/krb5cc",
	} {
		if env[k] != want {
			t.Errorf("env %s = %q, want %q", k, env[k], want)
		}
	}
	if c.Image != "registry.a.example.org/stage:1" || strings.Join(c.Command, " ") != defaultStageCommand {
		t.Errorf("container %s %v", c.Image, c.Command)
	}
	if c.Resources.Requests.Cpu().String() != "50m" || c.Resources.Requests.Memory().String() != "64Mi" ||
		c.Resources.Limits.Cpu().String() != "2" || c.Resources.Limits.Memory().String() != "2Gi" {
		t.Errorf("resources %+v", c.Resources)
	}
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.MountPath] = m.Name
	}
	claims := map[string]string{}
	for _, v := range pod.Volumes {
		if v.PersistentVolumeClaim != nil {
			claims[v.Name] = v.PersistentVolumeClaim.ClaimName
		}
	}
	if mounts["/eos"] != "eos" || mounts["/shared"] != "shared" || claims["eos"] != "eos-claim" || claims["shared"] != "shared-claim" || mounts["/keytab"] == "" || mounts["/ssh"] == "" {
		t.Errorf("mounts %v claims %v", mounts, claims)
	}

	e.endStageJob(stageInJob, batchv1.JobComplete, "")
	if resp := e.submit(req); resp.JobID != executorJobID {
		t.Fatalf("submit after the stage-in = %+v", resp)
	}
	if n := e.countActions("create", "jobs"); n != 1 {
		t.Fatalf("%d stage Jobs created, want 1", n)
	}
}

func TestExecutorStageInWithoutKerberos(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	e.do(http.MethodPost, "/submit", stagedRequest(StageInAnnotation, "/shared/alice/data"))
	job := e.stageJob(stageInJob)
	env := envOf(job.Spec.Template.Spec.Containers[0])
	if _, ok := env["KEYTAB"]; ok {
		t.Errorf("a path off the Kerberos volume got a keytab: %v", env)
	}
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "eos" || v.Name == "stage-keytab" {
			t.Errorf("volume %s mounted for a path on /shared only", v.Name)
		}
	}
}

func TestExecutorStageInFailed(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	req := stagedRequest(StageInAnnotation, "/shared/alice/data")
	e.do(http.MethodPost, "/submit", req)
	e.endStageJob(stageInJob, batchv1.JobFailed, "BackoffLimitExceeded")
	rec := e.do(http.MethodPost, "/submit", req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("submit after a failed stage-in = %d %s", rec.Code, rec.Body.String())
	}
	if resp := decodeError(t, rec); !resp.Permanent || !strings.Contains(resp.Message, "staging in failed") || !strings.Contains(resp.Message, "BackoffLimitExceeded") {
		t.Fatalf("response = %+v", resp)
	}
	if s := e.read("sbatch.scripts"); s != "" {
		t.Fatalf("sbatch ran")
	}
}

func TestExecutorStagePathRefusals(t *testing.T) {
	tests := []struct {
		name, annotation, paths, want string
		disabled                      bool
	}{
		{"outside the volumes", StageInAnnotation, "/shared/alice/data:/home/alice/data", `"/home/alice/data" is not under a staging volume`, false},
		{"the volume itself", StageOutAnnotation, "/shared", `"/shared" is not under a staging volume`, false},
		{"a prefix of a mount path", StageInAnnotation, "/sharedx/data", "is not under a staging volume", false},
		{"relative", StageInAnnotation, "shared/data", "not a clean absolute path", false},
		{"dot dot", StageOutAnnotation, "/shared/../etc", "not a clean absolute path", false},
		{"staging off", StageInAnnotation, "/shared/alice/data", "stages nothing", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			if !tt.disabled {
				e.enableStaging()
			}
			rec := e.do(http.MethodPost, "/submit", stagedRequest(tt.annotation, tt.paths))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("code = %d %s", rec.Code, rec.Body.String())
			}
			if resp := decodeError(t, rec); !resp.Permanent || !strings.Contains(resp.Message, tt.want) {
				t.Fatalf("response = %+v, want a permanent %q", resp, tt.want)
			}
			if n := e.countActions("create", "jobs"); n != 0 {
				t.Fatalf("a stage Job was created")
			}
		})
	}
}

func (e *executorEnv) endJob(exitCode string) {
	e.t.Helper()
	e.setState("CD None", exitCode+" CD")
	if err := os.WriteFile(e.memberDir(0)+"/run-"+executorCtn+".status", []byte(exitCode+"\n"), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func TestExecutorStageOut(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	e.submit(stagedRequest(StageOutAnnotation, "/shared/alice/out"))

	e.setState("R None", "0 R")
	if st := e.jobStatus(); st.State != JobRunning || e.countActions("create", "jobs") != 0 {
		t.Fatalf("running job: %+v, %d Job(s) created", st, e.countActions("create", "jobs"))
	}

	e.endJob("0")
	for range 2 {
		st := e.jobStatus()
		if st.State != JobRunning || !strings.HasPrefix(st.Message, "staging out") || st.CompletionTime != nil {
			t.Fatalf("while staging out: %+v", st)
		}
	}
	if n := e.countActions("create", "jobs"); n != 1 {
		t.Fatalf("%d stage Jobs created, want 1", n)
	}
	job := e.stageJob(stageOutJob)
	if env := envOf(job.Spec.Template.Spec.Containers[0]); env["STAGE_DIRECTION"] != "out" || env["STAGE_PATHS"] != "/shared/alice/out" || job.Labels[StageDirectionLabel] != "out" {
		t.Fatalf("stage-out Job: %+v", job)
	}

	e.endStageJob(stageOutJob, batchv1.JobComplete, "")
	if st := e.jobStatus(); st.State != JobCompleted || st.CompletionTime == nil {
		t.Fatalf("after the stage-out: %+v", st)
	}
}

func TestExecutorStageOutFailed(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	e.submit(stagedRequest(StageOutAnnotation, "/shared/alice/out"))
	e.endJob("0")
	e.jobStatus()
	e.endStageJob(stageOutJob, batchv1.JobFailed, "BackoffLimitExceeded")
	st := e.jobStatus()
	if st.State != JobFailed || st.Reason != "StageOutFailed" || !strings.Contains(st.Message, "staging out failed") || !strings.Contains(st.Message, "BackoffLimitExceeded") {
		t.Fatalf("after a failed stage-out: %+v", st)
	}
}

func TestExecutorDeleteRemovesStageJobs(t *testing.T) {
	other := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "stage-in-11111111", Namespace: executorNS, Labels: map[string]string{StageOfLabel: "11111111-other"}}}
	e := newExecutorEnv(t, other)
	e.enableStaging()
	req := stagedRequest(StageInAnnotation, "/shared/alice/data")
	req.PodSets[0].Template.Annotations[StageOutAnnotation] = "/shared/alice/out"
	e.do(http.MethodPost, "/submit", req)
	e.endStageJob(stageInJob, batchv1.JobComplete, "")
	e.submit(req)
	e.endJob("0")
	e.jobStatus()

	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d %s", rec.Code, rec.Body.String())
	}
	jobs, err := e.kube.BatchV1().Jobs(executorNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Name != other.Name {
		t.Fatalf("Jobs left: %+v", jobs.Items)
	}
	deletes := 0
	for _, a := range e.kube.Actions() {
		if d, ok := a.(k8stesting.DeleteAction); ok && a.GetResource().Resource == "jobs" {
			deletes++
			if p := d.GetDeleteOptions().PropagationPolicy; p == nil || *p != metav1.DeletePropagationBackground {
				t.Errorf("delete of %s without Background propagation", d.GetName())
			}
		}
	}
	if deletes != 2 {
		t.Fatalf("%d Job deletes, want 2", deletes)
	}
}

// A workload still staging in has no record; its DELETE removes the stage Job.
func TestExecutorDeleteWhileStagingIn(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	e.do(http.MethodPost, "/submit", stagedRequest(StageInAnnotation, "/shared/alice/data"))
	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d %s", rec.Code, rec.Body.String())
	}
	if _, err := e.kube.BatchV1().Jobs(executorNS).Get(context.Background(), stageInJob, metav1.GetOptions{}); err == nil {
		t.Fatalf("stage-in Job left")
	}
}

func TestValidateStagingConfig(t *testing.T) {
	base := func() SlurmConfig {
		return SlurmConfig{
			Shadow: ShadowConfig{KeySecret: "hpc-a-tunnel-key", KnownHostsConfigMap: "hpc-a-known-hosts"},
			Staging: StagingConfig{
				Enabled:      true,
				Image:        "registry.a.example.org/stage:1",
				TransferHost: "transfer.hpc-a.example.org",
				StageRoot:    "/scratch/proj42/stage",
				Volumes:      []StagingVolume{{Name: "shared", ClaimName: "shared-claim", MountPath: "/shared"}},
			},
		}
	}
	c := base()
	if err := validateStagingConfig(&c); err != nil {
		t.Fatal(err)
	}
	if st := c.Staging; st.Command != defaultStageCommand || st.KeySecret != "hpc-a-tunnel-key" || st.KeySecretKey != "ssh-privatekey" || st.KnownHostsConfigMap != "hpc-a-known-hosts" {
		t.Fatalf("defaults: %+v", st)
	}

	for name, mutate := range map[string]func(*SlurmConfig){
		"no image":           func(c *SlurmConfig) { c.Staging.Image = "" },
		"no transfer host":   func(c *SlurmConfig) { c.Staging.TransferHost = "" },
		"no key":             func(c *SlurmConfig) { c.Shadow.KeySecret = "" },
		"no volumes":         func(c *SlurmConfig) { c.Staging.Volumes = nil },
		"relative mount":     func(c *SlurmConfig) { c.Staging.Volumes[0].MountPath = "shared" },
		"root mount":         func(c *SlurmConfig) { c.Staging.Volumes[0].MountPath = "/" },
		"kerberos volume":    func(c *SlurmConfig) { c.Staging.Volumes[0].Kerberos = true },
		"incomplete keytab":  func(c *SlurmConfig) { c.Staging.Kerberos = &StagingKerberos{KeytabSecret: "alice-keytab"} },
		"duplicate volume":   func(c *SlurmConfig) { c.Staging.Volumes = append(c.Staging.Volumes, c.Staging.Volumes[0]) },
		"volume without pvc": func(c *SlurmConfig) { c.Staging.Volumes[0].ClaimName = "" },
	} {
		c := base()
		mutate(&c)
		if err := validateStagingConfig(&c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
