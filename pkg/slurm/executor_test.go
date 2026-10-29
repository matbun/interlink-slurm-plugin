package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/api/resource"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	executorNS       = "alice"
	executorSite     = "hpc-a"
	executorAccount  = "proj42"
	executorWorkload = "0b6c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"
	executorJobID    = "4242"
	executorCtn      = "main"
	executorSA       = "system:serviceaccount:kueue-offload:kueue-offload"
)

// executorEnv is an executor whose sbatch, squeue and scancel are scripts in bin.
// squeue answers the executor's state query with the contents of bin/state, the
// plugin's status query with bin/exitstate, and "Invalid job id" while
// bin/gone exists. sbatch copies every script it gets to bin/sbatch.scripts,
// and fails with the contents of bin/sbatch.stderr (exit code in
// bin/sbatch.rc) when that file exists.
type executorEnv struct {
	t        *testing.T
	bin      string
	dataRoot string
	d        *Executor
	kube     *fake.Clientset
	shadow   *recordingShadow
	mux      *http.ServeMux
}

type recordingShadow struct {
	mu      sync.Mutex
	ensured []ShadowSpec
	deleted []string
}

func (s *recordingShadow) Ensure(_ context.Context, spec ShadowSpec) (ShadowRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensured = append(s.ensured, spec)
	return ShadowRef{Deployment: "shadow-" + spec.WorkloadName, Service: "shadow-" + spec.WorkloadName}, nil
}

func (s *recordingShadow) Delete(_ context.Context, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, uid)
	return nil
}

func newExecutorEnv(t *testing.T, objects ...runtime.Object) *executorEnv {
	t.Helper()
	resetStatusCache()
	t.Cleanup(resetStatusCache)
	bin := t.TempDir()
	dataRoot := t.TempDir() + "/"
	sbatch := writeStubScript(t, bin, "sbatch", `
cat "$1" >> `+bin+`/sbatch.scripts
echo "#--- end of script" >> `+bin+`/sbatch.scripts
if [ -f `+bin+`/sbatch.stderr ]; then cat `+bin+`/sbatch.stderr >&2; exit $(cat `+bin+`/sbatch.rc); fi
echo "Submitted batch job `+executorJobID+`"`)
	squeue := writeStubScript(t, bin, "squeue", `
case "$*" in
*--me*) exit 0 ;;
*Reason*)
  if [ -f `+bin+`/gone ]; then echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1; fi
  cat `+bin+`/state ;;
*exit_code*)
  if [ -f `+bin+`/gone ]; then echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1; fi
  cat `+bin+`/exitstate ;;
esac`)
	scancel := writeStubScript(t, bin, "scancel", `echo "$@" >> `+bin+`/scancel.calls`)

	jids := map[string]*JidStruct{}
	h := &SidecarHandler{
		Config: SlurmConfig{
			BashPath:              testBashPath,
			Sbatchpath:            sbatch,
			Squeuepath:            squeue,
			Scancelpath:           scancel,
			DataRootFolder:        dataRoot,
			ContainerRuntime:      singularityRuntime,
			SingularityPath:       singularityRuntime,
			ExportPodData:         true,
			GangSchedulingEnabled: true,
			GangTimeout:           "10m",
			SiteScriptPath:        filepath.Join(bin, "no-site.sh"),
			Executor: ExecutorConfig{
				Enabled:   true,
				Namespace: executorNS,
				Auth:      ExecutorAuthConfig{Mode: ExecutorAuthNetwork},
				StateDir:  dataRoot + "executor",
			},
			Sites: map[string]SiteConfig{executorSite: {
				LoginHost:       "login.hpc-a.example.org",
				User:            "alice",
				Port:            22,
				AllowedAccounts: []string{executorAccount},
			}},
			Shadow: ShadowConfig{Enabled: true},
		},
		JIDs:      &jids,
		Ctx:       context.Background(),
		GangTable: map[string]*GangEntry{},
	}
	// Every namespace has the cluster's CA, published by the API server.
	hasRootCA := false
	for _, o := range objects {
		if cm, ok := o.(*v1.ConfigMap); ok && cm.Name == "kube-root-ca.crt" {
			hasRootCA = true
		}
	}
	if !hasRootCA {
		objects = append(objects, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: executorNS}, Data: map[string]string{"ca.crt": "-----BEGIN CERTIFICATE-----\n"}})
	}
	kube := fake.NewClientset(objects...)
	shadow := &recordingShadow{}
	d, err := NewExecutor(h, kube, shadow)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	d.RegisterRoutes(mux)
	e := &executorEnv{t: t, bin: bin, dataRoot: dataRoot, d: d, kube: kube, shadow: shadow, mux: mux}
	e.setState("PD Priority", "0 PD")
	return e
}

func (e *executorEnv) write(name, content string) {
	e.t.Helper()
	if err := os.WriteFile(filepath.Join(e.bin, name), []byte(content+"\n"), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

// setState sets what squeue reports: the executor's query (state and reason) and
// the plugin's (exit code and state). The status cache is cleared, as its TTL
// would between two polls.
func (e *executorEnv) setState(state, exitState string) {
	e.write("state", state)
	e.write("exitstate", exitState)
	resetStatusCache()
}

func (e *executorEnv) read(name string) string {
	raw, err := os.ReadFile(filepath.Join(e.bin, name))
	if err != nil && !os.IsNotExist(err) {
		e.t.Fatal(err)
	}
	return string(raw)
}

func (e *executorEnv) memberDir(i int) string {
	return e.dataRoot + executorNS + "-" + memberUID(executorWorkload, i)
}

func (e *executorEnv) do(method, target string, body any, header ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			e.t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, target, &buf)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

func (e *executorEnv) jobStatus() JobStatus {
	e.t.Helper()
	rec := e.do(http.MethodGet, "/jobs/"+executorWorkload, nil)
	if rec.Code != http.StatusOK {
		e.t.Fatalf("GET /jobs = %d %s", rec.Code, rec.Body.String())
	}
	var st JobStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		e.t.Fatal(err)
	}
	return st
}

func submitRequest(count int32) SubmitRequest {
	return SubmitRequest{
		APIVersion:   ExecutorAPIVersion,
		WorkloadUID:  executorWorkload,
		WorkloadName: "job-train-5d565",
		Namespace:    executorNS,
		Owner:        ExecutorOwner{APIVersion: "batch/v1", Kind: "Job", Name: "train", UID: "owner-uid"},
		Site:         executorSite,
		Account:      executorAccount,
		PodSets: []ExecutorPodSet{{
			Name:      "main",
			Count:     count,
			Partition: "gpu",
			QOS:       "debug",
			Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
				Name:    executorCtn,
				Image:   busyboxImage,
				Command: []string{"sh", "-c", "echo hello"},
			}}}},
		}},
	}
}

func decodeSubmit(t *testing.T, rec *httptest.ResponseRecorder) SubmitResponse {
	t.Helper()
	var resp SubmitResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) ExecutorError {
	t.Helper()
	var resp ExecutorError
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

func (e *executorEnv) submit(req SubmitRequest) SubmitResponse {
	e.t.Helper()
	rec := e.do(http.MethodPost, "/submit", req)
	if rec.Code != http.StatusCreated {
		e.t.Fatalf("POST /submit = %d %s", rec.Code, rec.Body.String())
	}
	return decodeSubmit(e.t, rec)
}

func TestExecutorSubmitGangIsOneJob(t *testing.T) {
	e := newExecutorEnv(t)
	resp := e.submit(submitRequest(4))
	if resp.JobID != executorJobID || !resp.Accepted || resp.APIVersion != ExecutorAPIVersion {
		t.Fatalf("response = %+v", resp)
	}

	scripts := e.read("sbatch.scripts")
	if n := strings.Count(scripts, "#--- end of script"); n != 1 {
		t.Fatalf("sbatch ran %d times, want 1\n%s", n, scripts)
	}
	for _, want := range []string{"#SBATCH --nodes=4\n", "#SBATCH --account=" + executorAccount + "\n", "#SBATCH --partition=gpu\n", "#SBATCH --qos=debug\n", "#SBATCH --job-name=" + executorWorkload + "\n"} {
		if !strings.Contains(scripts, want) {
			t.Errorf("job script lacks %q\n%s", want, scripts)
		}
	}
	for i := 0; i < 4; i++ {
		uid := memberUID(executorWorkload, i)
		if jid := (*e.d.Sidecar.JIDs)[uid]; jid == nil || jid.JID != executorJobID {
			t.Errorf("member %d: JID entry %+v, want job %s", i, jid, executorJobID)
		}
		env, err := os.ReadFile(e.memberDir(i) + "/" + executorCtn + "_envfile.properties")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(env), "INTERLINK_RANK="+string(rune('0'+i))+"\n") || !strings.Contains(string(env), "INTERLINK_WORLD_SIZE=4\n") {
			t.Errorf("member %d env file:\n%s", i, env)
		}
	}
	if len(e.d.Sidecar.GangTable) != 1 {
		t.Errorf("gang table has %d entries", len(e.d.Sidecar.GangTable))
	}
	if _, err := os.Stat(filepath.Join(e.dataRoot, "executor", executorWorkload+".json")); err != nil {
		t.Errorf("no record: %v", err)
	}
}

func TestExecutorSubmitSinglePod(t *testing.T) {
	e := newExecutorEnv(t)
	if resp := e.submit(submitRequest(1)); resp.JobID != executorJobID {
		t.Fatalf("jobID = %q", resp.JobID)
	}
	scripts := e.read("sbatch.scripts")
	if strings.Contains(scripts, "--nodes=") || strings.Contains(scripts, "gang") {
		t.Errorf("one member must take the single-pod script\n%s", scripts)
	}
	if !strings.Contains(scripts, "#SBATCH --account="+executorAccount) || !strings.Contains(scripts, "#SBATCH --partition=gpu") {
		t.Errorf("job script lacks account or partition\n%s", scripts)
	}
	if !strings.Contains(scripts, e.memberDir(0)+"/job.out") {
		t.Errorf("job script does not write to the member's directory\n%s", scripts)
	}
	if len(e.d.Sidecar.GangTable) != 0 {
		t.Errorf("single pod went through the gang table")
	}
}

func TestExecutorSubmitIsIdempotent(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(submitRequest(2))
	rec := e.do(http.MethodPost, "/submit", submitRequest(2))
	if rec.Code != http.StatusOK {
		t.Fatalf("second submit = %d %s", rec.Code, rec.Body.String())
	}
	if resp := decodeSubmit(t, rec); resp.JobID != executorJobID {
		t.Fatalf("second submit jobID = %q", resp.JobID)
	}
	if n := strings.Count(e.read("sbatch.scripts"), "#--- end of script"); n != 1 {
		t.Fatalf("sbatch ran %d times", n)
	}

	// A restarted executor knows the workload from its record.
	d2, err := NewExecutor(e.d.Sidecar, e.kube, nil)
	if err != nil {
		t.Fatal(err)
	}
	jobID, created, aerr := d2.submit(context.Background(), submitRequest(2))
	if aerr != nil || created || jobID != executorJobID {
		t.Fatalf("after restart: %q %v %+v", jobID, created, aerr)
	}
}

// Kueue charts the requests, so they also size the Slurm job when no limit is set.
func TestExecutorRequestsSizeTheJob(t *testing.T) {
	e := newExecutorEnv(t)
	req := submitRequest(1)
	req.PodSets[0].Template.Spec.Containers[0].Resources = v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("20"), v1.ResourceMemory: resource.MustParse("8Gi"), "nvidia.com/gpu": resource.MustParse("1")},
		Limits:   v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("16Gi")},
	}
	e.submit(req)
	script := e.read("sbatch.scripts")
	for _, want := range []string{"--cpus-per-task=20", "--mem=16384", "gpu:1"} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
}

// A save interrupted between removing the old record and renaming the new one
// leaves only <uid>.json.tmp; a restarted executor takes it as the record.
func TestExecutorInterruptedRecordSaveRecovered(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(submitRequest(2))
	path := e.d.recordPath(executorWorkload)
	if err := os.Rename(path, path+".tmp"); err != nil {
		t.Fatal(err)
	}
	d2, err := NewExecutor(e.d.Sidecar, e.kube, nil)
	if err != nil {
		t.Fatal(err)
	}
	jobID, created, aerr := d2.submit(context.Background(), submitRequest(2))
	if aerr != nil || created || jobID != executorJobID {
		t.Fatalf("after restart: %q %v %+v", jobID, created, aerr)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("record not restored: %v", err)
	}
}

func TestExecutorSubmitRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SubmitRequest)
		want   string
	}{
		{"account not allowed", func(r *SubmitRequest) { r.Account = "proj99" }, `account "proj99" is not allowed`},
		{"unknown site", func(r *SubmitRequest) { r.Site = "hpc-b" }, `unknown site "hpc-b"`},
		{"foreign namespace", func(r *SubmitRequest) { r.Namespace = "bob" }, `namespace "bob" is not this executor's`},
		{"no PodSets", func(r *SubmitRequest) { r.PodSets = nil }, "no PodSets"},
		{"unlisted reference", func(r *SubmitRequest) {
			r.PodSets[0].Template.Spec.Containers[0].EnvFrom = []v1.EnvFromSource{{SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}}}}
		}, `Secret "hf-token", which is not in secretRefs`},
		{"missing reference", func(r *SubmitRequest) {
			r.PodSets[0].ConfigMapRefs = []string{"train-config"}
			useConfigMap(r, "train-config")
		}, `cannot read ConfigMap "train-config"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := submitRequest(1)
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
}

func useConfigMap(r *SubmitRequest, name string) {
	r.PodSets[0].Template.Spec.Volumes = []v1.Volume{{Name: "cfg", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: name}}}}}
}

func TestExecutorSubmitSbatchRefusal(t *testing.T) {
	const stderr = "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified"
	e := newExecutorEnv(t)
	e.write("sbatch.stderr", stderr)
	e.write("sbatch.rc", "1")

	rec := e.do(http.MethodPost, "/submit", submitRequest(2))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeError(t, rec)
	if !resp.Permanent || resp.Message != stderr+"\n" {
		t.Fatalf("response = %+v, want sbatch's stderr verbatim", resp)
	}
	if e.d.record(executorWorkload) != nil || len(e.d.Sidecar.GangTable) != 0 || len(*e.d.Sidecar.JIDs) != 0 {
		t.Fatalf("a refused workload left state behind")
	}
	for i := 0; i < 2; i++ {
		if _, err := os.Stat(e.memberDir(i)); !os.IsNotExist(err) {
			t.Errorf("member %d directory left: %v", i, err)
		}
	}

	// ssh failing is worth retrying, and a retry starts from scratch.
	e.write("sbatch.rc", "255")
	e.write("sbatch.stderr", "ssh: connect to host login.hpc-a.example.org port 22: Connection timed out")
	rec = e.do(http.MethodPost, "/submit", submitRequest(2))
	if rec.Code != http.StatusServiceUnavailable || decodeError(t, rec).Permanent {
		t.Fatalf("ssh failure = %d %s, want a transient 503", rec.Code, rec.Body.String())
	}
	// So is a limit on submitted jobs, which ends when the user's other jobs end.
	e.write("sbatch.rc", "1")
	e.write("sbatch.stderr", "sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)")
	rec = e.do(http.MethodPost, "/submit", submitRequest(2))
	if rec.Code != http.StatusServiceUnavailable || decodeError(t, rec).Permanent || !strings.Contains(decodeError(t, rec).Message, "QOSMaxSubmitJobPerUserLimit") {
		t.Fatalf("submit limit = %d %s, want a transient 503 naming the limit", rec.Code, rec.Body.String())
	}
	os.Remove(filepath.Join(e.bin, "sbatch.stderr"))
	e.submit(submitRequest(2))
}

func TestExecutorSecretKeyRefResolved(t *testing.T) {
	secret := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("hf_s3cr3t")}}
	config := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "train-config", Namespace: executorNS}, Data: map[string]string{"lr": "0.001"}}
	e := newExecutorEnv(t, secret, config)
	req := submitRequest(1)
	ps := &req.PodSets[0]
	ps.SecretRefs = []string{"hf-token"}
	ps.ConfigMapRefs = []string{"train-config"}
	ps.Template.Labels = map[string]string{"team": "ml"}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{
		{Name: "HF_TOKEN", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "token"}}},
		{Name: "LR", ValueFrom: &v1.EnvVarSource{ConfigMapKeyRef: &v1.ConfigMapKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "train-config"}, Key: "lr"}}},
		{Name: "POD", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "TEAM", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.labels['team']"}}},
		{Name: "OPT", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "absent", Optional: ptrBool(true)}}},
	}
	e.submit(req)

	raw, err := os.ReadFile(e.memberDir(0) + "/" + executorCtn + "_envfile.properties")
	if err != nil {
		t.Fatal(err)
	}
	env := string(raw)
	for _, want := range []string{"HF_TOKEN=hf_s3cr3t\n", "LR=0.001\n", "POD=train-0\n", "TEAM=ml\n", "INTERLINK_RANK=0\n"} {
		if !strings.Contains(env, want) {
			t.Errorf("env file lacks %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "OPT=") {
		t.Errorf("an optional missing key must leave the variable out:\n%s", env)
	}
	if rec, _ := os.ReadFile(filepath.Join(e.dataRoot, "executor", executorWorkload+".json")); bytes.Contains(rec, []byte("hf_s3cr3t")) {
		t.Errorf("the record holds the secret's value")
	}
}

func ptrBool(b bool) *bool { return &b }

func TestExecutorJobStates(t *testing.T) {
	e := newExecutorEnv(t)
	req := submitRequest(2)
	req.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)

	st := e.jobStatus()
	if st.State != JobQueued || st.Reason != "Priority" || st.Message != "Slurm job 4242 pending (Priority)" || st.NodeName != "" {
		t.Fatalf("queued: %+v", st)
	}
	if len(e.shadow.ensured) != 0 {
		t.Fatalf("a queued job got a shadow")
	}

	e.setState("R None", "0 R")
	for i, node := range []string{"node07", "node08"} {
		if err := os.WriteFile(e.memberDir(i)+"/compute-node", []byte(node+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	st = e.jobStatus()
	if st.State != JobRunning || st.NodeName != "node07" || strings.Join(st.Nodes, ",") != "node07,node08" || st.StartTime == nil {
		t.Fatalf("running: %+v", st)
	}
	if len(st.Ranks) != 2 || st.Ranks[1].PodUID != memberUID(executorWorkload, 1) {
		t.Fatalf("ranks: %+v", st.Ranks)
	}
	if len(e.shadow.ensured) != 1 || e.shadow.ensured[0].NodeName != "node07" || e.shadow.ensured[0].Terminal || e.shadow.ensured[0].WorkloadName != "train" {
		t.Fatalf("shadow on running: %+v", e.shadow.ensured)
	}
	if st.Shadow == nil || st.Shadow.Deployment != "shadow-train" {
		t.Fatalf("shadow ref: %+v", st.Shadow)
	}

	e.setState("CD None", "0 CD")
	for i, code := range []string{"0", "3"} {
		if err := os.WriteFile(e.memberDir(i)+"/run-"+executorCtn+".status", []byte(code+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	st = e.jobStatus()
	if st.State != JobFailed || st.ExitCode != 0 || !strings.Contains(st.Message, "rank 1 exited with code 3") || st.CompletionTime == nil {
		t.Fatalf("failed rank: %+v", st)
	}
	if n := len(e.shadow.ensured); n != 2 || !e.shadow.ensured[1].Terminal {
		t.Fatalf("shadow at the end: %+v", e.shadow.ensured)
	}
	e.jobStatus()
	if n := len(e.shadow.ensured); n != 2 {
		t.Fatalf("terminal shadow ensured %d times", n-1)
	}
}

func TestExecutorJobCompletedAndFailures(t *testing.T) {
	tests := []struct {
		name, state, exitState, status string
		gone                           bool
		wantState, wantReason          string
		wantExit                       int32
	}{
		{name: "completed", state: "CD None", exitState: "0 CD", status: "0", wantState: JobCompleted, wantReason: "Completed"},
		{name: "non-zero exit", state: "F NonZeroExitCode", exitState: "2 F", status: "2", wantState: JobFailed, wantReason: "NonZeroExitCode", wantExit: 2},
		{name: "time limit", state: "TO TimeLimit", exitState: "0 TO", wantState: JobFailed, wantReason: "TimeLimit", wantExit: 1},
		{name: "cancelled outside", state: "CA None", exitState: "0 CA", wantState: JobCancelled, wantReason: "Cancelled"},
		{name: "vanished", gone: true, wantState: JobCancelled, wantReason: "Vanished"},
		{name: "vanished after ending", gone: true, status: "0", wantState: JobCompleted, wantReason: "Completed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.submit(submitRequest(1))
			e.setState(tt.state, tt.exitState)
			if tt.gone {
				e.write("gone", "")
			}
			if tt.status != "" {
				if err := os.WriteFile(e.memberDir(0)+"/run-"+executorCtn+".status", []byte(tt.status), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			st := e.jobStatus()
			if st.State != tt.wantState || st.Reason != tt.wantReason || st.ExitCode != tt.wantExit {
				t.Fatalf("got %s/%s exit %d (%s), want %s/%s exit %d", st.State, st.Reason, st.ExitCode, st.Message, tt.wantState, tt.wantReason, tt.wantExit)
			}
			if st.CompletionTime == nil || st.StartTime == nil {
				t.Fatalf("terminal state without times: %+v", st)
			}
		})
	}
}

func TestExecutorDelete(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(submitRequest(2))
	e.setState("R None", "0 R")

	rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d %s", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(e.read("scancel.calls")); got != executorJobID {
		t.Fatalf("scancel calls %q, want one for %s", got, executorJobID)
	}
	if len(e.shadow.deleted) != 1 || e.shadow.deleted[0] != executorWorkload {
		t.Fatalf("shadow deletes: %v", e.shadow.deleted)
	}
	for i := 0; i < 2; i++ {
		if _, err := os.Stat(e.memberDir(i)); !os.IsNotExist(err) {
			t.Errorf("member %d directory left", i)
		}
	}
	if _, err := os.Stat(filepath.Join(e.dataRoot, "executor", executorWorkload+".json")); !os.IsNotExist(err) {
		t.Errorf("record left")
	}
	if rec := e.do(http.MethodGet, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNotFound || !decodeError(t, rec).Permanent {
		t.Fatalf("GET after DELETE = %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("second DELETE = %d", rec.Code)
	}

	// A finished job is not cancelled.
	e.submit(submitRequest(1))
	e.setState("CD None", "0 CD")
	os.Remove(filepath.Join(e.bin, "scancel.calls"))
	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d", rec.Code)
	}
	if got := e.read("scancel.calls"); got != "" {
		t.Fatalf("scancel of an ended job: %q", got)
	}
}

func (e *executorEnv) writeLog(i int, content string) {
	e.t.Helper()
	f, err := os.OpenFile(e.memberDir(i)+"/run-"+executorCtn+".out", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		e.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		e.t.Fatal(err)
	}
}

func TestExecutorGetLogs(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(submitRequest(3))
	for i, text := range []string{"zero a\nzero b\n", "one\n", "two a\ntwo b"} {
		e.writeLog(i, text)
	}

	rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&rank=2", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "two a\ntwo b" {
		t.Fatalf("rank 2 = %d %q", rec.Code, rec.Body.String())
	}

	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload, nil)
	want := "[rank 0] zero a\n[rank 0] zero b\n[rank 1] one\n[rank 2] two a\n[rank 2] two b\n"
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("merged = %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&tail=2", nil)
	if rec.Body.String() != "[rank 2] two a\n[rank 2] two b\n" {
		t.Fatalf("tail 2 = %q", rec.Body.String())
	}
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&rank=3", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("rank out of range = %d", rec.Code)
	}
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID=nope", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown workload = %d", rec.Code)
	}
}

func TestExecutorGetLogsSinglePodHasNoPrefix(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(submitRequest(1))
	e.writeLog(0, "hello\n")
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload, nil); rec.Body.String() != "hello\n" {
		t.Fatalf("single member = %q", rec.Body.String())
	}
}

func TestExecutorGetLogsFollow(t *testing.T) {
	oldPoll, oldQuiet := logsPollInterval, logsDrainQuiet
	logsPollInterval, logsDrainQuiet = 10*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { logsPollInterval, logsDrainQuiet = oldPoll, oldQuiet })

	e := newExecutorEnv(t)
	e.submit(submitRequest(2))
	e.writeLog(0, "first\n")
	srv := httptest.NewServer(e.mux)
	defer srv.Close()

	done := make(chan string)
	go func() {
		resp, err := http.Get(srv.URL + "/getLogs?workloadUID=" + executorWorkload + "&follow=true")
		if err != nil {
			done <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		done <- buf.String()
	}()

	time.Sleep(50 * time.Millisecond)
	e.writeLog(1, "worker up\nhalf")
	time.Sleep(50 * time.Millisecond)
	e.writeLog(0, "second\n")
	select {
	case got := <-done:
		t.Fatalf("stream ended before the job: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	e.writeLog(1, " line\nlast")
	for i := 0; i < 2; i++ {
		if err := os.WriteFile(e.memberDir(i)+"/run-"+executorCtn+".status", []byte("0"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The head's log is completed after the ranks ended: the stream still takes it.
	time.Sleep(50 * time.Millisecond)
	e.writeLog(0, "late\n")
	select {
	case got := <-done:
		want := "[rank 0] first\n[rank 1] worker up\n[rank 0] second\n[rank 1] half line\n[rank 1] last\n[rank 0] late\n"
		if got != want {
			t.Fatalf("followed = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end with the job")
	}
}

func TestExecutorTokenReview(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Sidecar.Config.Executor.Auth = ExecutorAuthConfig{Mode: ExecutorAuthTokenReview, Audience: "kueue-offload-executor", AllowedServiceAccounts: []string{executorSA}}
	reviews := 0
	e.kube.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		reviews++
		tr := a.(k8stesting.CreateAction).GetObject().(*authv1.TokenReview)
		user := map[string]string{"good": executorSA, "other": "system:serviceaccount:bob:intruder"}[tr.Spec.Token]
		if len(tr.Spec.Audiences) != 1 || tr.Spec.Audiences[0] != "kueue-offload-executor" {
			user = ""
		}
		tr.Status = authv1.TokenReviewStatus{Authenticated: user != "", User: authv1.UserInfo{Username: user}, Audiences: tr.Spec.Audiences}
		return true, tr, nil
	})

	if rec := e.do(http.MethodPost, "/submit", submitRequest(1)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token = %d", rec.Code)
	}
	if rec := e.do(http.MethodPost, "/submit", submitRequest(1), "Authorization", "Bearer forged"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated token = %d", rec.Code)
	}
	rec := e.do(http.MethodPost, "/submit", submitRequest(1), "Authorization", "Bearer other")
	if rec.Code != http.StatusForbidden || decodeError(t, rec).Permanent {
		t.Fatalf("token of another user = %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.do(http.MethodGet, "/jobs/"+executorWorkload, nil, "Authorization", "Bearer other"); rec.Code != http.StatusForbidden {
		t.Fatalf("GET /jobs with another user's token = %d", rec.Code)
	}
	if s := e.read("sbatch.scripts"); s != "" {
		t.Fatalf("sbatch ran for a refused caller")
	}

	if rec := e.do(http.MethodPost, "/submit", submitRequest(1), "Authorization", "Bearer good"); rec.Code != http.StatusCreated {
		t.Fatalf("offload controller's token = %d %s", rec.Code, rec.Body.String())
	}
	before := reviews
	if rec := e.do(http.MethodGet, "/jobs/"+executorWorkload, nil, "Authorization", "Bearer good"); rec.Code != http.StatusOK {
		t.Fatalf("GET /jobs = %d %s", rec.Code, rec.Body.String())
	}
	if reviews != before {
		t.Fatalf("a token that passed was reviewed again within a minute")
	}
	if rec := e.do(http.MethodGet, "/healthz", nil); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
}

func TestExecutorGetLogsWithoutWorkloadIsThePodPath(t *testing.T) {
	e := newExecutorEnv(t)
	dir := e.dataRoot + "default-pod-uid"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/run-c.out", []byte("pod log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := `{"Namespace":"default","PodUID":"pod-uid","PodName":"p","ContainerName":"c"}`
	req := httptest.NewRequest(http.MethodGet, "/getLogs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "pod log\n" {
		t.Fatalf("pod /getLogs = %d %q", rec.Code, rec.Body.String())
	}
}

func TestMemberSocketDirsDiffer(t *testing.T) {
	seen := map[string]int{}
	for i := 0; i < 12; i++ {
		dir := socketDirFor(socketTestRoot, memberUID(executorWorkload, i))
		if dir == "" {
			t.Fatalf("member %d has no socket directory", i)
		}
		if j, dup := seen[dir]; dup {
			t.Fatalf("members %d and %d share %s", j, i, dir)
		}
		seen[dir] = i
		if len(dir) != len(socketTestRoot)+1+socketDirNameLen {
			t.Fatalf("member %d: %s is not 12 characters under the root", i, dir)
		}
	}
	if got := socketDirFor(socketTestRoot, executorWorkload); got != socketTestRoot+"/0b6c1d2e3f40" {
		t.Fatalf("a pod UID's directory changed: %s", got)
	}
}

func TestValidateExecutorConfig(t *testing.T) {
	for _, k := range []string{"REMOTE_USER", "REMOTE_LOGIN", "REMOTE_PORT", "SSH_KEY"} {
		t.Setenv(k, "")
	}
	nsFile := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(nsFile, []byte("alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := serviceAccountNamespaceFile
	serviceAccountNamespaceFile = nsFile
	t.Cleanup(func() { serviceAccountNamespaceFile = old })

	base := func() SlurmConfig {
		return SlurmConfig{
			DataRootFolder: "/data/",
			Executor:       ExecutorConfig{Enabled: true, Auth: ExecutorAuthConfig{AllowedServiceAccounts: []string{executorSA}}},
			Sites: map[string]SiteConfig{executorSite: {
				LoginHost: "login.hpc-a.example.org", User: "alice", KeyFile: "/etc/interlink/keys/hpc-a/ssh-privatekey",
				AllowedAccounts: []string{executorAccount},
			}},
		}
	}
	c := base()
	if err := validateExecutorConfig(&c); err != nil {
		t.Fatal(err)
	}
	if c.Executor.Namespace != "alice" || c.Executor.StateDir != "/data/executor" || c.Executor.Auth.Mode != ExecutorAuthTokenReview || c.Executor.Auth.Audience != "kueue-offload-executor" || !c.GangSchedulingEnabled {
		t.Fatalf("defaults: %+v", c.Executor)
	}
	for k, want := range map[string]string{"REMOTE_USER": "alice", "REMOTE_LOGIN": "login.hpc-a.example.org", "REMOTE_PORT": "22", "SSH_KEY": "/etc/interlink/keys/hpc-a/ssh-privatekey"} {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}

	bad := map[string]func(*SlurmConfig){
		"two sites": func(c *SlurmConfig) { c.Sites["hpc-b"] = c.Sites[executorSite] },
		"no sites":  func(c *SlurmConfig) { c.Sites = nil },
		"no accounts": func(c *SlurmConfig) {
			s := c.Sites[executorSite]
			s.AllowedAccounts = nil
			c.Sites[executorSite] = s
		},
		"tokenreview without callers": func(c *SlurmConfig) { c.Executor.Auth.AllowedServiceAccounts = nil },
		"unknown auth mode":           func(c *SlurmConfig) { c.Executor.Auth.Mode = "basic" },
		"shadow without key":          func(c *SlurmConfig) { c.Shadow.Enabled = true },
	}
	for name, mutate := range bad {
		c := base()
		mutate(&c)
		if err := validateExecutorConfig(&c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	off := SlurmConfig{Sites: map[string]SiteConfig{"a": {}, "b": {}}}
	if err := validateExecutorConfig(&off); err != nil {
		t.Errorf("Executor off must not be validated: %v", err)
	}
}

// Two containers of one pod mount the same volume: the second rewrites the folder the
// first wrote, without removing it, and a key the data no longer has goes.
func TestRemoveStaleFilesKeepsTheFolder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "projectedVolumeMaps", "kube-api-access")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"token", "namespace", "old", "sub/a"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeStaleFiles(dir, map[string][]byte{"token": nil, "namespace": nil, "sub/a": nil}); err != nil {
		t.Fatal(err)
	}
	for f, want := range map[string]bool{"token": true, "namespace": true, "sub/a": true, "old": false} {
		if _, err := os.Stat(filepath.Join(dir, f)); (err == nil) != want {
			t.Errorf("%s present: %v, want %v", f, err == nil, want)
		}
	}
	if err := removeStaleFiles(filepath.Join(dir, "missing"), nil); err != nil {
		t.Errorf("a folder that does not exist yet: %v", err)
	}
}
