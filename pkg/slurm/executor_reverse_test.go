package slurm

import (
	"context"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

const reverseTarget = "mlflow.alice.svc.cluster.local"

// reverseEnv is an executor whose shadows are rendered by the real shadow manager
// on a fake cluster, with the reverse tunnel enabled.
func reverseEnv(t *testing.T, reverse shadow.ReverseConfig) *executorEnv {
	t.Helper()
	e := newExecutorEnv(t)
	e.d.Sidecar.Config.SocketDirRoot = socketTestRoot
	e.d.Sidecar.Config.Shadow = shadowBlock()
	e.d.Sidecar.Config.Shadow.Reverse = reverse
	m, err := NewShadowManager(e.kube, e.d.Sidecar.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	return e
}

func reverseRequest(count int32, forwards string) SubmitRequest {
	req := submitRequest(count)
	req.PodSets[0].Template.Annotations = map[string]string{shadow.AnnReverseForwards: forwards}
	return req
}

// The head rank runs the tunnel's set-up on the node the shadow reaches, the
// other ranks do not, and the shadow carries the ssh-reverse container.
func TestExecutorReverseTunnel(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":5000"}})
	e.submit(reverseRequest(2, reverseTarget+":5000"))

	scripts := e.read("sbatch.scripts")
	head, workers, ok := strings.Cut(scripts, "# rank 1 (")
	if !ok {
		t.Fatalf("no rank 1 in the gang script\n%s", scripts)
	}
	names := reverseTarget + " mlflow.alice.svc mlflow.alice mlflow"
	for _, want := range []string{"interLink reverse tunnel", names, "rev-relay.py", "export INTERLINK_SOCKET_DIR=" + socketDirFor(socketTestRoot, memberUID(executorWorkload, 0))} {
		if !strings.Contains(head, want) {
			t.Errorf("rank 0 lacks %q\n%s", want, head)
		}
	}
	if strings.Contains(workers, "interLink reverse tunnel") || strings.Contains(workers, names) {
		t.Errorf("a worker rank sets up the tunnel\n%s", workers)
	}
	if strings.Index(head, "interLink reverse tunnel") > strings.Index(head, "exec "+e.memberDir(0)+"/job.sh") {
		t.Errorf("the tunnel is set up after the head's workload starts\n%s", head)
	}
	if member1, err := os.ReadFile(e.memberDir(1) + "/job.slurm"); err != nil || strings.Contains(string(member1), "interLink reverse tunnel") {
		t.Errorf("member 1 carries the tunnel's set-up (%v)", err)
	}
	if out, err := exec.Command("bash", "-n", e.memberDir(0)+"/job.slurm").CombinedOutput(); err != nil {
		t.Errorf("bash -n on the gang script: %v: %s", err, out)
	}

	e.setState("R None", "0 R")
	for i, node := range []string{"node07", "node08"} {
		if err := os.WriteFile(e.memberDir(i)+"/compute-node", []byte(node+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	st := e.jobStatus()
	if st.State != JobRunning || st.Shadow == nil || st.Shadow.Service != "" {
		t.Fatalf("running: state %s, shadow %+v", st.State, st.Shadow)
	}
	dep, err := e.kube.AppsV1().Deployments(executorNS).Get(context.Background(), st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names := containerNames(dep); names != "ssh-reverse,job-logs" {
		t.Fatalf("shadow containers %s", names)
	}
	env := map[string]string{}
	for _, ev := range dep.Spec.Template.Spec.Containers[0].Env {
		env[ev.Name] = ev.Value
	}
	if env["REV_FORWARDS"] != "5000:"+reverseTarget+":5000" || env["REV_JOB_NAME"] != executorWorkload {
		t.Errorf("ssh-reverse env %v", env)
	}
	cm, err := e.kube.CoreV1().ConfigMaps(executorNS).Get(context.Background(), st.Shadow.NodeConfigMap, metav1.GetOptions{})
	if err != nil || cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != socketDirFor(socketTestRoot, memberUID(executorWorkload, 0)) {
		t.Fatalf("node ConfigMap %v (%v): the tunnel must end in the head's socket directory", cm, err)
	}
}

func TestExecutorReverseSinglePod(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true})
	e.submit(reverseRequest(1, "8443:"+reverseTarget+":443"))
	scripts := e.read("sbatch.scripts")
	pre := strings.Index(scripts, "interLink reverse tunnel")
	if pre < 0 || pre > strings.Index(scripts, e.memberDir(0)+"/job.sh") {
		t.Fatalf("the tunnel is not set up before job.sh\n%s", scripts)
	}
}

func TestExecutorReverseRefusals(t *testing.T) {
	enabled := shadow.ReverseConfig{Enabled: true}
	tests := []struct {
		name    string
		reverse shadow.ReverseConfig
		req     SubmitRequest
		want    string
	}{
		{"reverse disabled", shadow.ReverseConfig{}, reverseRequest(1, reverseTarget+":5000"), "Shadow.Reverse.Enabled"},
		{"target not allowed", shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":5000"}},
			reverseRequest(1, reverseTarget+":5000 minio.alice.svc.cluster.local:9000"), "target minio.alice.svc.cluster.local:9000 is not in Shadow.Reverse.AllowedTargets"},
		{"socks not allowed", enabled, func() SubmitRequest {
			r := submitRequest(1)
			r.PodSets[0].Template.Annotations = map[string]string{shadow.AnnReverseSocksPort: "1080"}
			return r
		}(), "Shadow.Reverse.AllowSocks"},
		{"site launcher", enabled, func() SubmitRequest {
			r := reverseRequest(2, reverseTarget+":5000")
			r.PodSets[0].Template.Annotations[LauncherAnnotation] = "torchrun"
			return r
		}(), LauncherAnnotation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := reverseEnv(t, tt.reverse)
			rec := e.do(http.MethodPost, "/submit", tt.req)
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

func TestValidateExecutorConfigChecksReverse(t *testing.T) {
	t.Setenv(shadow.ExecutorImageEnv, "")
	base := func() SlurmConfig {
		c := SlurmConfig{
			DataRootFolder: "/data/",
			SocketDirRoot:  socketTestRoot,
			Executor:       ExecutorConfig{Enabled: true, Namespace: executorNS, Auth: ExecutorAuthConfig{Mode: ExecutorAuthNetwork}},
			Sites: map[string]SiteConfig{executorSite: {
				LoginHost: "login.hpc-a.example.org", User: "alice", AllowedAccounts: []string{executorAccount},
			}},
			Shadow: shadowBlock(),
		}
		c.Shadow.Reverse = shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":443"}}
		return c
	}
	c := base()
	if err := validateExecutorConfig(&c); err != nil {
		t.Fatalf("valid Reverse block refused: %v", err)
	}
	for name, mutate := range map[string]func(*SlurmConfig){
		"no shadows":         func(c *SlurmConfig) { c.Shadow.Enabled = false },
		"no socket dir root": func(c *SlurmConfig) { c.SocketDirRoot = "" },
		"bad target":         func(c *SlurmConfig) { c.Shadow.Reverse.AllowedTargets = []string{reverseTarget} },
		"socks with targets": func(c *SlurmConfig) { c.Shadow.Reverse.AllowSocks = true },
	} {
		c := base()
		mutate(&c)
		if err := validateExecutorConfig(&c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// tokenRequests answers TokenRequests with a token naming the ServiceAccount, and
// records them.
func tokenRequests(e *executorEnv) *[]k8stesting.CreateActionImpl {
	var seen []k8stesting.CreateActionImpl
	e.kube.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		create := a.(k8stesting.CreateActionImpl)
		if create.GetSubresource() != "token" {
			return false, nil, nil
		}
		seen = append(seen, create)
		tr := create.GetObject().(*authv1.TokenRequest).DeepCopy()
		tr.Status.Token = "minted-for-" + create.Name
		return true, tr, nil
	})
	return &seen
}

func kubeAPIAccessVolume() v1.Volume {
	expiration := int64(3607)
	return v1.Volume{Name: "kube-api-access-x7k2p", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
		{ConfigMap: &v1.ConfigMapProjection{LocalObjectReference: v1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []v1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		{DownwardAPI: &v1.DownwardAPIProjection{Items: []v1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
	}}}}
}

// findFile returns the content of the first file named name under dir.
func findFile(t *testing.T, dir, name string) (string, bool) {
	t.Helper()
	var content string
	var found bool
	err := filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil || found || de.IsDir() || de.Name() != name {
			return err
		}
		raw, err := os.ReadFile(path)
		content, found = string(raw), true
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return content, found
}

// The API server's kube-api-access volume of a plain pod gets a token minted for
// the pod's ServiceAccount, where the plugin writes projected volume files.
func TestExecutorServiceAccountToken(t *testing.T) {
	t.Setenv("SHARED_FS", "true")
	rootCA := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: executorNS}, Data: map[string]string{"ca.crt": "-----BEGIN CERTIFICATE-----\n"}}
	e := newExecutorEnv(t, rootCA)
	seen := tokenRequests(e)
	req := submitRequest(2)
	ps := &req.PodSets[0]
	ps.ConfigMapRefs = []string{"kube-root-ca.crt"}
	ps.Template.Spec.ServiceAccountName = "trainer"
	ps.Template.Spec.Volumes = []v1.Volume{kubeAPIAccessVolume()}
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "kube-api-access-x7k2p", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}}
	e.submit(req)

	if len(*seen) != 1 {
		t.Fatalf("%d TokenRequests, want 1", len(*seen))
	}
	got := (*seen)[0]
	tr := got.GetObject().(*authv1.TokenRequest)
	if got.Namespace != executorNS || got.Name != "trainer" || tr.Spec.ExpirationSeconds == nil || *tr.Spec.ExpirationSeconds != 3607 || len(tr.Spec.Audiences) != 0 {
		t.Fatalf("TokenRequest %s/%s %+v", got.Namespace, got.Name, tr.Spec)
	}
	for i := 0; i < 2; i++ {
		if token, ok := findFile(t, e.memberDir(i), "token"); !ok || token != "minted-for-trainer" {
			t.Errorf("member %d: token file %q (found %v)", i, token, ok)
		}
		if ns, ok := findFile(t, e.memberDir(i), "namespace"); !ok || ns != executorNS {
			t.Errorf("member %d: namespace file %q (found %v)", i, ns, ok)
		}
	}
}

func TestExecutorServiceAccountTokenDefaults(t *testing.T) {
	t.Setenv("SHARED_FS", "true")
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := submitRequest(1)
	ps := &req.PodSets[0]
	ps.Template.Spec.Volumes = []v1.Volume{{Name: "mlflow-token", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "jwt", Audience: "mlflow"}},
	}}}}}
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "mlflow-token", MountPath: "/var/run/mlflow"}}
	e.submit(req)
	if len(*seen) != 1 {
		t.Fatalf("%d TokenRequests, want 1", len(*seen))
	}
	got := (*seen)[0]
	tr := got.GetObject().(*authv1.TokenRequest)
	if got.Name != "default" || *tr.Spec.ExpirationSeconds != 3600 || len(tr.Spec.Audiences) != 1 || tr.Spec.Audiences[0] != "mlflow" {
		t.Fatalf("TokenRequest for %s: %+v", got.Name, tr.Spec)
	}
	if token, ok := findFile(t, e.memberDir(0), "jwt"); !ok || token != "minted-for-default" {
		t.Errorf("token file %q (found %v)", token, ok)
	}
}

func TestExecutorNoTokenWithoutProjection(t *testing.T) {
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := submitRequest(1)
	req.PodSets[0].Template.Spec.AutomountServiceAccountToken = ptrBool(true)
	e.submit(req)
	for _, a := range e.kube.Actions() {
		if a.GetResource().Resource == "serviceaccounts" {
			t.Fatalf("a template without a token projection made %s %s/%s", a.GetVerb(), a.GetResource().Resource, a.GetSubresource())
		}
	}
	if len(*seen) != 0 {
		t.Fatalf("%d TokenRequests", len(*seen))
	}
}

func TestExecutorTokenRefusedIsPermanent(t *testing.T) {
	e := newExecutorEnv(t)
	e.kube.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(v1.Resource("serviceaccounts/token"), "default", nil)
	})
	req := submitRequest(1)
	req.PodSets[0].Template.Spec.Volumes = []v1.Volume{{Name: "tok", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token"}},
	}}}}}
	rec := e.do(http.MethodPost, "/submit", req)
	if rec.Code != http.StatusBadRequest || !decodeError(t, rec).Permanent || !strings.Contains(decodeError(t, rec).Message, `ServiceAccount "default"`) {
		t.Fatalf("forbidden TokenRequest = %d %s, want a permanent 400", rec.Code, rec.Body.String())
	}
}

// A plain pod's step (Argo's executor) reaches the API server through its tunnel and
// addresses its own pod: the downward API names the real pod, and the kubelet's
// service variables point at the tunnel.
func TestExecutorSinglePodSeesItself(t *testing.T) {
	t.Setenv("SHARED_FS", "true")
	rootCA := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: executorNS}, Data: map[string]string{"ca.crt": "-----BEGIN CERTIFICATE-----\n"}}
	e := newExecutorEnv(t, rootCA)
	tokenRequests(e)
	e.d.Sidecar.Config.Executor.KubernetesAPI = ExecutorKubernetesAPI{Host: "kubernetes.default.svc.cluster.local", Port: "6443"}
	req := submitRequest(1)
	req.Owner = ExecutorOwner{APIVersion: "v1", Kind: "Pod", Name: "step-7f2k", UID: "real-pod-uid"}
	ps := &req.PodSets[0]
	ps.ConfigMapRefs = []string{"kube-root-ca.crt"}
	ps.Template.Spec.Volumes = []v1.Volume{kubeAPIAccessVolume()}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{
		{Name: "ARGO_POD_NAME", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "ARGO_POD_UID", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
	}
	e.submit(req)
	raw, err := os.ReadFile(e.memberDir(0) + "/" + executorCtn + "_envfile.properties")
	if err != nil {
		t.Fatal(err)
	}
	env := string(raw)
	for _, want := range []string{"ARGO_POD_NAME=step-7f2k", "ARGO_POD_UID=real-pod-uid", "KUBERNETES_SERVICE_HOST=kubernetes.default.svc.cluster.local", "KUBERNETES_SERVICE_PORT=6443"} {
		if !strings.Contains(env, want) {
			t.Errorf("env file lacks %s:\n%s", want, env)
		}
	}

	// Without a token the job gets no API address.
	e2 := newExecutorEnv(t)
	e2.d.Sidecar.Config.Executor.KubernetesAPI = ExecutorKubernetesAPI{Host: "kubernetes.default.svc.cluster.local", Port: "6443"}
	e2.submit(submitRequest(1))
	raw, err = os.ReadFile(e2.memberDir(0) + "/" + executorCtn + "_envfile.properties")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "KUBERNETES_SERVICE_HOST") {
		t.Errorf("a job without a token got the API address:\n%s", raw)
	}
}

// A Job's template has no ServiceAccount directory (the API server adds it to pods, and
// the workload's pods never exist): the executor adds it, with the namespace and the CA, and
// with a token only when the site allows cluster tokens.
func TestExecutorServiceAccountDirectory(t *testing.T) {
	t.Setenv("SHARED_FS", "true")
	for _, tc := range []struct {
		name       string
		automount  *bool
		siteTokens bool
		wantDir    bool
		wantToken  bool
	}{
		{"default, no cluster tokens", nil, false, true, false},
		{"default, cluster tokens", nil, true, true, true},
		{"opted out", ptrBool(false), true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			seen := tokenRequests(e)
			e.d.Sidecar.Config.Executor.AutomountServiceAccountToken = tc.siteTokens
			req := submitRequest(1)
			req.PodSets[0].Template.Spec.AutomountServiceAccountToken = tc.automount
			e.submit(req)
			ns, hasNS := findFile(t, e.memberDir(0), "namespace")
			_, hasCA := findFile(t, e.memberDir(0), "ca.crt")
			token, hasToken := findFile(t, e.memberDir(0), "token")
			if hasNS != tc.wantDir || hasCA != tc.wantDir || (hasNS && ns != executorNS) {
				t.Errorf("namespace %q (%v), ca.crt %v; want the directory: %v", ns, hasNS, hasCA, tc.wantDir)
			}
			if hasToken != tc.wantToken || len(*seen) != map[bool]int{true: 1, false: 0}[tc.wantToken] || (hasToken && token != "minted-for-default") {
				t.Errorf("token %q (%v), %d TokenRequests; want a token: %v", token, hasToken, len(*seen), tc.wantToken)
			}
		})
	}
}
