package slurm

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

func shadowBlock() ShadowConfig {
	return ShadowConfig{
		Enabled:       true,
		ExecutorImage: "registry.example.org/executor:test",
		ExecutorURL:   "http://kueue-offload-executor:8000",
		KeySecret:     "hpc-a-tunnel-key",
	}
}

func TestShadowConfigTakesSiteDefaults(t *testing.T) {
	c := SlurmConfig{
		Sites:  map[string]SiteConfig{executorSite: {LoginHost: "login.hpc-a.example.org", User: "alice", Port: 2222}},
		Shadow: shadowBlock(),
	}
	cfg := c.shadowConfig()
	if cfg.LoginHost != "login.hpc-a.example.org" || cfg.User != "alice" || cfg.Port != 2222 {
		t.Fatalf("site defaults not taken: %+v", cfg)
	}

	c.Shadow.LoginHost, c.Shadow.User, c.Shadow.Port = "gw.hpc-a.example.org", "tunnel", 22
	c.Shadow.ForwardMode, c.Shadow.ExecConnectCommand = "exec", "socat - TCP:%h:%p"
	cfg = c.shadowConfig()
	if cfg.LoginHost != "gw.hpc-a.example.org" || cfg.User != "tunnel" || cfg.Port != 22 || cfg.ForwardMode != "exec" || cfg.ExecConnectCommand != "socat - TCP:%h:%p" {
		t.Fatalf("Shadow block values not kept: %+v", cfg)
	}
}

func TestValidateExecutorConfigChecksShadow(t *testing.T) {
	t.Setenv(shadow.ExecutorImageEnv, "")
	base := func() SlurmConfig {
		return SlurmConfig{
			DataRootFolder: "/data/",
			Executor:       ExecutorConfig{Enabled: true, Namespace: executorNS, Auth: ExecutorAuthConfig{Mode: ExecutorAuthNetwork}},
			Sites: map[string]SiteConfig{executorSite: {
				LoginHost: "login.hpc-a.example.org", User: "alice", AllowedAccounts: []string{executorAccount},
			}},
			Shadow: shadowBlock(),
		}
	}
	c := base()
	if err := validateExecutorConfig(&c); err != nil {
		t.Fatalf("valid Shadow block refused: %v", err)
	}

	bad := map[string]func(*SlurmConfig){
		"no executor URL":      func(c *SlurmConfig) { c.Shadow.ExecutorURL = "" },
		"no executor image":    func(c *SlurmConfig) { c.Shadow.ExecutorImage = "" },
		"no key Secret":        func(c *SlurmConfig) { c.Shadow.KeySecret = "" },
		"bad forward mode":     func(c *SlurmConfig) { c.Shadow.ForwardMode = "socks" },
		"bad node wait":        func(c *SlurmConfig) { c.Shadow.NodeWaitTimeout = "soon" },
		"user unsafe in shell": func(c *SlurmConfig) { c.Shadow.User = "alice;rm" },
	}
	for name, mutate := range bad {
		c := base()
		mutate(&c)
		if err := validateExecutorConfig(&c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	t.Setenv(shadow.ExecutorImageEnv, "registry.example.org/executor:env")
	c = base()
	c.Shadow.ExecutorImage = ""
	if err := validateExecutorConfig(&c); err != nil {
		t.Fatalf("ExecutorImage from %s refused: %v", shadow.ExecutorImageEnv, err)
	}
}

// TestExecutorWithShadowManager runs a workload through the executor API with the
// real shadow manager on a fake cluster: the shadow appears once the job runs,
// loses its tunnel when the job ends, and goes with DELETE.
func TestExecutorWithShadowManager(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Sidecar.Config.Shadow = shadowBlock()
	m, err := NewShadowManager(e.kube, e.d.Sidecar.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	ctx := context.Background()
	deployments := e.kube.AppsV1().Deployments(executorNS)
	services := e.kube.CoreV1().Services(executorNS)
	configMaps := e.kube.CoreV1().ConfigMaps(executorNS)
	selector := metav1.ListOptions{LabelSelector: shadow.LabelShadowOf + "=" + executorWorkload}

	req := submitRequest(1)
	req.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	if st := e.jobStatus(); st.Shadow != nil {
		t.Fatalf("queued job has a shadow: %+v", st.Shadow)
	}
	if list, _ := deployments.List(ctx, selector); len(list.Items) != 0 {
		t.Fatalf("queued job got %d shadow Deployments", len(list.Items))
	}

	e.setState("R None", "0 R")
	if err := os.WriteFile(e.memberDir(0)+"/compute-node", []byte("node07\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st := e.jobStatus()
	if st.State != JobRunning || st.Shadow == nil || st.Shadow.Deployment == "" || st.Shadow.Service != st.Shadow.Deployment || st.Shadow.NodeConfigMap == "" {
		t.Fatalf("running: state %s, shadow %+v", st.State, st.Shadow)
	}
	dep, err := deployments.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if dep.Labels[shadow.LabelShadowOf] != executorWorkload {
		t.Errorf("Deployment labels: %v", dep.Labels)
	}
	forward, logs := shadowContainers(t, dep)
	if !strings.Contains(strings.Join(forward.Args, " ")+strings.Join(forward.Command, " "), "alice@login.hpc-a.example.org") {
		t.Errorf("ssh-forward does not reach the site's login node as its user:\n%v %v", forward.Command, forward.Args)
	}
	if logs.Image != "registry.example.org/executor:test" || !strings.Contains(strings.Join(logs.Command, " "), "--workload "+executorWorkload) {
		t.Errorf("job-logs: image %s, command %v", logs.Image, logs.Command)
	}
	if _, err := services.Get(ctx, st.Shadow.Service, metav1.GetOptions{}); err != nil {
		t.Fatalf("Service of a running job with a port: %v", err)
	}
	cm, err := configMaps.Get(ctx, st.Shadow.NodeConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["compute-node"] != "node07" {
		t.Fatalf("node ConfigMap: %v", cm.Data)
	}

	raw, err := os.ReadFile(e.d.recordPath(executorWorkload))
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Shadow map[string]string `json:"shadow"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Shadow["deployment"] != st.Shadow.Deployment {
		t.Fatalf("record keeps the shadow as %s (%v)", raw, err)
	}

	e.setState("CD None", "0 CD")
	if err := os.WriteFile(e.memberDir(0)+"/run-"+executorCtn+".status", []byte("0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st = e.jobStatus()
	if st.State != JobCompleted || st.Shadow == nil || st.Shadow.Service != "" {
		t.Fatalf("completed: state %s, shadow %+v", st.State, st.Shadow)
	}
	if _, err := services.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the Service outlived the job: %v", err)
	}
	dep, err = deployments.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the logs-only shadow is gone: %v", err)
	}
	if names := containerNames(dep); names != "job-logs" {
		t.Fatalf("logs-only shadow runs %s", names)
	}

	if rec := e.do(http.MethodDelete, "/jobs/"+executorWorkload, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d %s", rec.Code, rec.Body.String())
	}
	if list, _ := deployments.List(ctx, selector); len(list.Items) != 0 {
		t.Errorf("%d shadow Deployments left after DELETE", len(list.Items))
	}
	if list, _ := configMaps.List(ctx, selector); len(list.Items) != 0 {
		t.Errorf("%d shadow ConfigMaps left after DELETE", len(list.Items))
	}
}

func shadowContainers(t *testing.T, dep *appsv1.Deployment) (forward, logs v1.Container) {
	t.Helper()
	var haveForward, haveLogs bool
	for _, c := range dep.Spec.Template.Spec.Containers {
		switch c.Name {
		case "ssh-forward":
			forward, haveForward = c, true
		case "job-logs":
			logs, haveLogs = c, true
		}
	}
	if !haveForward || !haveLogs {
		t.Fatalf("shadow containers: %s", containerNames(dep))
	}
	return forward, logs
}

func containerNames(dep *appsv1.Deployment) string {
	var names []string
	for _, c := range dep.Spec.Template.Spec.Containers {
		names = append(names, c.Name)
	}
	return strings.Join(names, ",")
}
