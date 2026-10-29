package shadow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// sha256 of pkg/virtualkubelet/templates/shadow-ssh-template.yaml in interLink.
const upstreamTemplateSHA256 = "6f713de0bd899fc1e2e7c7d428ab94f92444374beb6de9a2d53d7610ec524f10"

const (
	testNamespace = "alice"
	testUID       = "0b6c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"
	testName      = "nb"
	testShadow    = "shadow-nb-alice"
	testSocketDir = "/scratch/alice/interlink/sockets/abc"
)

func testConfig() Config {
	return Config{
		AgentImage:          "registry.example.org/interlink-agent:1.0",
		AgentURL:            "http://interlink-agent:8000",
		LoginHost:           "login.hpc-a.example.org",
		User:                "alice",
		KeySecret:           "hpc-a-tunnel-key",
		KnownHostsConfigMap: "hpc-a-known-hosts",
		ForwardTarget:       ForwardTargetNode,
	}
}

func podTemplate(ports ...int32) corev1.PodTemplateSpec {
	c := corev1.Container{Name: "main", Image: "jupyter/base-notebook"}
	for _, p := range ports {
		c.Ports = append(c.Ports, corev1.ContainerPort{ContainerPort: p})
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"kueue.x-k8s.io/queue-name": "proj42"},
			Annotations: map[string]string{},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{c}},
	}
}

func notebookSpec() Spec {
	return Spec{
		WorkloadUID:  testUID,
		WorkloadName: testName,
		Namespace:    testNamespace,
		Template:     podTemplate(8888),
		NodeName:     "node07",
		SocketDir:    testSocketDir,
	}
}

func newTestManager(t *testing.T, cfg Config, objects ...runtime.Object) (*fake.Clientset, Manager) {
	t.Helper()
	client := fake.NewClientset(objects...)
	if err := NormalizeConfig(&cfg); err != nil {
		t.Fatalf("test config: %v", err)
	}
	return client, NewManager(client, testNamespace, cfg)
}

func getDeployment(t *testing.T, client *fake.Clientset, name string) *appsv1.Deployment {
	t.Helper()
	d, err := client.AppsV1().Deployments(testNamespace).Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment %s: %v", name, err)
	}
	return d
}

func containerNames(cs []corev1.Container) []string {
	var names []string
	for _, c := range cs {
		names = append(names, c.Name)
	}
	return names
}

func container(t *testing.T, d *appsv1.Deployment, name string) corev1.Container {
	t.Helper()
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no container %s in %v", name, containerNames(d.Spec.Template.Spec.Containers))
	return corev1.Container{}
}

func assertOwnerLabels(t *testing.T, kind string, labels map[string]string, uid string) {
	t.Helper()
	if labels[LabelShadow] != "true" || labels[LabelShadowOf] != uid {
		t.Errorf("%s labels %v: want %s=true and %s=%s", kind, labels, LabelShadow, LabelShadowOf, uid)
	}
}

// writes returns the actions other than get and list.
func writes(client *fake.Clientset) []string {
	var out []string
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" || a.GetVerb() == "list" {
			continue
		}
		out = append(out, a.GetVerb()+" "+a.GetResource().Resource)
	}
	return out
}

func TestTemplateIsTheVirtualKubelets(t *testing.T) {
	sum := sha256.Sum256([]byte(sshTemplate))
	if got := hex.EncodeToString(sum[:]); got != upstreamTemplateSHA256 {
		t.Fatalf("templates/shadow-ssh-template.yaml has sha256 %s, want %s: it must stay byte-identical to the virtual kubelet's", got, upstreamTemplateSHA256)
	}
}

func TestEnsureNotebook(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	ref, err := m.Ensure(t.Context(), notebookSpec())
	if err != nil {
		t.Fatal(err)
	}
	want := Ref{Deployment: testShadow, Service: testShadow, NodeConfigMap: testShadow + "-node"}
	if ref != want {
		t.Fatalf("ref %+v, want %+v", ref, want)
	}

	d := getDeployment(t, client, testShadow)
	assertOwnerLabels(t, "deployment", d.Labels, testUID)
	assertOwnerLabels(t, "pod template", d.Spec.Template.Labels, testUID)
	if _, ok := d.Spec.Template.Labels["kueue.x-k8s.io/queue-name"]; ok {
		t.Error("the shadow must not carry the workload's queue label")
	}
	if got := containerNames(d.Spec.Template.Spec.InitContainers); !slices.Equal(got, []string{"wait-for-node"}) {
		t.Errorf("init containers %v", got)
	}
	if got := containerNames(d.Spec.Template.Spec.Containers); !slices.Equal(got, []string{"ssh-forward", "job-logs"}) {
		t.Errorf("containers %v", got)
	}

	forward := container(t, d, "ssh-forward")
	script := forward.Args[0]
	for _, s := range []string{`-L "0.0.0.0:8888:127.0.0.1:8888"`, "target=alice@login.hpc-a.example.org", "-i /interlink/ssh/ssh-privatekey"} {
		if !strings.Contains(script, s) {
			t.Errorf("ssh-forward script lacks %q", s)
		}
	}
	if forward.ReadinessProbe == nil || forward.ReadinessProbe.TCPSocket.Port.IntVal != 8888 {
		t.Errorf("readiness probe %+v, want tcp 8888", forward.ReadinessProbe)
	}

	logs := container(t, d, "job-logs")
	wantCmd := []string{"/sidecar/slurm-sidecar", "logs", "--agent", "http://interlink-agent:8000", "--workload", testUID, "--follow"}
	if !slices.Equal(logs.Command, wantCmd) {
		t.Errorf("job-logs command %v, want %v", logs.Command, wantCmd)
	}
	if logs.Image != "registry.example.org/interlink-agent:1.0" {
		t.Errorf("job-logs image %s", logs.Image)
	}
	r := logs.Resources
	if r.Requests.Cpu().String() != "10m" || r.Requests.Memory().String() != "32Mi" ||
		r.Limits.Cpu().String() != "100m" || r.Limits.Memory().String() != "64Mi" {
		t.Errorf("job-logs resources %+v", r)
	}

	var volumes []string
	for _, v := range d.Spec.Template.Spec.Volumes {
		volumes = append(volumes, v.Name)
	}
	if !slices.Equal(volumes, []string{"shadow-node", "shadow-ssh-key", "shadow-known-hosts"}) {
		t.Errorf("volumes %v", volumes)
	}
	if got := d.Spec.Template.Spec.Volumes[0].ConfigMap.Name; got != testShadow+"-node" {
		t.Errorf("node volume mounts %s", got)
	}

	svc, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerLabels(t, "service", svc.Labels, testUID)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8888 || svc.Spec.Ports[0].TargetPort.IntVal != 8888 {
		t.Errorf("service ports %+v", svc.Spec.Ports)
	}
	if svc.Spec.Selector["app.kubernetes.io/component"] != testShadow {
		t.Errorf("service selector %v", svc.Spec.Selector)
	}

	cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerLabels(t, "configmap", cm.Labels, testUID)
	if cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != testSocketDir {
		t.Errorf("configmap data %v", cm.Data)
	}
}

func assertLogsOnly(t *testing.T, client *fake.Clientset, ref Ref) {
	t.Helper()
	if ref.Service != "" {
		t.Errorf("logs-only ref names service %q", ref.Service)
	}
	d := getDeployment(t, client, testShadow)
	if got := containerNames(d.Spec.Template.Spec.Containers); !slices.Equal(got, []string{"job-logs"}) {
		t.Errorf("logs-only containers %v", got)
	}
	if len(d.Spec.Template.Spec.InitContainers) != 0 {
		t.Errorf("logs-only init containers %v", containerNames(d.Spec.Template.Spec.InitContainers))
	}
	// Nothing mounts the tunnel key, so a missing Secret cannot hold the pod.
	if len(d.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("logs-only volumes %+v", d.Spec.Template.Spec.Volumes)
	}
	assertOwnerLabels(t, "pod template", d.Spec.Template.Labels, testUID)
	_, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("logs-only shadow has a service (err %v)", err)
	}
}

func TestEnsureWithoutPortsIsLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template = podTemplate()
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
	if ref.NodeConfigMap != testShadow+"-node" {
		t.Errorf("ref %+v", ref)
	}
}

func TestEnsureUDPOnlyIsLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template = podTemplate()
	spec.Template.Annotations[annExtraPorts] = "5353:dns:UDP"
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
}

func TestAlwaysShadowFalse(t *testing.T) {
	cfg := testConfig()
	never := false
	cfg.AlwaysShadow = &never
	client, m := newTestManager(t, cfg)

	spec := notebookSpec()
	spec.Template = podTemplate()
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if ref != (Ref{}) || len(writes(client)) != 0 {
		t.Fatalf("a job without ports got a shadow: ref %+v, writes %v", ref, writes(client))
	}

	if _, err := m.Ensure(t.Context(), notebookSpec()); err != nil {
		t.Fatal(err)
	}
	getDeployment(t, client, testShadow)
}

func TestTerminalFlipsToLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	spec.Terminal = true
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
	if ref.Deployment != testShadow {
		t.Errorf("ref %+v", ref)
	}
}

func TestEnsureTwiceWritesNothing(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	// Two containers and the extra-ports annotation: the order the ports are
	// gathered in must not change the rendering.
	spec.Template.Spec.Containers = append(spec.Template.Spec.Containers, corev1.Container{
		Name: "metrics", Ports: []corev1.ContainerPort{{ContainerPort: 9100}, {ContainerPort: 6006}},
	})
	spec.Template.Annotations[annExtraPorts] = "7000:api,5353:dns:UDP"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	before := getDeployment(t, client, testShadow)
	client.ClearActions()

	for range 3 {
		if _, err := m.Ensure(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
	}
	if w := writes(client); len(w) != 0 {
		t.Fatalf("re-applying the same spec wrote %v", w)
	}
	if after := getDeployment(t, client, testShadow); after.Annotations[specHashAnnotation] != before.Annotations[specHashAnnotation] {
		t.Fatal("the deployment changed")
	}

	svc, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var ports []int32
	for _, p := range svc.Spec.Ports {
		ports = append(ports, p.Port)
	}
	if !slices.Equal(ports, []int32{6006, 7000, 8888, 9100}) {
		t.Errorf("service ports %v, want the TCP ports sorted", ports)
	}
}

func TestNodeChangeUpdatesConfigMapOnly(t *testing.T) {
	shadowPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: testShadow + "-abc", Namespace: testNamespace,
		Labels: map[string]string{"app.kubernetes.io/component": testShadow, LabelShadowOf: testUID},
	}}
	client, m := newTestManager(t, testConfig(), shadowPod)
	spec := notebookSpec()
	spec.NodeName, spec.SocketDir = "", ""
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	hash := getDeployment(t, client, testShadow).Annotations[specHashAnnotation]

	for _, node := range []string{"node07", "node08"} {
		client.ClearActions()
		spec.NodeName, spec.SocketDir = node, testSocketDir+"-"+node
		if _, err := m.Ensure(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
		if w := writes(client); !slices.Equal(w, []string{"update configmaps", "patch pods"}) {
			t.Fatalf("node %s: writes %v, want the configmap updated and the shadow pod touched", node, w)
		}
		cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if cm.Data["compute-node"] != node || cm.Data["socket-dir"] != spec.SocketDir {
			t.Errorf("configmap data %v", cm.Data)
		}
		pod, err := client.CoreV1().Pods(testNamespace).Get(t.Context(), shadowPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if pod.Annotations[nodeAnnotation] != node {
			t.Errorf("shadow pod annotations %v", pod.Annotations)
		}
	}
	if got := getDeployment(t, client, testShadow).Annotations[specHashAnnotation]; got != hash {
		t.Error("a node change changed the deployment")
	}
}

func TestDeleteRemovesAll(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	if _, err := m.Ensure(t.Context(), notebookSpec()); err != nil {
		t.Fatal(err)
	}
	other := notebookSpec()
	other.WorkloadUID, other.WorkloadName = "11111111-2222-3333-4444-555555555555", "train"
	if _, err := m.Ensure(t.Context(), other); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := m.Delete(t.Context(), testUID); err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	if _, err := client.AppsV1().Deployments(testNamespace).Get(ctx, testShadow, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("deployment left: %v", err)
	}
	if _, err := client.CoreV1().Services(testNamespace).Get(ctx, testShadow, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("service left: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testShadow+"-node", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("configmap left: %v", err)
	}

	getDeployment(t, client, "shadow-train-alice")
	if _, err := client.CoreV1().Services(testNamespace).Get(ctx, "shadow-train-alice", metav1.GetOptions{}); err != nil {
		t.Errorf("another workload's service went: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, "shadow-train-alice-node", metav1.GetOptions{}); err != nil {
		t.Errorf("another workload's configmap went: %v", err)
	}
}

func TestEnsureRefusesAnotherWorkloadsName(t *testing.T) {
	previous := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: testShadow, Namespace: testNamespace,
		Labels: map[string]string{LabelShadow: "true", LabelShadowOf: "a-previous-job"},
	}}
	client, m := newTestManager(t, testConfig(), previous)
	_, err := m.Ensure(t.Context(), notebookSpec())
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("err %v, want ErrNameTaken", err)
	}
	if got := getDeployment(t, client, testShadow).Labels[LabelShadowOf]; got != "a-previous-job" {
		t.Errorf("the previous job's deployment was taken over (%s)", got)
	}
}

func TestEnsureRefusesUnsafeNode(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.NodeName = "node07 -oProxyCommand=id"
	if _, err := m.Ensure(t.Context(), spec); err == nil {
		t.Fatal("a node name with a space was accepted")
	}
	if w := writes(client); len(w) != 0 {
		t.Errorf("writes %v", w)
	}
}

func TestEnsureDropsUnsafeSocketDir(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.SocketDir = "/scratch/alice/.interlink"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != "" {
		t.Errorf("configmap data %v", cm.Data)
	}
}

func TestSocketPorts(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template.Annotations[annSocketPorts] = "8888"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	d := getDeployment(t, client, testShadow)
	if s := container(t, d, "ssh-forward").Args[0]; !strings.Contains(s, `-L "0.0.0.0:8888:$sd/fwd-8888.sock"`) {
		t.Errorf("socket port not forwarded to its socket:\n%s", s)
	}
	if s := d.Spec.Template.Spec.InitContainers[0].Args[0]; !strings.Contains(s, "/interlink/node/socket-dir") {
		t.Errorf("wait-for-node does not wait for the socket directory:\n%s", s)
	}

	for _, c := range []struct{ target, value string }{
		{ForwardTargetLogin, "8888"},
		{ForwardTargetNode, "9999"},
		{ForwardTargetNode, "http"},
	} {
		cfg := testConfig()
		cfg.ForwardTarget = c.target
		_, m := newTestManager(t, cfg)
		spec := notebookSpec()
		spec.Template.Annotations[annSocketPorts] = c.value
		if _, err := m.Ensure(t.Context(), spec); err == nil {
			t.Errorf("target %s, socket ports %q: accepted", c.target, c.value)
		}
	}
}

// TestTemplateRendersEveryMode renders every forward target, forward mode and host
// key check the virtual kubelet supports with public key auth, and runs each
// container script through the shell parser.
func TestTemplateRendersEveryMode(t *testing.T) {
	for _, target := range []string{ForwardTargetLogin, ForwardTargetNode} {
		for _, mode := range []string{ForwardModePortForward, ForwardModeExec} {
			for _, keys := range []string{NodeHostKeysVerify, NodeHostKeysAcceptNew} {
				t.Run(target+"/"+mode+"/"+keys, func(t *testing.T) {
					cfg := testConfig()
					cfg.ForwardTarget, cfg.ForwardMode, cfg.NodeHostKeys = target, mode, keys
					if err := NormalizeConfig(&cfg); err != nil {
						t.Fatal(err)
					}
					spec := notebookSpec()
					spec.Template = podTemplate(8888, 6006)
					ports := exposedPorts(spec.Template)
					if target == ForwardTargetNode {
						if err := markSocketPorts("6006", target, ports); err != nil {
							t.Fatal(err)
						}
					}
					d, svc, err := render(cfg, spec, testShadow, ports, false)
					if err != nil {
						t.Fatal(err)
					}
					if svc == nil {
						t.Fatal("no service")
					}
					pod := d.Spec.Template.Spec
					for _, c := range append(pod.InitContainers, pod.Containers...) {
						assertShellParses(t, c)
					}
					script := container(t, d, "ssh-forward").Args[0]
					var want string
					switch {
					case mode == ForwardModeExec:
						want = "connect_cmd='nc'"
					case target == ForwardTargetLogin:
						want = `-L "0.0.0.0:8888:$node:8888"`
					default:
						want = "-W %s:22"
					}
					if !strings.Contains(script, want) {
						t.Errorf("script lacks %q", want)
					}
				})
			}
		}
	}
}

// assertShellParses runs a container's script through `sh -n` and `bash -n`.
func assertShellParses(t *testing.T, c corev1.Container) {
	t.Helper()
	if len(c.Args) == 0 {
		return
	}
	for _, shell := range []string{"sh", "bash"} {
		path, err := exec.LookPath(shell)
		if err != nil {
			t.Logf("%s not found, not checking the %s script", shell, c.Name)
			continue
		}
		cmd := exec.Command(path, "-n")
		cmd.Stdin = strings.NewReader(c.Args[0])
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("%s -n on the %s script: %v: %s", shell, c.Name, err, stderr.String())
		}
	}
}

// The expected names come from the virtual kubelet's
// computeShadowResourceNamesForSameNamespace.
func TestResourceName(t *testing.T) {
	for _, c := range []struct{ name, namespace, want string }{
		{"nb", "alice", "shadow-nb-alice"},
		{"web.0", "alice", "shadow-web-0-alice"},
		{"train-a-very-long-job-name-that-goes-on-and-on-forever", "alice", "shadow-train-a-very-long-j-b329a129-alice"},
		{"nb", "a-namespace-whose-name-is-rather-long-too", "shadow-nb-a-1bcf97dc"},
		{"Job_With.Caps", "proj42-team", "shadow-job-with-caps-proj42-team"},
		{"x-really-long-name-of-a-pytorch-distributed-training-job-0123456789", "a-namespace-whose-name-is-rather-long-too-yes", "shadow-x-050adff1"},
	} {
		if got := resourceName(c.name, c.namespace); got != c.want {
			t.Errorf("resourceName(%q, %q) = %q, want %q", c.name, c.namespace, got, c.want)
		}
	}
}

func TestNormalizeConfig(t *testing.T) {
	t.Setenv(AgentImageEnv, "registry.example.org/interlink-agent:from-env")
	cfg := testConfig()
	cfg.AgentImage = ""
	cfg.ForwardTarget = ""
	if err := NormalizeConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	want := Config{
		Image: DefaultImage, AgentImage: "registry.example.org/interlink-agent:from-env",
		AgentURL: cfg.AgentURL, LoginHost: cfg.LoginHost, User: cfg.User, Port: 22,
		KeySecret: cfg.KeySecret, KeySecretKey: "ssh-privatekey", KnownHostsConfigMap: cfg.KnownHostsConfigMap,
		NodeHostKeys: NodeHostKeysVerify, ForwardTarget: ForwardTargetLogin, ForwardMode: ForwardModePortForward,
		NodeWaitTimeout: "2h",
		Reverse:         ReverseConfig{NodeExec: ReverseNodeExecSSH, Sshd: ReverseSshdNode, SshdPath: DefaultReverseSshdPath, PythonPath: DefaultReversePythonPath},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("normalized\n%+v\nwant\n%+v", cfg, want)
	}
	if !cfg.alwaysShadow() || cfg.nodeWaitSeconds() != 7200 {
		t.Errorf("alwaysShadow %v, nodeWaitSeconds %d", cfg.alwaysShadow(), cfg.nodeWaitSeconds())
	}

	execMode := testConfig()
	execMode.ForwardMode = "Exec"
	if err := NormalizeConfig(&execMode); err != nil || execMode.ForwardMode != ForwardModeExec || execMode.ExecConnectCommand != "nc" {
		t.Errorf("exec mode: %v, %+v", err, execMode)
	}

	t.Setenv(AgentImageEnv, "")
	for name, broken := range map[string]func(*Config){
		"no agent image":       func(c *Config) { c.AgentImage = "" },
		"no agent url":         func(c *Config) { c.AgentURL = "" },
		"agent url not http":   func(c *Config) { c.AgentURL = "interlink-agent:8000" },
		"no login host":        func(c *Config) { c.LoginHost = "" },
		"login host injection": func(c *Config) { c.LoginHost = "login;id" },
		"user with space":      func(c *Config) { c.User = "alice -oProxyCommand=id" },
		"no key secret":        func(c *Config) { c.KeySecret = "" },
		"bad key secret key":   func(c *Config) { c.KeySecretKey = "../id" },
		"port out of range":    func(c *Config) { c.Port = 70000 },
		"unknown mode":         func(c *Config) { c.ForwardMode = "socks" },
		"unknown target":       func(c *Config) { c.ForwardTarget = "both" },
		"unknown host keys":    func(c *Config) { c.NodeHostKeys = "trust" },
		"quoted exec command":  func(c *Config) { c.ForwardMode, c.ExecConnectCommand = ForwardModeExec, "nc'" },
		"bad wait":             func(c *Config) { c.NodeWaitTimeout = "forever" },
		"unknown node exec":    func(c *Config) { c.Reverse.NodeExec = "pbsdsh" },
		"unknown sshd":         func(c *Config) { c.Reverse.Sshd = "dropbear" },
		"quoted srun command":  func(c *Config) { c.Reverse.NodeExec, c.ExecConnectCommand = ReverseNodeExecSrun, "nc'" },
		"target without port":  func(c *Config) { c.Reverse.AllowedTargets = []string{"mlflow.alice.svc.cluster.local"} },
		"target injection":     func(c *Config) { c.Reverse.AllowedTargets = []string{"a;id:443"} },
		"socks with targets": func(c *Config) {
			c.Reverse.AllowSocks, c.Reverse.AllowedTargets = true, []string{"mlflow.alice.svc.cluster.local:443"}
		},
		"negative wait": func(c *Config) { c.NodeWaitTimeout = "-1m" },
	} {
		cfg := testConfig()
		broken(&cfg)
		if err := NormalizeConfig(&cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEnsureWithInvalidConfigFails(t *testing.T) {
	cfg := testConfig()
	cfg.KeySecret = ""
	client := fake.NewClientset()
	m := NewManager(client, testNamespace, cfg)
	if _, err := m.Ensure(t.Context(), notebookSpec()); err == nil {
		t.Fatal("Ensure ran on an invalid configuration")
	}
	if len(client.Actions()) != 0 {
		t.Errorf("actions %v", client.Actions())
	}
}

func TestEnsureRefusesOtherNamespace(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Namespace = "bob"
	if _, err := m.Ensure(t.Context(), spec); err == nil {
		t.Fatal("a shadow in another namespace was accepted")
	}
	if len(client.Actions()) != 0 {
		t.Errorf("actions %v", client.Actions())
	}
}
