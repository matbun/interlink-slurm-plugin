package shadow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os/exec"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// sha256 of pkg/virtualkubelet/templates/reverse-relay.py in interLink.
const upstreamRelaySHA256 = "efde504969f566811c614d30120c387c287098863570cfcfc71bb7983049c5c0"

const (
	reverseTestService = "mlflow.alice.svc.cluster.local"
	reverseTestBind    = "127.77.1.2"
	reverseTestJob     = testUID
)

func reverseConfig() ReverseConfig {
	cfg := ReverseConfig{Enabled: true}
	if err := NormalizeReverseConfig(&cfg); err != nil {
		panic(err)
	}
	return cfg
}

func TestRelayIsTheVirtualKubelets(t *testing.T) {
	sum := sha256.Sum256([]byte(reverseRelay))
	if got := hex.EncodeToString(sum[:]); got != upstreamRelaySHA256 {
		t.Fatalf("templates/reverse-relay.py has sha256 %s, want %s: it must stay byte-identical to the virtual kubelet's", got, upstreamRelaySHA256)
	}
}

func TestParseReverseTunnel(t *testing.T) {
	cfg := reverseConfig()
	rt, err := ParseReverseTunnel(map[string]string{AnnReverseForwards: reverseTestService + ":5000"}, testUID+"-0", reverseTestJob, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := &ReverseTunnel{
		Forwards: []ReverseForward{{ListenPort: 5000, Host: reverseTestService, Port: 5000}},
		Bind:     reverseBindAddress(testUID + "-0"),
		NodeExec: ReverseNodeExecSSH, Sshd: ReverseSshdNode, SshdPath: DefaultReverseSshdPath, PythonPath: DefaultReversePythonPath,
		JobName: reverseTestJob,
	}
	if !reflect.DeepEqual(rt, want) {
		t.Fatalf("parsed\n%+v\nwant\n%+v", rt, want)
	}

	if rt, err := ParseReverseTunnel(map[string]string{"interlink.eu/other": "x"}, testUID, reverseTestJob, cfg); rt != nil || err != nil {
		t.Fatalf("no reverse annotations: %+v, %v", rt, err)
	}

	socks := cfg
	socks.AllowSocks = true
	rt, err = ParseReverseTunnel(map[string]string{
		AnnReverseForwards:  "6443:kubernetes.default.svc.cluster.local:443 redis.alice.svc:6379",
		AnnReverseSocksPort: "1080",
		AnnReverseBind:      reverseTestBind,
	}, testUID, reverseTestJob, socks)
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.Forwards) != 2 || rt.Forwards[0] != (ReverseForward{6443, "kubernetes.default.svc.cluster.local", 443}) || rt.SocksPort != 1080 || rt.Bind != reverseTestBind {
		t.Fatalf("remapped port, socks and bind: %+v", rt)
	}

	allowed := cfg
	allowed.AllowedTargets = []string{"MLflow.alice.svc.cluster.local:443", "redis.alice.svc:6379"}
	if _, err := ParseReverseTunnel(map[string]string{AnnReverseForwards: "8443:mlflow.alice.svc.cluster.local:443 redis.alice.svc:6379"}, testUID, reverseTestJob, allowed); err != nil {
		t.Fatalf("listed targets refused: %v", err)
	}

	disabled := cfg
	disabled.Enabled = false
	for _, tt := range []struct {
		name        string
		annotations map[string]string
		cfg         ReverseConfig
		want        string
	}{
		{"reverse disabled", map[string]string{AnnReverseForwards: reverseTestService + ":5000"}, disabled, "Shadow.Reverse.Enabled"},
		{"socks not allowed", map[string]string{AnnReverseSocksPort: "1080"}, cfg, "Shadow.Reverse.AllowSocks"},
		{"target not allowed", map[string]string{AnnReverseForwards: "8443:mlflow.alice.svc.cluster.local:443 redis.bob.svc:6379"}, allowed, "target redis.bob.svc:6379 is not in Shadow.Reverse.AllowedTargets"},
		{"malformed forward", map[string]string{AnnReverseForwards: "minio"}, cfg, "expected host:port"},
		{"privileged port without a listen port", map[string]string{AnnReverseForwards: "kubernetes.default.svc:443"}, cfg, "below 1024"},
		{"host that is not a name", map[string]string{AnnReverseForwards: "minio;rm:9000"}, cfg, "not a hostname"},
		{"two forwards on one listen port", map[string]string{AnnReverseForwards: "a.alice.svc:9000 9000:b.alice.svc:9001"}, cfg, "both listen on port 9000"},
		{"socks on a forward's port", map[string]string{AnnReverseForwards: "a.alice.svc:9000", AnnReverseSocksPort: "9000"}, socks, "already used"},
		{"socks on a privileged port", map[string]string{AnnReverseSocksPort: "80"}, socks, "below 1024"},
		{"bind outside loopback", map[string]string{AnnReverseForwards: "a.alice.svc:9000", AnnReverseBind: "10.0.0.1"}, cfg, "127.x.y.z"},
		{"bind on the shared loopback address", map[string]string{AnnReverseForwards: "a.alice.svc:9000", AnnReverseBind: "127.0.0.1"}, cfg, "127.x.y.z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseReverseTunnel(tt.annotations, testUID, reverseTestJob, tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %v, want %q", err, tt.want)
			}
		})
	}
}

func TestReverseBindAddress(t *testing.T) {
	a := reverseBindAddress(testUID + "-0")
	b := reverseBindAddress(testUID + "-1")
	if a != reverseBindAddress(testUID+"-0") || a == b {
		t.Fatalf("bind addresses %s and %s: stable per UID, distinct across UIDs", a, b)
	}
	re := regexp.MustCompile(`^127\.([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-4])\.[0-9]{1,3}\.([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-4])$`)
	for _, addr := range []string{a, b} {
		if !re.MatchString(addr) {
			t.Errorf("%s is in 127.0.0.0/24 or ends in .0 or .255", addr)
		}
	}
}

func assertBashParses(t *testing.T, script string) {
	t.Helper()
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Log("bash not found, not checking the pre-exec")
		return
	}
	cmd := exec.Command(path, "-n")
	cmd.Stdin = strings.NewReader(script)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Errorf("bash -n on the pre-exec: %v: %s", err, stderr.String())
	}
}

func TestReversePreExec(t *testing.T) {
	rt := &ReverseTunnel{
		Forwards: []ReverseForward{
			{ListenPort: 5000, Host: reverseTestService, Port: 5000},
			{ListenPort: 6379, Host: "redis.other.svc.cluster.local", Port: 6379},
		},
		Bind:       reverseTestBind,
		PythonPath: DefaultReversePythonPath,
	}
	script := ReversePreExec(rt, testNamespace)
	for _, want := range []string{
		"echo '127.77.1.2 mlflow.alice.svc.cluster.local mlflow.alice.svc mlflow.alice mlflow redis.other.svc.cluster.local redis.other.svc redis.other'",
		`export SINGULARITY_BIND="${SINGULARITY_BIND:+$SINGULARITY_BIND,}$il_rev_hosts:/etc/hosts"`,
		`mktemp "${TMPDIR:-/tmp}/interlink-hosts.XXXXXX"`,
		"needs a plugin with SocketDirRoot set",
		`python3 -I "$il_rev_d/rev-relay.py" 127.77.1.2 5000="$il_rev_d/rev-5000.sock" 6379="$il_rev_d/rev-6379.sock" &`,
		`[ -S "$il_rev_d/rev-6379.sock" ]`,
		"peer_uid",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("pre-exec lacks %q", want)
		}
	}
	if strings.Contains(script, "il_rev_owner") {
		t.Errorf("SOCKS wait without SOCKS")
	}
	assertBashParses(t, script)

	socksOnly := ReversePreExec(&ReverseTunnel{SocksPort: 1080, Bind: reverseTestBind, PythonPath: DefaultReversePythonPath}, testNamespace)
	if !strings.Contains(socksOnly, `until [ "$(il_rev_owner 0438)" = own ]; do`) || strings.Contains(socksOnly, "/etc/hosts") {
		t.Errorf("SOCKS-only pre-exec:\n%s", socksOnly)
	}
	assertBashParses(t, socksOnly)
}

// A job that only opens connections into the cluster gets a shadow with the
// ssh-reverse container and no Service; once it ended, the shadow keeps job-logs only.
func TestEnsureReverseOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Reverse.Enabled = true
	client, m := newTestManager(t, cfg)
	spec := notebookSpec()
	spec.Template = podTemplate()
	spec.Template.Annotations[AnnReverseForwards] = reverseTestService + ":5000"
	rt, err := ParseReverseTunnel(spec.Template.Annotations, testUID+"-0", reverseTestJob, reverseConfig())
	if err != nil {
		t.Fatal(err)
	}
	spec.Reverse = rt

	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Service != "" {
		t.Errorf("a reverse-only shadow has Service %q", ref.Service)
	}
	d := getDeployment(t, client, ref.Deployment)
	if names := containerNames(d.Spec.Template.Spec.Containers); !slices.Equal(names, []string{"ssh-reverse", jobLogsContainer}) {
		t.Fatalf("containers %v", names)
	}
	reverse := container(t, d, "ssh-reverse")
	env := map[string]string{}
	for _, e := range reverse.Env {
		env[e.Name] = e.Value
	}
	if env["REV_FORWARDS"] != "5000:"+reverseTestService+":5000" || env["REV_JOB_NAME"] != reverseTestJob || env["REV_NODE_EXEC"] != ReverseNodeExecSSH {
		t.Errorf("ssh-reverse env %v", env)
	}
	if !strings.Contains(reverse.Args[0], "read_sockdir") {
		t.Errorf("ssh-reverse does not read the socket directory")
	}
	assertShellParses(t, reverse)
	if !hasInitWaitingForSocketDir(d.Spec.Template.Spec.InitContainers) {
		t.Errorf("the shadow does not wait for the socket directory: %v", containerNames(d.Spec.Template.Spec.InitContainers))
	}

	spec.Terminal = true
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)

	off := testConfig()
	_, m = newTestManager(t, off)
	spec.Terminal = false
	if _, err := m.Ensure(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "Shadow.Reverse.Enabled") {
		t.Fatalf("reverse tunnel with Reverse off: %v", err)
	}
}

func hasInitWaitingForSocketDir(cs []corev1.Container) bool {
	for _, c := range cs {
		if strings.Contains(strings.Join(c.Args, " "), "socket-dir") {
			return true
		}
	}
	return false
}
