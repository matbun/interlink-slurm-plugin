package shadow

import (
	_ "embed"
	"fmt"
	"hash/fnv"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// Pod annotations asking the ssh shadow for connections back into the cluster, as
// the virtual kubelet reads them.
const (
	// AnnReverseForwards lists the cluster endpoints the job may open connections
	// to, space separated, each "host:port" or "listen-port:host:port".
	AnnReverseForwards = "interlink.eu/reverse-forwards"
	// AnnReverseSocksPort asks for a SOCKS5 proxy on the given port for endpoints
	// not listed in AnnReverseForwards.
	AnnReverseSocksPort = "interlink.eu/reverse-socks-port"
	// AnnReverseBind overrides the 127.x.y.z address of the owner-only relay
	// inside the job, normally derived from the pod UID.
	AnnReverseBind = "interlink.eu/reverse-bind"
)

// How the shadow reaches the compute node, as Network.SSH.Reverse.NodeExec.
const (
	// ReverseNodeExecSSH opens node:22 through the login node, as ForwardMode says.
	ReverseNodeExecSSH = "ssh"
	// ReverseNodeExecSrun runs `srun --jobid=<job> --overlap` on the login node,
	// inside the job's own allocation. A last resort.
	ReverseNodeExecSrun = "srun"
)

// Which sshd makes the forwards on the compute node, as Network.SSH.Reverse.Sshd.
const (
	// ReverseSshdNode is the compute node's own sshd.
	ReverseSshdNode = "node"
	// ReverseSshdUserspace is an sshd the shadow starts in the job as the job's
	// user, for sites whose node sshd refuses forwarding.
	ReverseSshdUserspace = "userspace"
)

const (
	// DefaultReverseSshdPath is where sshd is looked for on the compute node.
	DefaultReverseSshdPath = "/usr/sbin/sshd"
	// DefaultReversePythonPath runs the job's owner-only relay.
	DefaultReversePythonPath = "python3"
	// reverseTunnelWaitSeconds bounds how long the job waits for its tunnel before
	// giving up. The tunnel follows the compute node name, which the shadow learns a
	// status poll or two after the job starts.
	reverseTunnelWaitSeconds = 600
	// reversePrivilegedPortLimit: an unprivileged sshd cannot bind below it.
	reversePrivilegedPortLimit = 1024
)

// ReverseConfig is the executor's Shadow.Reverse block: the virtual kubelet's
// Network.SSH.Reverse plus AllowedTargets.
type ReverseConfig struct {
	// Enabled lets workloads ask for it (default false). A workload that asks
	// while this is false is refused rather than run without its tunnel.
	Enabled bool `yaml:"Enabled"`
	// Sshd is "node" (default) or "userspace".
	Sshd string `yaml:"Sshd"`
	// AllowSocks lets workloads ask for a SOCKS5 proxy with
	// interlink.eu/reverse-socks-port (default false). The proxy listens on
	// 127.0.0.1 of the compute node, where every user of that node can reach
	// through it any endpoint the shadow reaches in the cluster.
	AllowSocks bool `yaml:"AllowSocks"`
	// NodeExec is "ssh" (default) or "srun".
	NodeExec string `yaml:"NodeExec"`
	// SshdPath is the sshd binary on the compute node, for Sshd "userspace" only
	// (default "/usr/sbin/sshd").
	SshdPath string `yaml:"SshdPath"`
	// PythonPath is the python3 (3.6 or later) on the compute node that runs the
	// job's owner-only relay (default "python3").
	PythonPath string `yaml:"PythonPath"`
	// AllowedTargets, when not empty, are the only host:port a forward may name.
	AllowedTargets []string `yaml:"AllowedTargets"`
}

// ReverseForward is one cluster endpoint the job may connect to: the job dials
// ListenPort on its loopback address, the shadow dials Host:Port.
type ReverseForward struct {
	ListenPort int32
	Host       string
	Port       int32
}

// Target is the endpoint the shadow dials, host:port.
func (f ReverseForward) Target() string {
	return f.Host + ":" + strconv.Itoa(int(f.Port))
}

// ReverseTunnel carries a workload's reverse settings into the shadow template and
// the job's pre-exec. Everything in it has been validated: the template passes it
// to ssh unquoted.
type ReverseTunnel struct {
	Forwards []ReverseForward
	// SocksPort is 0 when no SOCKS proxy was asked for.
	SocksPort int32
	// Bind is the 127.x.y.z address the job's relay listens on.
	Bind string
	// NodeExec is ReverseNodeExecSSH or ReverseNodeExecSrun.
	NodeExec string
	// Sshd is ReverseSshdNode or ReverseSshdUserspace.
	Sshd       string
	SshdPath   string
	PythonPath string
	// JobName is how srun mode finds the allocation: the Slurm job's name.
	JobName string
}

var reverseHostPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)

// NormalizeReverseConfig defaults and validates Shadow.Reverse. NormalizeConfig
// calls it; it is idempotent.
func NormalizeReverseConfig(r *ReverseConfig) error {
	r.NodeExec = strings.ToLower(strings.TrimSpace(r.NodeExec))
	switch r.NodeExec {
	case "":
		r.NodeExec = ReverseNodeExecSSH
	case ReverseNodeExecSSH, ReverseNodeExecSrun:
	default:
		return fmt.Errorf("unknown Shadow.Reverse.NodeExec %q: expected %q or %q", r.NodeExec, ReverseNodeExecSSH, ReverseNodeExecSrun)
	}
	r.Sshd = strings.ToLower(strings.TrimSpace(r.Sshd))
	switch r.Sshd {
	case "":
		r.Sshd = ReverseSshdNode
	case ReverseSshdNode, ReverseSshdUserspace:
	default:
		return fmt.Errorf("unknown Shadow.Reverse.Sshd %q: expected %q or %q", r.Sshd, ReverseSshdNode, ReverseSshdUserspace)
	}
	if r.SshdPath == "" {
		r.SshdPath = DefaultReverseSshdPath
	}
	if r.PythonPath == "" {
		r.PythonPath = DefaultReversePythonPath
	}
	for _, target := range r.AllowedTargets {
		host, port, ok := strings.Cut(target, ":")
		if _, err := parseReversePort(port); !ok || err != nil || len(host) > 253 || !reverseHostPattern.MatchString(host) {
			return fmt.Errorf("Shadow.Reverse.AllowedTargets: %q is not host:port", target)
		}
	}
	// A SOCKS proxy reaches any endpoint the shadow reaches, which would make the
	// list meaningless.
	if len(r.AllowedTargets) > 0 && r.AllowSocks {
		return fmt.Errorf("Shadow.Reverse.AllowSocks cannot be combined with AllowedTargets: the SOCKS proxy reaches any target")
	}
	return nil
}

// HasReverseAnnotations reports whether the annotations ask for any reverse forward.
func HasReverseAnnotations(annotations map[string]string) bool {
	return strings.TrimSpace(annotations[AnnReverseForwards]) != "" ||
		strings.TrimSpace(annotations[AnnReverseSocksPort]) != ""
}

// ParseReverseTunnel turns a workload's reverse annotations into template data,
// rejecting anything that could not work, that the template could not pass to ssh
// safely, or that cfg does not allow. podUID derives the relay's address; jobName
// is the Slurm job's name. It returns nil, nil when nothing is asked for.
func ParseReverseTunnel(annotations map[string]string, podUID, jobName string, cfg ReverseConfig) (*ReverseTunnel, error) {
	if !HasReverseAnnotations(annotations) {
		return nil, nil
	}
	if !cfg.Enabled {
		return nil, fmt.Errorf("%s is set, but this executor does not allow the reverse tunnel (Shadow.Reverse.Enabled)", AnnReverseForwards)
	}
	if strings.TrimSpace(annotations[AnnReverseSocksPort]) != "" && !cfg.AllowSocks {
		return nil, fmt.Errorf("%s is set, but this executor does not allow a SOCKS proxy (Shadow.Reverse.AllowSocks)", AnnReverseSocksPort)
	}
	rt := &ReverseTunnel{
		NodeExec:   cfg.NodeExec,
		Sshd:       cfg.Sshd,
		SshdPath:   cfg.SshdPath,
		PythonPath: cfg.PythonPath,
		JobName:    jobName,
		Bind:       reverseBindAddress(podUID),
	}

	listening := map[int32]string{}
	for _, spec := range strings.Fields(annotations[AnnReverseForwards]) {
		f, err := parseReverseForward(spec)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", AnnReverseForwards, err)
		}
		if other, taken := listening[f.ListenPort]; taken {
			return nil, fmt.Errorf("%s: %q and %q both listen on port %d", AnnReverseForwards, other, spec, f.ListenPort)
		}
		if len(cfg.AllowedTargets) > 0 && !containsFold(cfg.AllowedTargets, f.Target()) {
			return nil, fmt.Errorf("%s: target %s is not in Shadow.Reverse.AllowedTargets", AnnReverseForwards, f.Target())
		}
		listening[f.ListenPort] = spec
		rt.Forwards = append(rt.Forwards, f)
	}

	if v := strings.TrimSpace(annotations[AnnReverseSocksPort]); v != "" {
		port, err := parseReversePort(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", AnnReverseSocksPort, err)
		}
		if port < reversePrivilegedPortLimit {
			return nil, fmt.Errorf("%s: port %d is below %d, which an unprivileged sshd cannot bind on the compute node", AnnReverseSocksPort, port, reversePrivilegedPortLimit)
		}
		if other, taken := listening[port]; taken {
			return nil, fmt.Errorf("%s: port %d is already used by forward %q", AnnReverseSocksPort, port, other)
		}
		rt.SocksPort = port
	}

	if v := strings.TrimSpace(annotations[AnnReverseBind]); v != "" {
		ip := net.ParseIP(v).To4()
		if ip == nil || ip[0] != 127 || v == "127.0.0.1" {
			return nil, fmt.Errorf("%s: %q is not a 127.x.y.z address other than 127.0.0.1", AnnReverseBind, v)
		}
		rt.Bind = ip.String()
	}

	return rt, nil
}

func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// parseReverseForward parses "host:port" or "listen-port:host:port". The listen
// port defaults to the destination port, which the job then uses unchanged; a
// destination port below 1024 needs an explicit listen port.
func parseReverseForward(spec string) (ReverseForward, error) {
	parts := strings.Split(spec, ":")
	var f ReverseForward
	var err error
	switch len(parts) {
	case 2:
		f.Host = parts[0]
		if f.Port, err = parseReversePort(parts[1]); err != nil {
			return f, fmt.Errorf("%q: %w", spec, err)
		}
		f.ListenPort = f.Port
	case 3:
		if f.ListenPort, err = parseReversePort(parts[0]); err != nil {
			return f, fmt.Errorf("%q: listen %w", spec, err)
		}
		f.Host = parts[1]
		if f.Port, err = parseReversePort(parts[2]); err != nil {
			return f, fmt.Errorf("%q: %w", spec, err)
		}
	default:
		return f, fmt.Errorf("%q: expected host:port or listen-port:host:port", spec)
	}
	if len(f.Host) > 253 || !reverseHostPattern.MatchString(f.Host) {
		return f, fmt.Errorf("%q: %q is not a hostname or IP address", spec, f.Host)
	}
	if f.ListenPort < reversePrivilegedPortLimit {
		return f, fmt.Errorf("%q: port %d is below %d, which an unprivileged sshd cannot bind on the compute node; use listen-port:host:port with a higher listen port", spec, f.ListenPort, reversePrivilegedPortLimit)
	}
	return f, nil
}

func parseReversePort(s string) (int32, error) {
	port, err := strconv.ParseInt(s, 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %q is not in 1..65535", s)
	}
	return int32(port), nil
}

// reverseBindAddress picks the loopback address a pod's forwards listen on inside
// the job. Two pods of the same user on one compute node must not share it, so it
// is derived from the pod UID rather than fixed; 127.0.0.0/24 is left alone.
func reverseBindAddress(podUID string) string {
	h := fnv.New32a()
	h.Write([]byte(podUID))
	s := h.Sum32()
	return fmt.Sprintf("127.%d.%d.%d", 1+(s>>16)%254, (s>>8)&0xff, 1+(s&0xff)%254)
}

// reverseHostAliases lists the names a forward's host resolves to inside the job:
// the host as given plus, for a Service FQDN, the shorter forms cluster DNS would
// also answer. The bare Service name only holds in the pod's own namespace.
func reverseHostAliases(host, podNamespace string) []string {
	names := []string{host}
	parts := strings.Split(host, ".")
	if len(parts) >= 4 && parts[2] == "svc" {
		name, ns := parts[0], parts[1]
		names = append(names, name+"."+ns+".svc", name+"."+ns)
		if ns == podNamespace {
			names = append(names, name)
		}
	}
	return names
}

// reverseRelay is the owner-only relay the job runs in front of the tunnel's unix
// sockets, byte for byte the virtual kubelet's.
//
//go:embed templates/reverse-relay.py
var reverseRelay string

// ReversePreExec is the shell the job runs before its workload, as the virtual
// kubelet's pre-exec. It starts the relay that holds the pod's loopback address
// for the whole job, maps the forwarded names to that address in the container's
// /etc/hosts, and waits until the shadow has bound every forward in the socket
// directory, which happens only once the shadow has learned which node the job
// landed on. A SOCKS proxy is waited for on 127.0.0.1 and must belong to the job's
// user. It assumes a bash batch script and a Singularity or Apptainer runtime.
//
// The hosts file stays in node-local temporary space and never goes into
// $INTERLINK_SOCKET_DIR: only the batch node runs the relay, so ranks on other
// nodes must not resolve the names to an address nobody holds there.
func ReversePreExec(rt *ReverseTunnel, podNamespace string) string {
	seen := map[string]bool{}
	var hosts []string
	for _, f := range rt.Forwards {
		for _, name := range reverseHostAliases(f.Host, podNamespace) {
			if !seen[name] {
				seen[name] = true
				hosts = append(hosts, name)
			}
		}
	}
	timeout := fmt.Sprintf("[ $(( $(date +%%s) - il_rev_t0 )) -ge %d ] && { echo 'interLink: the reverse tunnel did not come up within %ds' >&2; exit 1; }\n",
		reverseTunnelWaitSeconds, reverseTunnelWaitSeconds)

	var b strings.Builder
	b.WriteString("# interLink reverse tunnel: cluster endpoints reachable from this job\n")
	b.WriteString("il_rev_d=${INTERLINK_SOCKET_DIR:-}\n")
	b.WriteString("[ -n \"$il_rev_d\" ] || { echo 'interLink: the reverse tunnel needs a plugin with SocketDirRoot set' >&2; exit 1; }\n")
	b.WriteString("il_rev_t0=$(date +%s)\n")
	if len(rt.Forwards) > 0 {
		fmt.Fprintf(&b, "# Node-local on purpose: only the batch node has a relay behind %s.\n", rt.Bind)
		b.WriteString("il_rev_hosts=$(mktemp \"${TMPDIR:-/tmp}/interlink-hosts.XXXXXX\") || exit 1\n")
		fmt.Fprintf(&b, "{ cat /etc/hosts; echo '%s %s'; } > \"$il_rev_hosts\"\n", rt.Bind, strings.Join(hosts, " "))
		b.WriteString("export SINGULARITY_BIND=\"${SINGULARITY_BIND:+$SINGULARITY_BIND,}$il_rev_hosts:/etc/hosts\"\n")
		b.WriteString("export APPTAINER_BIND=\"${APPTAINER_BIND:+$APPTAINER_BIND,}$il_rev_hosts:/etc/hosts\"\n")
		b.WriteString("cat > \"$il_rev_d/rev-relay.py\" <<'EORELAY'\n")
		b.WriteString(reverseRelay)
		if !strings.HasSuffix(reverseRelay, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("EORELAY\n")
		// -I: a module loaded by the batch script must not reach the relay through
		// PYTHONPATH or PYTHONHOME.
		fmt.Fprintf(&b, "%s -I \"$il_rev_d/rev-relay.py\" %s", rt.PythonPath, rt.Bind)
		var ready []string
		for _, f := range rt.Forwards {
			sock := fmt.Sprintf("\"$il_rev_d/rev-%d.sock\"", f.ListenPort)
			fmt.Fprintf(&b, " %d=%s", f.ListenPort, sock)
			ready = append(ready, "[ -S "+sock+" ]")
		}
		b.WriteString(" &\n")
		b.WriteString("il_rev_relay=$!\n")
		fmt.Fprintf(&b, "until [ -e \"$il_rev_d/rev-relay.ready\" ] && %s; do\n", strings.Join(ready, " && "))
		b.WriteString("  kill -0 \"$il_rev_relay\" 2>/dev/null || { echo 'interLink: the reverse relay stopped, see above' >&2; exit 1; }\n")
		b.WriteString("  " + timeout)
		b.WriteString("  sleep 2\n")
		b.WriteString("done\n")
	}
	if rt.SocksPort != 0 {
		// Anyone on the node may bind 127.0.0.1 first: wait for the listener to be
		// the job user's own, and fail if it is somebody else's.
		port := fmt.Sprintf("%04X", rt.SocksPort)
		b.WriteString("il_rev_uid=$(id -u)\n")
		b.WriteString(`il_rev_owner() { awk -v p=":$1" -v u="$il_rev_uid" 'NR>1 && $4=="0A" && ($2=="0100007F"p || $2=="00000000"p) {print ($8==u ? "own" : "other"); exit}' /proc/net/tcp; }` + "\n")
		fmt.Fprintf(&b, "until [ \"$(il_rev_owner %s)\" = own ]; do\n", port)
		fmt.Fprintf(&b, "  [ \"$(il_rev_owner %s)\" = other ] && { echo 'interLink: another user listens on the SOCKS port' >&2; exit 1; }\n", port)
		b.WriteString("  " + timeout)
		b.WriteString("  sleep 2\n")
		b.WriteString("done\n")
	}
	b.WriteString("echo \"interLink: reverse tunnel ready after $(( $(date +%s) - il_rev_t0 ))s\"\n")
	return b.String()
}
