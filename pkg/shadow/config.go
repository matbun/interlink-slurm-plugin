package shadow

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// Forward modes, as the virtual kubelet's Network.SSH.ForwardMode.
const (
	// ForwardModePortForward asks sshd to forward: `ssh -L` on the login node, or
	// `-W` on the login node and `-L` on the compute node. This is the default.
	ForwardModePortForward = "portforward"
	// ForwardModeExec pipes each connection through a session command instead, for
	// sites that do not grant TCP forwarding.
	ForwardModeExec = "exec"
)

// Forward targets, as the virtual kubelet's Network.SSH.ForwardTarget.
const (
	// ForwardTargetLogin ends the session on the login node, which connects to the
	// compute node in clear TCP. This is the default.
	ForwardTargetLogin = "login"
	// ForwardTargetNode ends the session on the compute node's own sshd and opens
	// every forward from inside the node, to 127.0.0.1 or to a unix socket.
	ForwardTargetNode = "node"
)

// Compute node host key checks, as the virtual kubelet's Network.SSH.NodeHostKeys.
const (
	NodeHostKeysVerify    = "verify"
	NodeHostKeysAcceptNew = "accept-new"
)

const (
	// DefaultImage is the image of the shadow's ssh containers.
	DefaultImage = "ghcr.io/interlink-hq/interlink/ssh-tunnel:0.6.3-pre2"
	// DefaultPort is the login node's SSH port.
	DefaultPort = 22
	// DefaultKeySecretKey is the key of a kubernetes.io/ssh-auth Secret.
	DefaultKeySecretKey = "ssh-privatekey"
	// DefaultNodeWaitTimeout bounds the wait of the shadow for a compute node.
	DefaultNodeWaitTimeout = "2h"
	// DefaultExecConnectCommand relays a connection on stdin/stdout in exec mode.
	DefaultExecConnectCommand = "nc"
	// AgentImageEnv names the environment variable AgentImage falls back to.
	AgentImageEnv = "AGENT_IMAGE"
)

// Config is the agent's Shadow block. LoginHost and User have no default here: the
// agent fills them from its site before NewManager when they are empty.
type Config struct {
	// Image runs the shadow's ssh containers (default DefaultImage).
	Image string `yaml:"Image"`
	// AgentImage runs the job-logs container: the agent's own image. Empty: the
	// AGENT_IMAGE environment variable.
	AgentImage string `yaml:"AgentImage"`
	// AgentURL is where job-logs reaches the agent, e.g. http://interlink-agent:8000.
	AgentURL  string `yaml:"AgentURL"`
	LoginHost string `yaml:"LoginHost"`
	User      string `yaml:"User"`
	Port      int    `yaml:"Port"`
	// KeySecret is the namespace's Secret holding the tunnel's private key, under
	// KeySecretKey (default "ssh-privatekey").
	KeySecret    string `yaml:"KeySecret"`
	KeySecretKey string `yaml:"KeySecretKey"`
	// KnownHostsConfigMap holds a known_hosts file pinning the login node and,
	// for NodeHostKeys "verify", compute node keys or an @cert-authority line.
	KnownHostsConfigMap string `yaml:"KnownHostsConfigMap"`
	// NodeHostKeys is "verify" (default) or "accept-new".
	NodeHostKeys string `yaml:"NodeHostKeys"`
	// ForwardTarget is "login" (default) or "node".
	ForwardTarget string `yaml:"ForwardTarget"`
	// ForwardMode is "portforward" (default) or "exec".
	ForwardMode string `yaml:"ForwardMode"`
	// ExecConnectCommand is the relay command of the exec mode, run on the login
	// node (default "nc").
	ExecConnectCommand string `yaml:"ExecConnectCommand"`
	// NodeWaitTimeout bounds how long the shadow waits for the compute node
	// (default "2h").
	NodeWaitTimeout string `yaml:"NodeWaitTimeout"`
	// AlwaysShadow gives a job without exposed ports a logs-only shadow. nil means true.
	AlwaysShadow *bool `yaml:"AlwaysShadow"`
	// Reverse lets jobs open connections back into the cluster, see the
	// interlink.eu/reverse-* annotations.
	Reverse ReverseConfig `yaml:"Reverse"`
}

// The template puts these values in shell commands unquoted, so they are held to
// plain characters rather than escaped.
var (
	userPattern      = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`)
	secretKeyPattern = regexp.MustCompile(`^[-._A-Za-z0-9]+$`)
)

// NormalizeConfig fills in the defaults and rejects values the shadow cannot use,
// so the agent can fail at startup rather than at the first job.
func NormalizeConfig(cfg *Config) error {
	if cfg.Image == "" {
		cfg.Image = DefaultImage
	}
	if cfg.AgentImage == "" {
		cfg.AgentImage = os.Getenv(AgentImageEnv)
	}
	if cfg.AgentImage == "" {
		return fmt.Errorf("Shadow.AgentImage is empty and %s is not set", AgentImageEnv)
	}
	u, err := url.Parse(cfg.AgentURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("Shadow.AgentURL %q is not an http(s) URL", cfg.AgentURL)
	}
	if !isValidComputeNodeName(cfg.LoginHost) {
		return fmt.Errorf("Shadow.LoginHost %q is not a hostname or IP address", cfg.LoginHost)
	}
	if !userPattern.MatchString(cfg.User) {
		return fmt.Errorf("Shadow.User %q is not a user name", cfg.User)
	}
	if cfg.Port == 0 {
		cfg.Port = DefaultPort
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("Shadow.Port %d is outside 1-65535", cfg.Port)
	}
	if strings.TrimSpace(cfg.KeySecret) == "" {
		return fmt.Errorf("Shadow.KeySecret is required")
	}
	if cfg.KeySecretKey == "" {
		cfg.KeySecretKey = DefaultKeySecretKey
	}
	if !secretKeyPattern.MatchString(cfg.KeySecretKey) {
		return fmt.Errorf("Shadow.KeySecretKey %q is not a Secret key", cfg.KeySecretKey)
	}

	cfg.NodeHostKeys = strings.ToLower(strings.TrimSpace(cfg.NodeHostKeys))
	switch cfg.NodeHostKeys {
	case "":
		cfg.NodeHostKeys = NodeHostKeysVerify
	case NodeHostKeysVerify, NodeHostKeysAcceptNew:
	default:
		return fmt.Errorf("unknown Shadow.NodeHostKeys %q: expected %q or %q", cfg.NodeHostKeys, NodeHostKeysVerify, NodeHostKeysAcceptNew)
	}

	cfg.ForwardTarget = strings.ToLower(strings.TrimSpace(cfg.ForwardTarget))
	switch cfg.ForwardTarget {
	case "":
		cfg.ForwardTarget = ForwardTargetLogin
	case ForwardTargetLogin, ForwardTargetNode:
	default:
		return fmt.Errorf("unknown Shadow.ForwardTarget %q: expected %q or %q", cfg.ForwardTarget, ForwardTargetLogin, ForwardTargetNode)
	}

	cfg.ForwardMode = strings.ToLower(strings.TrimSpace(cfg.ForwardMode))
	switch cfg.ForwardMode {
	case "":
		cfg.ForwardMode = ForwardModePortForward
	case ForwardModePortForward, ForwardModeExec:
	default:
		return fmt.Errorf("unknown Shadow.ForwardMode %q: expected %q or %q", cfg.ForwardMode, ForwardModePortForward, ForwardModeExec)
	}
	if err := NormalizeReverseConfig(&cfg.Reverse); err != nil {
		return err
	}
	// The srun carrier of the reverse tunnel runs through the same command.
	if cfg.ForwardMode == ForwardModeExec || cfg.Reverse.NodeExec == ReverseNodeExecSrun {
		if strings.TrimSpace(cfg.ExecConnectCommand) == "" {
			cfg.ExecConnectCommand = DefaultExecConnectCommand
		}
		// The template quotes it with single quotes.
		if strings.Contains(cfg.ExecConnectCommand, "'") {
			return fmt.Errorf("Shadow.ExecConnectCommand %q contains a single quote", cfg.ExecConnectCommand)
		}
	}

	if cfg.NodeWaitTimeout == "" {
		cfg.NodeWaitTimeout = DefaultNodeWaitTimeout
	}
	if d, err := time.ParseDuration(cfg.NodeWaitTimeout); err != nil || d <= 0 {
		return fmt.Errorf("invalid Shadow.NodeWaitTimeout %q: a positive duration is required", cfg.NodeWaitTimeout)
	}
	return nil
}

// alwaysShadow reports whether a job without exposed ports gets a shadow.
func (cfg Config) alwaysShadow() bool {
	return cfg.AlwaysShadow == nil || *cfg.AlwaysShadow
}

// nodeWaitSeconds is NodeWaitTimeout in whole seconds, for the template's shell loop.
func (cfg Config) nodeWaitSeconds() int {
	d, err := time.ParseDuration(cfg.NodeWaitTimeout)
	if err != nil {
		return 0
	}
	return int(d.Seconds())
}
