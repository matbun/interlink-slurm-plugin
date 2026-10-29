package slurm

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

// DispatchConfig is the agent side of interlink-dispatch.
type DispatchConfig struct {
	Enabled bool `yaml:"Enabled"`
	// Namespace is the only namespace the agent accepts workloads from. Empty:
	// the namespace of its ServiceAccount.
	Namespace string             `yaml:"Namespace"`
	Auth      DispatchAuthConfig `yaml:"Auth"`
	// StateDir holds one record per workload. Empty: <DataRootFolder>dispatch.
	StateDir string `yaml:"StateDir"`
}

// DispatchAuthConfig says who may call /submit and /jobs.
type DispatchAuthConfig struct {
	// Mode is "tokenreview" (the default: the bearer token is checked with a
	// TokenReview) or "network" (every caller is accepted; a NetworkPolicy
	// decides who reaches the agent).
	Mode string `yaml:"Mode"`
	// Audience the caller's token must be issued for. Empty: interlink-agent.
	Audience string `yaml:"Audience"`
	// AllowedServiceAccounts are the user names a TokenReview may return, e.g.
	// system:serviceaccount:<namespace>:<name>.
	AllowedServiceAccounts []string `yaml:"AllowedServiceAccounts"`
}

// SiteConfig is one HPC site: where the ssh shims connect and which Slurm
// accounts this agent may charge.
type SiteConfig struct {
	LoginHost       string   `yaml:"LoginHost"`
	User            string   `yaml:"User"`
	Port            int      `yaml:"Port"`
	KeyFile         string   `yaml:"KeyFile"`
	AllowedAccounts []string `yaml:"AllowedAccounts"`
}

// ShadowConfig is what the shadow manager renders a workload's shadow with.
type ShadowConfig struct {
	Enabled bool `yaml:"Enabled"`
	// AlwaysShadow gives a shadow, for its logs, to jobs that expose no port.
	// Unset: true.
	AlwaysShadow *bool  `yaml:"AlwaysShadow"`
	Image        string `yaml:"Image"`
	// AgentImage runs the job-logs container. Empty: env AGENT_IMAGE.
	AgentImage string `yaml:"AgentImage"`
	// AgentURL is where job-logs reaches the agent.
	AgentURL string `yaml:"AgentURL"`
	// LoginHost, User and Port default to the site's.
	LoginHost           string `yaml:"LoginHost"`
	User                string `yaml:"User"`
	Port                int    `yaml:"Port"`
	KeySecret           string `yaml:"KeySecret"`
	KeySecretKey        string `yaml:"KeySecretKey"`
	KnownHostsConfigMap string `yaml:"KnownHostsConfigMap"`
	NodeHostKeys        string `yaml:"NodeHostKeys"`
	ForwardTarget       string `yaml:"ForwardTarget"`
	ForwardMode         string `yaml:"ForwardMode"`
	// ExecConnectCommand relays a connection in ForwardMode exec. Empty: nc.
	ExecConnectCommand string `yaml:"ExecConnectCommand"`
	NodeWaitTimeout    string `yaml:"NodeWaitTimeout"`
	// Reverse lets jobs open connections to listed cluster endpoints through the
	// shadow, as the virtual kubelet's Network.SSH.Reverse, plus AllowedTargets.
	Reverse shadow.ReverseConfig `yaml:"Reverse"`
}

// Dispatch.Auth.Mode values.
const (
	DispatchAuthTokenReview = "tokenreview"
	DispatchAuthNetwork     = "network"
)

const (
	defaultDispatchAudience = "interlink-agent"
	defaultSSHPort          = 22
)

// serviceAccountNamespaceFile is where a pod reads its own namespace.
var serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// AlwaysShadowEnabled reports Shadow.AlwaysShadow, true when unset.
func (s ShadowConfig) AlwaysShadowEnabled() bool {
	return s.AlwaysShadow == nil || *s.AlwaysShadow
}

// DispatchSite returns the agent's only site.
func (c SlurmConfig) DispatchSite() (string, SiteConfig) {
	for name, site := range c.Sites {
		return name, site
	}
	return "", SiteConfig{}
}

// validateDispatchConfig fills the defaults of the Dispatch, Sites and Shadow
// blocks and refuses a configuration the agent cannot run with. It does nothing
// when Dispatch is off.
func validateDispatchConfig(c *SlurmConfig) error {
	if !c.Dispatch.Enabled {
		return nil
	}
	if len(c.Sites) != 1 {
		return fmt.Errorf("Dispatch.Enabled needs exactly one entry in Sites, found %d: run one agent per site", len(c.Sites))
	}
	for name, site := range c.Sites {
		if site.LoginHost == "" || site.User == "" {
			return fmt.Errorf("Sites.%s: LoginHost and User are required", name)
		}
		if site.Port == 0 {
			site.Port = defaultSSHPort
		}
		if site.Port < 1 || site.Port > 65535 {
			return fmt.Errorf("Sites.%s: Port %d is not a TCP port", name, site.Port)
		}
		if len(site.AllowedAccounts) == 0 {
			return fmt.Errorf("Sites.%s: AllowedAccounts is empty, /submit would refuse every workload", name)
		}
		c.Sites[name] = site
	}

	auth := &c.Dispatch.Auth
	if auth.Mode == "" {
		auth.Mode = DispatchAuthTokenReview
	}
	switch auth.Mode {
	case DispatchAuthTokenReview:
		if len(auth.AllowedServiceAccounts) == 0 {
			return fmt.Errorf("Dispatch.Auth.Mode %s needs AllowedServiceAccounts", auth.Mode)
		}
	case DispatchAuthNetwork:
	default:
		return fmt.Errorf("Dispatch.Auth.Mode %q: want %q or %q", auth.Mode, DispatchAuthTokenReview, DispatchAuthNetwork)
	}
	if auth.Audience == "" {
		auth.Audience = defaultDispatchAudience
	}

	if c.Dispatch.Namespace == "" {
		raw, err := os.ReadFile(serviceAccountNamespaceFile)
		if err != nil {
			return fmt.Errorf("Dispatch.Namespace is empty and the ServiceAccount namespace cannot be read: %w", err)
		}
		c.Dispatch.Namespace = strings.TrimSpace(string(raw))
	}
	if c.Dispatch.StateDir == "" {
		c.Dispatch.StateDir = c.DataRootFolder + "dispatch"
	}

	// Workloads with more than one member go through the gang path.
	c.GangSchedulingEnabled = true
	if c.GangTimeout == "" {
		c.GangTimeout = "10m"
	}

	if c.Shadow.Enabled {
		cfg := c.shadowConfig()
		if err := shadow.NormalizeConfig(&cfg); err != nil {
			return err
		}
	}
	if c.Shadow.Reverse.Enabled {
		switch {
		case !c.Shadow.Enabled:
			return fmt.Errorf("Shadow.Reverse.Enabled needs Shadow.Enabled: the shadow carries the tunnel")
		case c.SocketDirRoot == "":
			return fmt.Errorf("Shadow.Reverse.Enabled needs SocketDirRoot: the tunnel ends in the job's socket directory")
		}
	}

	siteName, site := c.DispatchSite()
	return exportSiteEnv(siteName, site)
}

// exportSiteEnv sets the environment the sbatch, squeue and scancel shims read
// their target from.
func exportSiteEnv(name string, site SiteConfig) error {
	for k, v := range map[string]string{
		"REMOTE_USER":  site.User,
		"REMOTE_LOGIN": site.LoginHost,
		"REMOTE_PORT":  strconv.Itoa(site.Port),
		"SSH_KEY":      site.KeyFile,
	} {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("site %s: export %s: %w", name, k, err)
		}
	}
	return nil
}
