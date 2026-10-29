package slurm

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

// ExecutorConfig is the executor side of kueue-offload.
type ExecutorConfig struct {
	Enabled bool `yaml:"Enabled"`
	// Namespace is the only namespace the executor accepts workloads from. Empty:
	// the namespace of its ServiceAccount.
	Namespace string             `yaml:"Namespace"`
	Auth      ExecutorAuthConfig `yaml:"Auth"`
	// StateDir holds one record per workload. Empty: <DataRootFolder>executor.
	StateDir string `yaml:"StateDir"`
	// KubernetesAPI is where a job that carries a ServiceAccount token reaches the API
	// server, through its reverse tunnel: the address the kubelet would have put in
	// KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT. Empty Host: not set.
	KubernetesAPI ExecutorKubernetesAPI `yaml:"KubernetesAPI"`
	// AutomountServiceAccountToken puts a token of the template's ServiceAccount in the
	// service account directory the executor adds to templates that do not opt out (see
	// withServiceAccountDir). False: the directory holds the namespace and the
	// cluster's CA only, so no cluster credential reaches the site unless the template
	// asks for one with its own projected volume.
	AutomountServiceAccountToken bool `yaml:"AutomountServiceAccountToken"`
}

// ExecutorKubernetesAPI is the API server's address as a job sees it.
type ExecutorKubernetesAPI struct {
	Host string `yaml:"Host"`
	Port string `yaml:"Port"`
}

// ExecutorAuthConfig says who may call /submit and /jobs.
type ExecutorAuthConfig struct {
	// Mode is "tokenreview" (the default: the bearer token is checked with a
	// TokenReview) or "network" (every caller is accepted; a NetworkPolicy
	// decides who reaches the executor).
	Mode string `yaml:"Mode"`
	// Audience the caller's token must be issued for. Empty: kueue-offload-executor.
	Audience string `yaml:"Audience"`
	// AllowedServiceAccounts are the user names a TokenReview may return, e.g.
	// system:serviceaccount:<namespace>:<name>.
	AllowedServiceAccounts []string `yaml:"AllowedServiceAccounts"`
}

// SiteConfig is one HPC site: where the ssh shims connect and which Slurm
// accounts this executor may charge.
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
	// ExecutorImage runs the job-logs container. Empty: env EXECUTOR_IMAGE.
	ExecutorImage string `yaml:"ExecutorImage"`
	// ExecutorURL is where job-logs reaches the executor.
	ExecutorURL string `yaml:"ExecutorURL"`
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

// StagingConfig copies the paths of a workload's interlink.eu/stage-in annotation
// to the site before its job is submitted, and those of interlink.eu/stage-out
// back once it ended, each with a batch Job in the executor's namespace that runs
// Command. The job finds the staged inputs at their own paths only through binds
// of the site's stage directory in SingularityDefaultOptions: that is deployment
// configuration, the executor does not add them.
//
// Command's contract is its environment: STAGE_DIRECTION (in or out),
// STAGE_PATHS (':'-separated), REMOTE (<site user>@<TransferHost>), STAGE_ROOT,
// STAGE_NAMESPACE, SSH_KEY (the mounted key), and for a path on a Kerberos volume
// KEYTAB, PRINCIPAL and KRB5CCNAME.
type StagingConfig struct {
	Enabled bool   `yaml:"Enabled"`
	Image   string `yaml:"Image"`
	// Command runs the copy. Empty: /usr/local/bin/stage-rsync.
	Command      string `yaml:"Command"`
	TransferHost string `yaml:"TransferHost"`
	// StageRoot is the site's stage directory; Command keeps each namespace's
	// paths under it.
	StageRoot string `yaml:"StageRoot"`
	// KeySecret, KeySecretKey and KnownHostsConfigMap default to the Shadow's.
	KeySecret           string `yaml:"KeySecret"`
	KeySecretKey        string `yaml:"KeySecretKey"`
	KnownHostsConfigMap string `yaml:"KnownHostsConfigMap"`
	// Volumes are the claims the staged paths live on: a path must be under
	// one of their mount paths.
	Volumes []StagingVolume `yaml:"Volumes"`
	// Kerberos is used for the paths on a volume marked Kerberos.
	Kerberos *StagingKerberos `yaml:"Kerberos"`
}

// StagingVolume is a PersistentVolumeClaim of the namespace, mounted in the
// stage Job at MountPath.
type StagingVolume struct {
	Name      string `yaml:"Name"`
	ClaimName string `yaml:"ClaimName"`
	MountPath string `yaml:"MountPath"`
	// Kerberos: reading or writing the volume needs a ticket.
	Kerberos bool `yaml:"Kerberos"`
}

// StagingKerberos is the keytab the stage Job gets its ticket with.
type StagingKerberos struct {
	KeytabSecret string `yaml:"KeytabSecret"`
	KeytabKey    string `yaml:"KeytabKey"`
	Principal    string `yaml:"Principal"`
	// Krb5ConfigMap holds krb5.conf. Empty: the image's.
	Krb5ConfigMap string `yaml:"Krb5ConfigMap"`
}

const defaultStageCommand = "/usr/local/bin/stage-rsync"

// Executor.Auth.Mode values.
const (
	ExecutorAuthTokenReview = "tokenreview"
	ExecutorAuthNetwork     = "network"
)

const (
	defaultExecutorAudience = "kueue-offload-executor"
	defaultSSHPort          = 22
)

// serviceAccountNamespaceFile is where a pod reads its own namespace.
var serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// AlwaysShadowEnabled reports Shadow.AlwaysShadow, true when unset.
func (s ShadowConfig) AlwaysShadowEnabled() bool {
	return s.AlwaysShadow == nil || *s.AlwaysShadow
}

// ExecutorSite returns the executor's only site.
func (c SlurmConfig) ExecutorSite() (string, SiteConfig) {
	for name, site := range c.Sites {
		return name, site
	}
	return "", SiteConfig{}
}

// validateExecutorConfig fills the defaults of the Executor, Sites and Shadow
// blocks and refuses a configuration the executor cannot run with. It does nothing
// when Executor is off.
func validateExecutorConfig(c *SlurmConfig) error {
	if !c.Executor.Enabled {
		return nil
	}
	if len(c.Sites) != 1 {
		return fmt.Errorf("Executor.Enabled needs exactly one entry in Sites, found %d: run one executor per site", len(c.Sites))
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

	auth := &c.Executor.Auth
	if auth.Mode == "" {
		auth.Mode = ExecutorAuthTokenReview
	}
	switch auth.Mode {
	case ExecutorAuthTokenReview:
		if len(auth.AllowedServiceAccounts) == 0 {
			return fmt.Errorf("Executor.Auth.Mode %s needs AllowedServiceAccounts", auth.Mode)
		}
	case ExecutorAuthNetwork:
	default:
		return fmt.Errorf("Executor.Auth.Mode %q: want %q or %q", auth.Mode, ExecutorAuthTokenReview, ExecutorAuthNetwork)
	}
	if auth.Audience == "" {
		auth.Audience = defaultExecutorAudience
	}

	if c.Executor.Namespace == "" {
		raw, err := os.ReadFile(serviceAccountNamespaceFile)
		if err != nil {
			return fmt.Errorf("Executor.Namespace is empty and the ServiceAccount namespace cannot be read: %w", err)
		}
		c.Executor.Namespace = strings.TrimSpace(string(raw))
	}
	if c.Executor.StateDir == "" {
		c.Executor.StateDir = c.DataRootFolder + "executor"
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

	if err := validateStagingConfig(c); err != nil {
		return err
	}

	siteName, site := c.ExecutorSite()
	return exportSiteEnv(siteName, site)
}

// validateStagingConfig fills the Staging block's defaults and checks it.
func validateStagingConfig(c *SlurmConfig) error {
	st := &c.Staging
	if !st.Enabled {
		return nil
	}
	if st.Command == "" {
		st.Command = defaultStageCommand
	}
	if st.KeySecret == "" {
		st.KeySecret = c.Shadow.KeySecret
	}
	if st.KeySecretKey == "" {
		st.KeySecretKey = c.Shadow.KeySecretKey
	}
	if st.KeySecretKey == "" {
		st.KeySecretKey = shadow.DefaultKeySecretKey
	}
	if st.KnownHostsConfigMap == "" {
		st.KnownHostsConfigMap = c.Shadow.KnownHostsConfigMap
	}
	switch {
	case st.Image == "":
		return fmt.Errorf("Staging.Image is required")
	case st.TransferHost == "":
		return fmt.Errorf("Staging.TransferHost is required")
	case st.StageRoot == "":
		return fmt.Errorf("Staging.StageRoot is required")
	case st.KeySecret == "":
		return fmt.Errorf("Staging.KeySecret is required (or Shadow.KeySecret)")
	case len(st.Volumes) == 0:
		return fmt.Errorf("Staging.Volumes is empty: every staged path would be refused")
	}
	names := map[string]bool{}
	for _, v := range st.Volumes {
		switch {
		case v.Name == "" || v.ClaimName == "":
			return fmt.Errorf("Staging.Volumes: Name and ClaimName are required")
		case names[v.Name]:
			return fmt.Errorf("Staging.Volumes: %s appears twice", v.Name)
		case !path.IsAbs(v.MountPath) || path.Clean(v.MountPath) != v.MountPath || v.MountPath == "/":
			return fmt.Errorf("Staging.Volumes.%s: MountPath %q is not a clean absolute path below /", v.Name, v.MountPath)
		case v.Kerberos && st.Kerberos == nil:
			return fmt.Errorf("Staging.Volumes.%s needs Kerberos, but Staging.Kerberos is not set", v.Name)
		}
		names[v.Name] = true
	}
	if k := st.Kerberos; k != nil && (k.KeytabSecret == "" || k.KeytabKey == "" || k.Principal == "") {
		return fmt.Errorf("Staging.Kerberos: KeytabSecret, KeytabKey and Principal are required")
	}
	return nil
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
