package slurm

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
)

// FlavorConfig holds the configuration for a specific flavor
type FlavorConfig struct {
	Name          string   `yaml:"Name"`
	Description   string   `yaml:"Description"`
	CPUDefault    int64    `yaml:"CPUDefault"`
	MemoryDefault string   `yaml:"MemoryDefault"` // e.g., "16G", "32000M", "1024"
	UID           *int64   `yaml:"UID"`           // Optional User ID for this flavor
	SlurmFlags    []string `yaml:"SlurmFlags"`
}

// Validate checks if the FlavorConfig is valid
func (f *FlavorConfig) Validate() error {
	if f.Name == "" {
		return fmt.Errorf("flavor Name cannot be empty")
	}

	if f.CPUDefault < 0 {
		return fmt.Errorf("flavor '%s': CPUDefault cannot be negative (got %d)", f.Name, f.CPUDefault)
	}

	if f.MemoryDefault != "" {
		// Try to parse the memory string to ensure it's valid
		if _, err := parseMemoryString(f.MemoryDefault); err != nil {
			return fmt.Errorf("flavor '%s': invalid MemoryDefault format '%s': %w", f.Name, f.MemoryDefault, err)
		}
	}

	// Validate SLURM flags format (basic check)
	for i, flag := range f.SlurmFlags {
		flag = strings.TrimSpace(flag)
		if flag == "" {
			return fmt.Errorf("flavor '%s': SLURM flag at index %d is empty", f.Name, i)
		}
		// Check if flag starts with -- or -
		if !strings.HasPrefix(flag, "--") && !strings.HasPrefix(flag, "-") {
			return fmt.Errorf("flavor '%s': SLURM flag '%s' should start with '--' or '-'", f.Name, flag)
		}
	}

	// Validate UID if set
	if f.UID != nil && *f.UID < 0 {
		return fmt.Errorf("flavor '%s': UID cannot be negative (got %d)", f.Name, *f.UID)
	}

	return nil
}

// InterLinkConfig holds the whole configuration
type SlurmConfig struct {
	VKConfigPath              string   `yaml:"VKConfigPath"`
	Sbatchpath                string   `yaml:"SbatchPath"`
	Scancelpath               string   `yaml:"ScancelPath"`
	Squeuepath                string   `yaml:"SqueuePath"`
	Sinfopath                 string   `yaml:"SinfoPath"`
	Sidecarport               string   `yaml:"SidecarPort"`
	Socket                    string   `yaml:"Socket"`
	ExportPodData             bool     `yaml:"ExportPodData"`
	Commandprefix             string   `yaml:"CommandPrefix"`
	ImagePrefix               string   `yaml:"ImagePrefix"`
	DataRootFolder            string   `yaml:"DataRootFolder"`
	Namespace                 string   `yaml:"Namespace"`
	Tsocks                    bool     `yaml:"Tsocks"`
	Tsockspath                string   `yaml:"TsocksPath"`
	Tsockslogin               string   `yaml:"TsocksLoginNode"`
	BashPath                  string   `yaml:"BashPath"`
	VerboseLogging            bool     `yaml:"VerboseLogging"`
	ErrorsOnlyLogging         bool     `yaml:"ErrorsOnlyLogging"`
	SingularityDefaultOptions []string `yaml:"SingularityDefaultOptions"`
	SingularityPrefix         string   `yaml:"SingularityPrefix"`
	SingularityPath           string   `yaml:"SingularityPath"`
	EnableProbes              bool     `yaml:"EnableProbes"`
	set                       bool
	EnrootDefaultOptions      []string                `yaml:"EnrootDefaultOptions" default:"[\"--rw\"]"`
	EnrootPrefix              string                  `yaml:"EnrootPrefix"`
	EnrootPath                string                  `yaml:"EnrootPath"`
	ContainerRuntime          string                  `yaml:"ContainerRuntime" default:"singularity"` // "singularity" or "enroot"
	Flavors                   map[string]FlavorConfig `yaml:"Flavors"`
	DefaultFlavor             string                  `yaml:"DefaultFlavor"`
	DefaultUID                *int64                  `yaml:"DefaultUID"` // Optional default User ID for all jobs (RFC: https://github.com/interlink-hq/interlink-slurm-plugin/discussions/58)

	// Gang scheduling. When GangSchedulingEnabled is false (the
	// default) the plugin behaves exactly as before: 1 Pod = 1 sbatch. When
	// enabled, pods carrying the `interlink.eu/gang-name` annotation are
	// BUFFERED in the plugin and submitted together as a single co-scheduled
	// `sbatch --nodes=N` job. Pods WITHOUT that annotation always take the
	// unchanged single-pod path, regardless of this flag.
	GangSchedulingEnabled bool   `yaml:"GangSchedulingEnabled"` // default false
	// GangTimeout is how long an incomplete gang is buffered before being
	// abandoned (its buffered members' dirs removed and the entry dropped).
	// Parsed as a Go duration string (e.g. "10m", "30s"). Empty => 10m.
	GangTimeout string `yaml:"GangTimeout"`
}

type CreateStruct struct {
	PodUID string `json:"PodUID"`
	PodJID string `json:"PodJID"`
}

// Gang-scheduling annotation keys. The plugin reads ONLY these
// interlink.eu/gang-* keys; it never inspects ray.io/* or JobSet labels.
const (
	// GangNameAnnotation is the co-allocation key. All pods sharing the same
	// value are buffered and submitted as ONE `sbatch --nodes=N` job. Its
	// presence (with GangSchedulingEnabled) is what triggers the gang path.
	GangNameAnnotation = "interlink.eu/gang-name"
	// GangSizeAnnotation is the integer N: both the buffering quorum and the
	// `#SBATCH --nodes=N`. Required on every gang member.
	GangSizeAnnotation = "interlink.eu/gang-size"
	// GangRoleAnnotation is "head" or "worker" (default "worker"). role=head is
	// forced to rank 0.
	GangRoleAnnotation = "interlink.eu/gang-role"
	// GangRankAnnotation is an optional explicit rank in 0..N-1. When absent,
	// ranks are assigned by arrival order (with head pinned to rank 0).
	GangRankAnnotation = "interlink.eu/gang-rank"

	// GangRoleHead / GangRoleWorker are the two recognised role values.
	GangRoleHead   = "head"
	GangRoleWorker = "worker"
)

// BufferedMember holds everything produceGangSLURMScript needs to render one
// gang member's own rank line (its already-rendered singularity/enroot runtime
// command, its per-pod files dir for isolated logs, its role/rank, and the pod
// itself for command/args). One BufferedMember == one rank on one node.
type BufferedMember struct {
	PodUID          string
	Namespace       string
	FilesPath       string // per-pod dir (DataRootFolder + ns + "-" + uid): job.sh, JobID.jid, logs
	Role            string // GangRoleHead or GangRoleWorker
	Rank            int    // 0..N-1; head is always 0
	Pod             v1.Pod
	RuntimeCommands []ContainerCommand // this member's own rendered container commands
	ResourceLimits  ResourceLimits
	IsDefaultCPU    bool
	IsDefaultRam    bool
	Flavor          *FlavorResolution
}

// GangEntry is the buffer for a single gang (keyed by gang-name in GangTable).
// It accumulates members until len(Members) == Size, then submits ONE sbatch.
type GangEntry struct {
	Name      string
	Size      int
	Members   map[string]*BufferedMember // keyed by PodUID
	JID       string                     // the shared SLURM job ID once submitted
	Submitted bool
	CreatedAt time.Time
}

type ProbeType string

const (
	ProbeTypeHTTP ProbeType = "http"
	ProbeTypeExec ProbeType = "exec"
)

type ProbeCommand struct {
	Type                ProbeType
	HTTPGetAction       *HTTPGetAction
	ExecAction          *ExecAction
	InitialDelaySeconds int32
	PeriodSeconds       int32
	TimeoutSeconds      int32
	SuccessThreshold    int32
	FailureThreshold    int32
}

type HTTPGetAction struct {
	Path   string
	Port   int32
	Host   string
	Scheme string
}

type ExecAction struct {
	Command []string
}

// LifecycleHookType indicates whether a lifecycle hook is an exec or httpGet hook.
type LifecycleHookType string

const (
	LifecycleHookTypeExec    LifecycleHookType = "exec"
	LifecycleHookTypeHTTPGet LifecycleHookType = "httpGet"
)

// LifecycleHTTPGetSpec holds the parameters for an httpGet-type lifecycle hook.
type LifecycleHTTPGetSpec struct {
	Scheme string
	Host   string
	Port   int32
	Path   string
}

// LifecycleHookSpec describes a container lifecycle hook (postStart or preStop)
// in a runtime-agnostic form.
type LifecycleHookSpec struct {
	Type        LifecycleHookType
	ExecCommand []string           // populated when Type == LifecycleHookTypeExec
	HTTPGet     *LifecycleHTTPGetSpec // populated when Type == LifecycleHookTypeHTTPGet
}

type ContainerCommand struct {
	containerName    string
	isInitContainer  bool
	runtimeCommand   []string
	containerCommand []string
	containerArgs    []string
	containerImage   string
	readinessProbes  []ProbeCommand
	livenessProbes   []ProbeCommand
	startupProbes    []ProbeCommand
	preStopHook      *LifecycleHookSpec // optional preStop lifecycle hook
	postStartHook    *LifecycleHookSpec // optional postStart lifecycle hook
}
