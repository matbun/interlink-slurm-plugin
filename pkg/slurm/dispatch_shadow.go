package slurm

import (
	"context"

	"k8s.io/client-go/kubernetes"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

// ShadowSpec is what the shadow of a workload is rendered from.
type ShadowSpec = shadow.Spec

// ShadowRef names the objects of a shadow.
type ShadowRef = shadow.Ref

// ShadowManager creates and removes the shadow of a workload: the pod that
// carries its tunnel and streams its logs.
type ShadowManager interface {
	// Ensure creates or updates the shadow for spec. Idempotent; called on every
	// poll once the job runs.
	Ensure(ctx context.Context, spec ShadowSpec) (ShadowRef, error)
	// Delete removes every object labelled for this workload. Idempotent.
	Delete(ctx context.Context, workloadUID string) error
}

var _ ShadowManager = shadow.Manager(nil)

// noopShadowManager is the manager when shadows are off.
type noopShadowManager struct{}

func (noopShadowManager) Ensure(context.Context, ShadowSpec) (ShadowRef, error) {
	return ShadowRef{}, nil
}

func (noopShadowManager) Delete(context.Context, string) error { return nil }

// shadowConfig is the Shadow block as the shadow package reads it, with the
// site's LoginHost, User and Port where the block leaves them unset.
func (c SlurmConfig) shadowConfig() shadow.Config {
	_, site := c.DispatchSite()
	s := c.Shadow
	cfg := shadow.Config{
		Image:               s.Image,
		AgentImage:          s.AgentImage,
		AgentURL:            s.AgentURL,
		LoginHost:           s.LoginHost,
		User:                s.User,
		Port:                s.Port,
		KeySecret:           s.KeySecret,
		KeySecretKey:        s.KeySecretKey,
		KnownHostsConfigMap: s.KnownHostsConfigMap,
		NodeHostKeys:        s.NodeHostKeys,
		ForwardTarget:       s.ForwardTarget,
		ForwardMode:         s.ForwardMode,
		ExecConnectCommand:  s.ExecConnectCommand,
		NodeWaitTimeout:     s.NodeWaitTimeout,
		AlwaysShadow:        s.AlwaysShadow,
		Reverse:             s.Reverse,
	}
	if cfg.LoginHost == "" {
		cfg.LoginHost = site.LoginHost
	}
	if cfg.User == "" {
		cfg.User = site.User
	}
	if cfg.Port == 0 {
		cfg.Port = site.Port
	}
	return cfg
}

// NewShadowManager returns the manager of the shadows in the agent's namespace,
// or the reason the Shadow block cannot render one.
func NewShadowManager(kube kubernetes.Interface, c SlurmConfig) (ShadowManager, error) {
	cfg := c.shadowConfig()
	if err := shadow.NormalizeConfig(&cfg); err != nil {
		return nil, err
	}
	return shadow.NewManager(kube, c.Dispatch.Namespace, cfg), nil
}
