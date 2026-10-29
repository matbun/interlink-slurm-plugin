package shadow

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
)

// The virtual kubelet's SSH shadow template, byte for byte.
//
//go:embed templates/shadow-ssh-template.yaml
var sshTemplate string

var parsedSSHTemplate = template.Must(template.New("shadow").Parse(sshTemplate))

// sshTunnel carries the fields of the virtual kubelet's Network.SSH block the
// template reads. The executor supports public key authentication only.
type sshTunnel struct {
	LoginHost           string
	Port                int
	User                string
	Image               string
	Auth                string
	ForwardTarget       string
	ForwardMode         string
	ExecConnectCommand  string
	NodeHostKeys        string
	KeySecret           string
	KeySecretKey        string
	KeytabSecret        string
	KeytabSecretKey     string
	Principal           string
	Krb5ConfigMap       string
	KnownHostsConfigMap string
	NodeWaitTimeout     string
	ExtraOptions        []string
}

// templateData is the part of the virtual kubelet's ShadowTemplateData the SSH
// template reads.
type templateData struct {
	Name               string
	Namespace          string
	ExposedPorts       []portMapping
	ReadinessPort      int32
	NodeConfigMap      string
	NodeNameKey        string
	SocketDirKey       string
	NeedsSocketDir     bool
	SSH                sshTunnel
	SSHNodeWaitSeconds int
	Reverse            *ReverseTunnel
}

const (
	jobLogsContainer = "job-logs"
	jobLogsCommand   = "/sidecar/slurm-sidecar"
)

func (cfg Config) sshTunnel() sshTunnel {
	return sshTunnel{
		LoginHost:           cfg.LoginHost,
		Port:                cfg.Port,
		User:                cfg.User,
		Image:               cfg.Image,
		Auth:                "publickey",
		ForwardTarget:       cfg.ForwardTarget,
		ForwardMode:         cfg.ForwardMode,
		ExecConnectCommand:  cfg.ExecConnectCommand,
		NodeHostKeys:        cfg.NodeHostKeys,
		KeySecret:           cfg.KeySecret,
		KeySecretKey:        cfg.KeySecretKey,
		KnownHostsConfigMap: cfg.KnownHostsConfigMap,
		NodeWaitTimeout:     cfg.NodeWaitTimeout,
	}
}

// executeTemplate renders the SSH template and decodes its documents.
func executeTemplate(data templateData) (*appsv1.Deployment, *corev1.Service, error) {
	var buf bytes.Buffer
	if err := parsedSSHTemplate.Execute(&buf, data); err != nil {
		return nil, nil, fmt.Errorf("failed to execute the shadow template: %w", err)
	}
	decoder := serializer.NewCodecFactory(scheme.Scheme).UniversalDeserializer()
	var deployment *appsv1.Deployment
	var service *corev1.Service
	for _, doc := range strings.Split(buf.String(), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		obj, _, err := decoder.Decode([]byte(doc), nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode the rendered shadow: %w", err)
		}
		switch o := obj.(type) {
		case *appsv1.Deployment:
			deployment = o
		case *corev1.Service:
			service = o
		default:
			return nil, nil, fmt.Errorf("unexpected %T in the rendered shadow", obj)
		}
	}
	if deployment == nil {
		return nil, nil, fmt.Errorf("the rendered shadow has no Deployment")
	}
	return deployment, service, nil
}

// render builds the shadow's Deployment and, when it forwards ports, its Service.
// A logs-only shadow forwards nothing: no ssh-forward, no ssh-reverse, no init
// containers, no Service, only job-logs.
func render(cfg Config, spec Spec, name string, ports []portMapping, logsOnly bool) (*appsv1.Deployment, *corev1.Service, error) {
	data := templateData{
		Name:               name,
		Namespace:          spec.Namespace,
		NodeConfigMap:      nodeConfigMapName(name),
		NodeNameKey:        nodeNameKey,
		SocketDirKey:       socketDirKey,
		SSH:                cfg.sshTunnel(),
		SSHNodeWaitSeconds: cfg.nodeWaitSeconds(),
	}
	if !logsOnly {
		data.ExposedPorts = ports
		data.ReadinessPort = firstTCPPort(ports)
		data.NeedsSocketDir = hasSocketPorts(ports)
		if spec.Reverse != nil {
			data.Reverse = spec.Reverse
			data.NeedsSocketDir = true
		}
	}
	deployment, service, err := executeTemplate(data)
	if err != nil {
		return nil, nil, err
	}
	if logsOnly {
		service = nil
	}

	labels := ownerLabels(spec.WorkloadUID)
	setLabels(&deployment.ObjectMeta, labels)
	setLabels(&deployment.Spec.Template.ObjectMeta, labels)
	if service != nil {
		setLabels(&service.ObjectMeta, labels)
	}

	pod := &deployment.Spec.Template.Spec
	if logsOnly {
		pod.InitContainers = nil
	}
	pod.Containers = append(pod.Containers, jobLogs(cfg, spec.WorkloadUID))
	pod.Volumes = mountedVolumes(pod)
	return deployment, service, nil
}

// jobLogs is the container that streams the job's output from the executor.
func jobLogs(cfg Config, workloadUID string) corev1.Container {
	noEscalation := false
	return corev1.Container{
		Name:    jobLogsContainer,
		Image:   cfg.ExecutorImage,
		Command: []string{jobLogsCommand, "logs", "--executor", cfg.ExecutorURL, "--workload", workloadUID, "--follow"},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &noEscalation,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
}

// mountedVolumes drops the volumes no container mounts, so a logs-only shadow does
// not wait for the tunnel key or the known hosts it never reads.
func mountedVolumes(pod *corev1.PodSpec) []corev1.Volume {
	mounted := map[string]bool{}
	for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		for _, m := range c.VolumeMounts {
			mounted[m.Name] = true
		}
	}
	var volumes []corev1.Volume
	for _, v := range pod.Volumes {
		if mounted[v.Name] {
			volumes = append(volumes, v)
		}
	}
	return volumes
}
