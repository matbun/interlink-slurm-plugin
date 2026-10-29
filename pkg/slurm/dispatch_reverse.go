package slurm

import (
	"context"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"

	"github.com/intertwin-eu/interlink-slurm-plugin/pkg/shadow"
)

// preExecAnnotation is shell the job script runs before the workload.
const preExecAnnotation = "slurm-job.vk.io/pre-exec"

// reverseJobName is the name of the workload's Slurm job, by which the srun
// carrier finds the allocation: the single-pod script names the job after its
// pod, a gang after its group (the workload UID).
func reverseJobName(req SubmitRequest) string {
	if req.PodSets[0].Count > 1 {
		return req.WorkloadUID
	}
	return memberUID(req.WorkloadUID, 0)
}

// reverseTunnel parses the reverse annotations of the workload's template, with
// the same rules as the virtual kubelet plus Shadow.Reverse.AllowedTargets. It is
// nil when the template asks for none.
func (d *Dispatcher) reverseTunnel(req SubmitRequest) (*shadow.ReverseTunnel, *apiError) {
	ps := req.PodSets[0]
	if !shadow.HasReverseAnnotations(ps.Template.Annotations) {
		return nil, nil
	}
	cfg := d.config()
	if !cfg.Shadow.Enabled {
		return nil, permanentError("%s is set, but this agent runs no shadows (Shadow.Enabled) to carry the tunnel", shadow.AnnReverseForwards)
	}
	// A site launcher starts every rank from the batch shell, where the tunnel's
	// /etc/hosts would reach ranks on nodes that have no relay.
	if ps.Count > 1 && selectedLauncherName(ps.Template.ObjectMeta) != "" {
		return nil, permanentError("%s cannot be combined with %s: only the head rank has the tunnel", shadow.AnnReverseForwards, LauncherAnnotation)
	}
	rcfg := cfg.Shadow.Reverse
	if err := shadow.NormalizeReverseConfig(&rcfg); err != nil {
		return nil, permanentError("%v", err)
	}
	rt, err := shadow.ParseReverseTunnel(ps.Template.Annotations, memberUID(req.WorkloadUID, 0), reverseJobName(req), rcfg)
	if err != nil {
		return nil, permanentError("%v", err)
	}
	if rt.SocksPort != 0 {
		log.G(context.Background()).Warningf("dispatch: workload %s: SOCKS proxy on 127.0.0.1:%d on the compute node: every user of that node can reach through it any endpoint the shadow reaches in the cluster", req.WorkloadUID, rt.SocksPort)
	}
	return rt, nil
}

// addReversePreExec puts the tunnel's set-up before the member's own pre-exec, as
// the virtual kubelet does. The forwarded names reach the job only through it: the
// plugin does not read spec.hostAliases. Only member 0 gets it: the shadow reaches
// the head rank's node and the other ranks have no tunnel end (multi-node tunnels
// are a non-goal).
func addReversePreExec(pod *v1.Pod, rt *shadow.ReverseTunnel) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[preExecAnnotation] = shadow.ReversePreExec(rt, pod.Namespace) + pod.Annotations[preExecAnnotation]
}
