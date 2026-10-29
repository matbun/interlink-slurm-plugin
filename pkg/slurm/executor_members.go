package slurm

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/containerd/containerd/log"
	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
)

// Environment every member's containers get.
const (
	RankEnv      = "INTERLINK_RANK"
	WorldSizeEnv = "INTERLINK_WORLD_SIZE"
)

// slurmFlagsAnnotation carries extra sbatch flags, as for a pod sent by the
// virtual kubelet.
const slurmFlagsAnnotation = "slurm-job.vk.io/flags"

// defaultVolumeMode is the file mode the API server defaults volumes to.
const defaultVolumeMode int32 = 0o644

// ownerName is the base of the members' names: the owner's, else the workload's.
func ownerName(req SubmitRequest) string {
	if req.Owner.Name != "" {
		return req.Owner.Name
	}
	return req.WorkloadName
}

// memberUID is the UID of member i of a workload.
func memberUID(workloadUID string, i int) string {
	return workloadUID + "-" + strconv.Itoa(i)
}

// PodSetAnnotation names the PodSet a member comes from.
const PodSetAnnotation = "interlink.eu/podset"

// workloadMembers is the number of members of the request: every PodSet's count.
func workloadMembers(req SubmitRequest) int {
	n := 0
	for _, ps := range req.PodSets {
		n += int(ps.Count)
	}
	return n
}

// memberPodSet returns the index of the PodSet member i comes from. Members are
// numbered across the PodSets in request order.
func memberPodSet(req SubmitRequest, i int) int {
	for p, ps := range req.PodSets {
		if i < int(ps.Count) {
			return p
		}
		i -= int(ps.Count)
	}
	return -1
}

// buildMember returns member i of the request as the pod the create path takes:
// its own PodSet's template, named after the owner, labelled with its completion
// index within the whole workload, with the site's account and partition among
// its sbatch flags. A workload of more than one member is a Kueue pod group of
// that size, member 0 (of the first PodSet) its head. References are not
// resolved here.
func buildMember(req SubmitRequest, i int) *v1.Pod {
	ps := req.PodSets[memberPodSet(req, i)]
	count := workloadMembers(req)
	tmpl := ps.Template.DeepCopy()

	labels := map[string]string{}
	for k, v := range tmpl.Labels {
		labels[k] = v
	}
	annotations := map[string]string{}
	for k, v := range tmpl.Annotations {
		annotations[k] = v
	}
	for _, k := range []string{PodGroupNameLabel, JobCompletionIndexLabel} {
		delete(labels, k)
	}
	for _, k := range []string{PodGroupTotalCountAnnotation, GangNameAnnotation, GangSizeAnnotation, GangRankAnnotation, GangRoleAnnotation} {
		delete(annotations, k)
	}
	labels[JobCompletionIndexLabel] = strconv.Itoa(i)
	annotations[PodSetAnnotation] = ps.Name
	if count > 1 {
		labels[PodGroupNameLabel] = req.WorkloadUID
		annotations[PodGroupTotalCountAnnotation] = strconv.Itoa(count)
		annotations[GangRankAnnotation] = strconv.Itoa(i)
		annotations[GangRoleAnnotation] = GangRoleWorker
		if i == 0 {
			annotations[GangRoleAnnotation] = GangRoleHead
		}
	}
	flags := strings.TrimSpace(annotations[slurmFlagsAnnotation] + " --account=" + req.Account)
	if ps.Partition != "" {
		flags += " --partition=" + ps.Partition
	}
	if ps.QOS != "" {
		flags += " --qos=" + ps.QOS
	}
	annotations[slurmFlagsAnnotation] = flags

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%d", ownerName(req), i),
			Namespace:   req.Namespace,
			UID:         types.UID(memberUID(req.WorkloadUID, i)),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: tmpl.Spec,
	}

	rankEnv := []v1.EnvVar{
		{Name: RankEnv, Value: strconv.Itoa(i)},
		{Name: WorldSizeEnv, Value: strconv.Itoa(count)},
	}
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			requestsAsLimits(&list[c].Resources)
			var env []v1.EnvVar
			for _, e := range list[c].Env {
				if e.Name != RankEnv && e.Name != WorldSizeEnv {
					env = append(env, e)
				}
			}
			list[c].Env = append(env, rankEnv...)
		}
	}

	for v := range pod.Spec.Volumes {
		src := &pod.Spec.Volumes[v].VolumeSource
		switch {
		case src.Secret != nil && src.Secret.DefaultMode == nil:
			src.Secret.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.ConfigMap != nil && src.ConfigMap.DefaultMode == nil:
			src.ConfigMap.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.Projected != nil && src.Projected.DefaultMode == nil:
			src.Projected.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.DownwardAPI != nil && src.DownwardAPI.DefaultMode == nil:
			src.DownwardAPI.DefaultMode = ptrInt32(defaultVolumeMode)
		}
	}
	// A workload of one plain pod is that pod: its downward API names it, as a workflow
	// engine's executor (Argo's) addresses its own pod by name and UID through the API
	// server, and an owner reference to a UID that does not exist is garbage collected.
	if count == 1 && req.Owner.Kind == "Pod" && req.Owner.Name != "" {
		pod.Name = req.Owner.Name
		if req.Owner.UID != "" {
			pod.Annotations[DownwardUIDAnnotation] = req.Owner.UID
		}
	}
	return pod
}

// requestsAsLimits gives a resource that has a request but no limit a limit equal to
// the request. Kueue charts a Workload by its requests and the job script is sized
// from limits, so without it a Job that requests 20 CPUs would ask Slurm for one.
func requestsAsLimits(r *v1.ResourceRequirements) {
	for name, q := range r.Requests {
		if _, ok := r.Limits[name]; ok {
			continue
		}
		if r.Limits == nil {
			r.Limits = v1.ResourceList{}
		}
		r.Limits[name] = q.DeepCopy()
	}
}

// serviceAccountDir is where the API server mounts a pod's ServiceAccount files.
const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// withServiceAccountDir does for a PodSet template what the API server's
// ServiceAccount admission does for a pod, since the workload's pods never exist:
// unless the template opts out or mounts the directory itself, every container gets
// /var/run/secrets/kubernetes.io/serviceaccount with namespace and ca.crt, and the
// token when token is true. Clients read their namespace there (Katib's SDK does).
func withServiceAccountDir(ps *ExecutorPodSet, token bool) {
	spec := &ps.Template.Spec
	if spec.AutomountServiceAccountToken != nil && !*spec.AutomountServiceAccountToken {
		return
	}
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range list {
			for _, m := range c.VolumeMounts {
				if m.MountPath == serviceAccountDir {
					return
				}
			}
		}
	}
	const name = "kube-api-access-interlink"
	var sources []v1.VolumeProjection
	if token {
		expiration := int64(3607)
		sources = append(sources, v1.VolumeProjection{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}})
	}
	sources = append(sources,
		v1.VolumeProjection{ConfigMap: &v1.ConfigMapProjection{LocalObjectReference: v1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []v1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		v1.VolumeProjection{DownwardAPI: &v1.DownwardAPIProjection{Items: []v1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
	)
	spec.Volumes = append(spec.Volumes, v1.Volume{Name: name, VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: sources}}})
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for c := range list {
			list[c].VolumeMounts = append(list[c].VolumeMounts, v1.VolumeMount{Name: name, MountPath: serviceAccountDir, ReadOnly: true})
		}
	}
	if !slices.Contains(ps.ConfigMapRefs, "kube-root-ca.crt") {
		ps.ConfigMapRefs = append(ps.ConfigMapRefs, "kube-root-ca.crt")
	}
}

// DownwardUIDAnnotation carries the UID a member's downward API reports when it is not
// the member's own (see buildMember).
const DownwardUIDAnnotation = "interlink.eu/downward-uid"

// withKubernetesAPI gives every container the API server's address, as the kubelet
// does, unless the container sets it itself.
func withKubernetesAPI(pod *v1.Pod, api ExecutorKubernetesAPI) {
	vars := []v1.EnvVar{
		{Name: "KUBERNETES_SERVICE_HOST", Value: api.Host},
		{Name: "KUBERNETES_SERVICE_PORT", Value: api.Port},
		{Name: "KUBERNETES_SERVICE_PORT_HTTPS", Value: api.Port},
	}
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			for _, v := range vars {
				if !slices.ContainsFunc(list[c].Env, func(e v1.EnvVar) bool { return e.Name == v.Name }) {
					list[c].Env = append(list[c].Env, v)
				}
			}
		}
	}
}

func ptrInt32(v int32) *int32 { return &v }

// podRefs are the Secrets and ConfigMaps of a workload, as read from the cluster.
// A name that is listed but absent, and only referenced as optional, is missing.
type podRefs struct {
	secrets    map[string]*v1.Secret
	configMaps map[string]*v1.ConfigMap
	// tokens are the minted service account tokens, by tokenKey.
	tokens map[string]string
}

// defaultTokenExpirationSeconds is what the API server defaults a
// serviceAccountToken projection's expirationSeconds to.
const defaultTokenExpirationSeconds int64 = 3600

// tokenKey names one serviceAccountToken projection: its volume and its path.
func tokenKey(volume, path string) string { return volume + "/" + path }

// mintTokens requests a token through the TokenRequest API for every
// serviceAccountToken projection of the template, as the kubelet does for a pod:
// for the pod's ServiceAccount, with the projection's audience and lifetime. A
// template without one makes no request. The tokens are not bound to a pod: the
// workload's pods never exist.
func (d *Executor) mintTokens(ctx context.Context, namespace string, spec v1.PodSpec) (map[string]string, *apiError) {
	tokens := map[string]string{}
	account := spec.ServiceAccountName
	if account == "" {
		account = "default"
	}
	for _, vol := range spec.Volumes {
		if vol.Projected == nil {
			continue
		}
		for _, src := range vol.Projected.Sources {
			p := src.ServiceAccountToken
			if p == nil {
				continue
			}
			if d.Kube == nil {
				return nil, transientError("the executor has no Kubernetes client to request a token for ServiceAccount %q", account)
			}
			expiration := defaultTokenExpirationSeconds
			if p.ExpirationSeconds != nil {
				expiration = *p.ExpirationSeconds
			}
			req := &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &expiration}}
			if p.Audience != "" {
				req.Spec.Audiences = []string{p.Audience}
			}
			tr, err := d.Kube.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, account, req, metav1.CreateOptions{})
			switch {
			case apierrors.IsNotFound(err), apierrors.IsForbidden(err):
				return nil, permanentError("volume %s: cannot request a token for ServiceAccount %q: %v", vol.Name, account, err)
			case err != nil:
				return nil, transientError("volume %s: requesting a token for ServiceAccount %q: %v", vol.Name, account, err)
			}
			tokens[tokenKey(vol.Name, p.Path)] = tr.Status.Token
		}
	}
	return tokens, nil
}

// templateRefs returns the Secrets and ConfigMaps the template references,
// each mapped to whether one of its references is not optional.
func templateRefs(spec v1.PodSpec) (secrets, configMaps map[string]bool) {
	secrets, configMaps = map[string]bool{}, map[string]bool{}
	add := func(m map[string]bool, name string, optional *bool) {
		if name == "" {
			return
		}
		m[name] = m[name] || optional == nil || !*optional
	}
	for _, vol := range spec.Volumes {
		switch {
		case vol.Secret != nil:
			add(secrets, vol.Secret.SecretName, vol.Secret.Optional)
		case vol.ConfigMap != nil:
			add(configMaps, vol.ConfigMap.Name, vol.ConfigMap.Optional)
		case vol.Projected != nil:
			for _, src := range vol.Projected.Sources {
				if src.Secret != nil {
					add(secrets, src.Secret.Name, src.Secret.Optional)
				}
				if src.ConfigMap != nil {
					add(configMaps, src.ConfigMap.Name, src.ConfigMap.Optional)
				}
			}
		}
	}
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range list {
			for _, ef := range c.EnvFrom {
				if ef.SecretRef != nil {
					add(secrets, ef.SecretRef.Name, ef.SecretRef.Optional)
				}
				if ef.ConfigMapRef != nil {
					add(configMaps, ef.ConfigMapRef.Name, ef.ConfigMapRef.Optional)
				}
			}
			for _, e := range c.Env {
				if e.ValueFrom == nil {
					continue
				}
				if r := e.ValueFrom.SecretKeyRef; r != nil {
					add(secrets, r.Name, r.Optional)
				}
				if r := e.ValueFrom.ConfigMapKeyRef; r != nil {
					add(configMaps, r.Name, r.Optional)
				}
			}
		}
	}
	return secrets, configMaps
}

// fetchRefs reads the PodSet's secretRefs and configMapRefs, and only those,
// from the executor's namespace. A reference of the template that the lists leave
// out, or a required one the executor cannot read, is a permanent refusal.
func (d *Executor) fetchRefs(ctx context.Context, namespace string, ps ExecutorPodSet) (podRefs, *apiError) {
	refs := podRefs{secrets: map[string]*v1.Secret{}, configMaps: map[string]*v1.ConfigMap{}}
	usedSecrets, usedConfigMaps := templateRefs(ps.Template.Spec)
	for name := range usedSecrets {
		if !slices.Contains(ps.SecretRefs, name) {
			return refs, permanentError("the template references Secret %q, which is not in secretRefs", name)
		}
	}
	for name := range usedConfigMaps {
		if !slices.Contains(ps.ConfigMapRefs, name) {
			return refs, permanentError("the template references ConfigMap %q, which is not in configMapRefs", name)
		}
	}
	if len(ps.SecretRefs)+len(ps.ConfigMapRefs) > 0 && d.Kube == nil {
		return refs, transientError("the executor has no Kubernetes client to read the workload's references")
	}

	readErr := func(kind, name string, required bool, err error) *apiError {
		switch {
		case apierrors.IsNotFound(err) && !required:
			return nil
		case apierrors.IsNotFound(err), apierrors.IsForbidden(err):
			return permanentError("cannot read %s %q: %v", kind, name, err)
		}
		return transientError("reading %s %q: %v", kind, name, err)
	}
	for _, name := range ps.SecretRefs {
		if _, done := refs.secrets[name]; done {
			continue
		}
		s, err := d.Kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if aerr := readErr("Secret", name, usedSecrets[name], err); aerr != nil {
				return refs, aerr
			}
			continue
		}
		refs.secrets[name] = s
	}
	for _, name := range ps.ConfigMapRefs {
		if _, done := refs.configMaps[name]; done {
			continue
		}
		cm, err := d.Kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if aerr := readErr("ConfigMap", name, usedConfigMaps[name], err); aerr != nil {
				return refs, aerr
			}
			continue
		}
		refs.configMaps[name] = cm
	}
	return refs, nil
}

// metadataFieldRefPattern matches metadata.labels['k'] and metadata.annotations['k'].
var metadataFieldRefPattern = regexp.MustCompile(`^metadata\.(labels|annotations)\[['"]?(.+?)['"]?\]$`)

// fieldRefValue resolves a fieldRef as the kubelet would for this pod. ok is
// false for a path the executor cannot know.
func fieldRefValue(pod *v1.Pod, path string) (string, bool) {
	switch path {
	case "metadata.name":
		return pod.Name, true
	case "metadata.namespace":
		return pod.Namespace, true
	case "metadata.uid":
		if uid := pod.Annotations[DownwardUIDAnnotation]; uid != "" {
			return uid, true
		}
		return string(pod.UID), true
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, true
	}
	if m := metadataFieldRefPattern.FindStringSubmatch(path); m != nil {
		if m[1] == "labels" {
			return pod.Labels[m[2]], true
		}
		return pod.Annotations[m[2]], true
	}
	return "", false
}

// resourceFieldRefValue resolves a resourceFieldRef as the kubelet would: the
// quantity divided by the divisor, rounded up. A limit that is not set falls
// back to the request.
func resourceFieldRefValue(pod *v1.Pod, self *v1.Container, ref *v1.ResourceFieldSelector) (string, error) {
	target := self
	if ref.ContainerName != "" && ref.ContainerName != self.Name {
		target = nil
		for _, list := range [][]v1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for c := range list {
				if list[c].Name == ref.ContainerName {
					target = &list[c]
				}
			}
		}
		if target == nil {
			return "", fmt.Errorf("resourceFieldRef: no container %q", ref.ContainerName)
		}
	}
	scope, name, ok := strings.Cut(ref.Resource, ".")
	if !ok {
		return "", fmt.Errorf("resourceFieldRef: unsupported resource %q", ref.Resource)
	}
	var qty resource.Quantity
	switch scope {
	case "requests":
		qty = target.Resources.Requests[v1.ResourceName(name)]
	case "limits":
		var set bool
		qty, set = target.Resources.Limits[v1.ResourceName(name)]
		if !set {
			qty = target.Resources.Requests[v1.ResourceName(name)]
		}
	default:
		return "", fmt.Errorf("resourceFieldRef: unsupported resource %q", ref.Resource)
	}
	divisor := ref.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	return strconv.FormatInt(int64(math.Ceil(float64(qty.MilliValue())/float64(divisor.MilliValue()))), 10), nil
}

// resolveEnv replaces every env[].valueFrom of the pod's containers by its value,
// as the virtual kubelet does before /create. An optional reference to a missing
// key drops the variable; a required one is a permanent refusal.
func resolveEnv(pod *v1.Pod, refs podRefs) *apiError {
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			container := &list[c]
			env := make([]v1.EnvVar, 0, len(container.Env))
			for _, e := range container.Env {
				if e.ValueFrom == nil {
					env = append(env, e)
					continue
				}
				value, keep, aerr := envFromSource(pod, container, e, refs)
				if aerr != nil {
					return aerr
				}
				if keep {
					env = append(env, v1.EnvVar{Name: e.Name, Value: value})
				}
			}
			container.Env = env
		}
	}
	return nil
}

func envFromSource(pod *v1.Pod, container *v1.Container, e v1.EnvVar, refs podRefs) (string, bool, *apiError) {
	src := e.ValueFrom
	optional := func(o *bool) bool { return o != nil && *o }
	switch {
	case src.SecretKeyRef != nil:
		r := src.SecretKeyRef
		if s := refs.secrets[r.Name]; s != nil {
			if v, ok := s.Data[r.Key]; ok {
				return string(v), true, nil
			}
			if v, ok := s.StringData[r.Key]; ok {
				return v, true, nil
			}
		}
		if optional(r.Optional) {
			return "", false, nil
		}
		return "", false, permanentError("container %s, env %s: Secret %q has no key %q", container.Name, e.Name, r.Name, r.Key)
	case src.ConfigMapKeyRef != nil:
		r := src.ConfigMapKeyRef
		if cm := refs.configMaps[r.Name]; cm != nil {
			if v, ok := cm.Data[r.Key]; ok {
				return v, true, nil
			}
		}
		if optional(r.Optional) {
			return "", false, nil
		}
		return "", false, permanentError("container %s, env %s: ConfigMap %q has no key %q", container.Name, e.Name, r.Name, r.Key)
	case src.FieldRef != nil:
		v, ok := fieldRefValue(pod, src.FieldRef.FieldPath)
		if !ok {
			log.G(context.Background()).Warningf("executor: container %s, env %s: fieldRef %s is unknown outside a node, left empty", container.Name, e.Name, src.FieldRef.FieldPath)
		}
		return v, true, nil
	case src.ResourceFieldRef != nil:
		v, err := resourceFieldRefValue(pod, container, src.ResourceFieldRef)
		if err != nil {
			return "", false, permanentError("container %s, env %s: %v", container.Name, e.Name, err)
		}
		return v, true, nil
	}
	return "", true, nil
}

// formatDownwardAPIMap renders labels or annotations as a downward API file does.
func formatDownwardAPIMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + strconv.Quote(m[k]) + "\n")
	}
	return b.String()
}

func downwardAPIFile(pod *v1.Pod, path string) (string, bool) {
	switch path {
	case "metadata.labels":
		return formatDownwardAPIMap(pod.Labels), true
	case "metadata.annotations":
		return formatDownwardAPIMap(pod.Annotations), true
	}
	return fieldRefValue(pod, path)
}

// retrievedPodData builds what the virtual kubelet sends to /create for this
// pod: every container gets the workload's Secrets and ConfigMaps, and the
// contents of the pod's projected and downward API volumes, service account
// tokens included (mintTokens). A downward API volume becomes a projected one, as
// the virtual kubelet does.
func retrievedPodData(ctx context.Context, pod *v1.Pod, refs podRefs) (commonIL.RetrievedPodData, *apiError) {
	var projected []v1.ConfigMap
	for v := range pod.Spec.Volumes {
		vol := &pod.Spec.Volumes[v]
		if vol.DownwardAPI != nil {
			items := vol.DownwardAPI.Items
			vol.VolumeSource = v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{
				DefaultMode: vol.DownwardAPI.DefaultMode,
				Sources:     []v1.VolumeProjection{{DownwardAPI: &v1.DownwardAPIProjection{Items: items}}},
			}}
		}
		if vol.Projected == nil {
			continue
		}
		cm := v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vol.Name}, Data: map[string]string{}}
		for _, src := range vol.Projected.Sources {
			switch {
			case src.Secret != nil:
				s := refs.secrets[src.Secret.Name]
				if s == nil {
					continue
				}
				data := map[string]string{}
				for k, val := range s.Data {
					data[k] = string(val)
				}
				if aerr := projectItems(cm.Data, data, src.Secret.Items, "Secret", src.Secret.Name); aerr != nil {
					return commonIL.RetrievedPodData{}, aerr
				}
			case src.ConfigMap != nil:
				c := refs.configMaps[src.ConfigMap.Name]
				if c == nil {
					continue
				}
				if aerr := projectItems(cm.Data, c.Data, src.ConfigMap.Items, "ConfigMap", src.ConfigMap.Name); aerr != nil {
					return commonIL.RetrievedPodData{}, aerr
				}
			case src.DownwardAPI != nil:
				for _, item := range src.DownwardAPI.Items {
					if item.FieldRef == nil {
						continue
					}
					if val, ok := downwardAPIFile(pod, item.FieldRef.FieldPath); ok {
						cm.Data[item.Path] = val
					}
				}
			case src.ServiceAccountToken != nil:
				token, ok := refs.tokens[tokenKey(vol.Name, src.ServiceAccountToken.Path)]
				if !ok {
					return commonIL.RetrievedPodData{}, transientError("volume %s: no token was requested for %s", vol.Name, src.ServiceAccountToken.Path)
				}
				cm.Data[src.ServiceAccountToken.Path] = token
			}
		}
		projected = append(projected, cm)
	}

	secrets := make([]v1.Secret, 0, len(refs.secrets))
	for _, name := range sortedKeys(refs.secrets) {
		secrets = append(secrets, *refs.secrets[name])
	}
	configMaps := make([]v1.ConfigMap, 0, len(refs.configMaps))
	for _, name := range sortedKeys(refs.configMaps) {
		configMaps = append(configMaps, *refs.configMaps[name])
	}
	data := commonIL.RetrievedPodData{Pod: *pod}
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, c := range list {
			data.Containers = append(data.Containers, commonIL.RetrievedContainer{
				Name:                c.Name,
				Secrets:             secrets,
				ConfigMaps:          configMaps,
				ProjectedVolumeMaps: projected,
			})
		}
	}
	return data, nil
}

// projectItems copies the keys items selects (all of them without items) of a
// Secret's or ConfigMap's data into a projected volume's files.
func projectItems(files, data map[string]string, items []v1.KeyToPath, kind, name string) *apiError {
	if len(items) == 0 {
		for k, v := range data {
			files[k] = v
		}
		return nil
	}
	for _, item := range items {
		v, ok := data[item.Key]
		if !ok {
			return permanentError("%s %q has no key %q", kind, name, item.Key)
		}
		files[item.Path] = v
	}
	return nil
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
