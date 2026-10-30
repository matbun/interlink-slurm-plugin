package slurm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// SubmitHandler generates and submits a SLURM batch script according to provided data.
// 1 Pod = 1 Job. If a Pod has multiple containers, every container is a line with it's parameters in the SLURM script.
func (h *SidecarHandler) SubmitHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "Create", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	log.G(h.Ctx).Info("Slurm Sidecar: received Submit call")
	statusCode := http.StatusOK
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	var data commonIL.RetrievedPodData
	err = json.Unmarshal(bodyBytes, &data)
	if err != nil {
		h.handleError(spanCtx, w, http.StatusBadRequest, err)
		return
	}

	returnedJID, cerr := h.createPod(spanCtx, data)
	if cerr != nil {
		if !cerr.noReply {
			h.handleError(spanCtx, w, cerr.code, cerr.err)
		}
		return
	}

	returnedJIDBytes, err := json.Marshal(returnedJID)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	w.WriteHeader(statusCode)

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(statusCode))

	w.Write(returnedJIDBytes)
}

// createError is a failed create: the HTTP status and error SubmitHandler answers
// with. noReply is set where SubmitHandler has always returned without writing.
// 503 means trying again later can work and 4xx that it cannot; the virtual
// kubelet treats every code the same.
type createError struct {
	code    int
	err     error
	noReply bool
}

// slurmFlagsAnnotation carries extra sbatch flags.
const slurmFlagsAnnotation = "slurm-job.vk.io/flags"

// preExecAnnotation is shell the job script runs before the workload.
const preExecAnnotation = "slurm-job.vk.io/pre-exec"

// slurmNamePattern is what an account, a partition or a QOS may contain: they end
// up in the job script's #SBATCH lines.
var slurmNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.,-]*$`)

// queueFlagNames names the flags checked against slurmNamePattern, by the key
// slurmFlagKey gives them.
var queueFlagNames = map[string]string{
	"--account":   "account",
	"--partition": "partition",
	"--qos":       "QOS",
	"-q":          "QOS",
}

// checkQueueFlags refuses an account, partition or QOS in the pod's sbatch flags
// that slurmNamePattern does not allow.
func checkQueueFlags(annotations map[string]string) error {
	for _, flag := range splitSlurmFlags(annotations[slurmFlagsAnnotation]) {
		name, ok := queueFlagNames[slurmFlagKey(flag)]
		if !ok {
			continue
		}
		if value := slurmFlagValue(flag); !slurmNamePattern.MatchString(value) {
			return fmt.Errorf("%s: invalid %s %q", slurmFlagsAnnotation, name, value)
		}
	}
	return nil
}

// sbatchLaterRe matches sbatch refusals that end once other jobs end or the
// controller answers again: limits on the number of submitted jobs, and an
// unreachable or busy controller. A refused account or an invalid script stays
// permanent.
var sbatchLaterRe = regexp.MustCompile(`(QOS|Assoc)MaxSubmitJob[A-Za-z]*|MaxSubmitJobs?Limit|Unable to contact slurm controller|Socket timed out|Slurm temporarily unable to accept job`)

// submitFailure is the answer to a failed sbatch. ssh failing (exit code 255) and
// the refusals of sbatchLaterRe are 503; any other refusal by sbatch, such as an
// invalid account or partition, is 400 with sbatch's text. Not running sbatch at
// all stays 504.
func submitFailure(err error) *createError {
	var se *SbatchError
	if !errors.As(err, &se) {
		return &createError{code: http.StatusGatewayTimeout, err: err}
	}
	if se.ExitCode == 255 {
		return &createError{code: http.StatusServiceUnavailable, err: fmt.Errorf("ssh to the site failed: %s", strings.TrimSpace(se.Stderr))}
	}
	if m := sbatchLaterRe.FindString(se.Stderr); m != "" {
		return &createError{code: http.StatusServiceUnavailable, err: fmt.Errorf("slurm takes no more jobs of this user now (%s): %s", m, strings.TrimSpace(se.Stderr))}
	}
	return &createError{code: http.StatusBadRequest, err: se}
}

// createPod renders the pod's job script and submits it, or buffers the pod when
// it is a gang member. A buffered member gets an empty PodJID.
func (h *SidecarHandler) createPod(spanCtx context.Context, data commonIL.RetrievedPodData) (CreateStruct, *createError) {
	span := trace.SpanFromContext(spanCtx)
	fail := func(code int, err error) (CreateStruct, *createError) {
		return CreateStruct{}, &createError{code: code, err: err}
	}

	if err := checkQueueFlags(data.Pod.Annotations); err != nil {
		return fail(http.StatusBadRequest, err)
	}

	containers := data.Pod.Spec.InitContainers
	containers = append(containers, data.Pod.Spec.Containers...)
	metadata := data.Pod.ObjectMeta
	filesPath := h.Config.DataRootFolder + data.Pod.Namespace + "-" + string(data.Pod.UID)
	workDir := getJobWorkDir(h.Config, metadata.Annotations, data.Pod.Namespace, string(data.Pod.UID))
	socketDir := socketDirFor(h.Config.SocketDirRoot, string(data.Pod.UID))

	if err := os.MkdirAll(filesPath, 0o755); err != nil {
		return fail(http.StatusInternalServerError, err)
	}
	// Resolve flavor to apply default CPU and memory
	flavor, err := resolveFlavor(spanCtx, h.Config, metadata, data.Pod.Spec.Containers)
	if err != nil {
		log.G(h.Ctx).Error("Failed to resolve flavor: ", err)
		return fail(http.StatusInternalServerError, err)
	}

	var runtime_command_pod []ContainerCommand
	var resourceLimits ResourceLimits

	isDefaultCPU := true
	isDefaultRam := true

	cpuLimit := int64(0)
	memoryLimit := int64(0)

	for i, container := range containers {
		log.G(h.Ctx).Info("- Beginning script generation for container " + container.Name)

		image := ""

		cpuLimitFloat := container.Resources.Limits.Cpu().AsApproximateFloat64()
		// Value() rounds up to whole bytes. AsInt64() would return (0, false) for
		// fractional quantities such as "10307921510400m" (Kubeflow-style limits),
		// which used to be misread as "no memory limit set".
		memoryLimitFromContainer := container.Resources.Limits.Memory().Value()

		cpuLimitFromContainer := int64(math.Ceil(cpuLimitFloat))

		if cpuLimitFromContainer == 0 {
			// No CPU limit specified in container, check if we should use flavor default
			if isDefaultCPU && flavor != nil && flavor.CPUDefault > 0 {
				log.G(h.Ctx).Infof("Max CPU resource not set for %s. Using flavor '%s' default: %d CPU", container.Name, flavor.FlavorName, flavor.CPUDefault)
				cpuLimit = flavor.CPUDefault
			} else if isDefaultCPU {
				log.G(h.Ctx).Warning(errors.New("Max CPU resource not set for " + container.Name + ". Only 1 CPU will be used"))
				cpuLimit = 1
			}
		} else {
			// Container specified CPU limit
			if cpuLimitFromContainer > cpuLimit {
				log.G(h.Ctx).Info("Setting CPU limit to " + strconv.FormatInt(cpuLimitFromContainer, 10))
				cpuLimit = cpuLimitFromContainer
			}
			isDefaultCPU = false
		}

		if memoryLimitFromContainer == 0 {
			// No memory limit specified in container, check if we should use flavor default
			if isDefaultRam && flavor != nil && flavor.MemoryDefault > 0 {
				log.G(h.Ctx).Infof("Max Memory resource not set for %s. Using flavor '%s' default: %d bytes", container.Name, flavor.FlavorName, flavor.MemoryDefault)
				memoryLimit = flavor.MemoryDefault
			} else if isDefaultRam {
				log.G(h.Ctx).Warning(errors.New("Max Memory resource not set for " + container.Name + ". Only 1MB will be used"))
				memoryLimit = 1024 * 1024
			}
		} else {
			// Container specified memory limit
			if memoryLimitFromContainer > memoryLimit {
				log.G(h.Ctx).Info("Setting Memory limit to " + strconv.FormatInt(memoryLimitFromContainer, 10))
				memoryLimit = memoryLimitFromContainer
			}
			isDefaultRam = false
		}

		resourceLimits.CPU = cpuLimit
		resourceLimits.Memory = memoryLimit

		mounts, err := prepareMounts(spanCtx, h.Config, &data, &container, workDir)
		log.G(h.Ctx).Debug(mounts)
		if err != nil {
			os.RemoveAll(filesPath)
			return fail(http.StatusGatewayTimeout, err)
		}

		// prepareEnvs creates a file in the working directory, that must exist. This is created at prepareMounts.
		envs := prepareEnvs(spanCtx, h.Config, data, container, workDir)
		image = prepareImage(spanCtx, h.Config, metadata, container.Image)
		commstr1 := prepareRuntimeCommand(h.Config, container, metadata)
		log.G(h.Ctx).Debug("-- Appending all commands together...")
		runtime_command := append(commstr1, envs...)
		switch h.Config.ContainerRuntime {
		case "singularity":
			runtime_command = append(runtime_command, mounts)
			runtime_command = append(runtime_command, image)
		case "enroot":
			containerName := container.Name + string(data.Pod.UID)
			mounts = strings.ReplaceAll(mounts, ":ro", "")
			runtime_command = append(runtime_command, mounts)
			runtime_command = append(runtime_command, containerName)
		}
		runtime_command = injectSocketDir(runtime_command, h.Config.ContainerRuntime, socketDir)

		isInit := false

		if i < len(data.Pod.Spec.InitContainers) {
			isInit = true
		}

		span.SetAttributes(
			attribute.String("job.container"+strconv.Itoa(i)+".name", container.Name),
			attribute.Bool("job.container"+strconv.Itoa(i)+".isinit", isInit),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".envs", envs),
			attribute.String("job.container"+strconv.Itoa(i)+".image", image),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".command", container.Command),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".args", container.Args),
		)

		// Process probes if enabled
		var readinessProbes, livenessProbes, startupProbes []ProbeCommand
		if h.Config.EnableProbes && !isInit {
			readinessProbes, livenessProbes, startupProbes = translateKubernetesProbes(spanCtx, container)
			if len(readinessProbes) > 0 || len(livenessProbes) > 0 || len(startupProbes) > 0 {
				log.G(h.Ctx).Info("-- Container " + container.Name + " has probes configured")
				span.SetAttributes(
					attribute.Int("job.container"+strconv.Itoa(i)+".readiness_probes", len(readinessProbes)),
					attribute.Int("job.container"+strconv.Itoa(i)+".liveness_probes", len(livenessProbes)),
					attribute.Int("job.container"+strconv.Itoa(i)+".startup_probes", len(startupProbes)),
				)
			}
		}

		// Translate preStop and postStart lifecycle hooks (init containers do not support lifecycle hooks)
		var preStopHook *LifecycleHookSpec
		var postStartHook *LifecycleHookSpec
		if !isInit && container.Lifecycle != nil {
			preStopHook = translateLifecycleHook(container.Lifecycle.PreStop)
			if preStopHook != nil {
				log.G(h.Ctx).Info("-- Container " + container.Name + " has a preStop lifecycle hook configured")
			}
			postStartHook = translateLifecycleHook(container.Lifecycle.PostStart)
			if postStartHook != nil {
				log.G(h.Ctx).Info("-- Container " + container.Name + " has a postStart lifecycle hook configured")
			}
		}

		runtime_command_pod = append(runtime_command_pod, ContainerCommand{
			runtimeCommand:   runtime_command,
			containerName:    container.Name,
			containerArgs:    expandContainerArgs(container.Args, container.Env),
			containerCommand: expandContainerArgs(container.Command, container.Env),
			isInitContainer:  isInit,
			readinessProbes:  readinessProbes,
			livenessProbes:   livenessProbes,
			startupProbes:    startupProbes,
			containerImage:   image,
			preStopHook:      preStopHook,
			postStartHook:    postStartHook,
		})
	}

	span.SetAttributes(
		attribute.Int64("job.limits.cpu", resourceLimits.CPU),
		attribute.Int64("job.limits.memory", resourceLimits.Memory),
	)

	var path string

	// If workDir differs from filesPath, ensure the working directory exists.
	if workDir != filesPath {
		if err = os.MkdirAll(workDir, os.ModePerm); err != nil {
			log.G(h.Ctx).Error("Failed to create job working directory: ", err)
			os.RemoveAll(filesPath)
			return fail(http.StatusInternalServerError, err)
		}
	}

	if data.JobScript == "" {
		log.G(h.Ctx).Info("-- No custom job script provided, generating one...")
		path, err = produceSLURMScript(spanCtx, h.Config, data.Pod, workDir, metadata, runtime_command_pod, resourceLimits, isDefaultCPU, isDefaultRam, flavor)
		if err != nil {
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			if workDir != filesPath {
				os.RemoveAll(workDir)
			}
			return CreateStruct{}, &createError{err: err, noReply: true}
		}
	} else {

		pathFile, err := createFile(workDir + "/jobScript.sh")
		if err != nil {
			log.G(h.Ctx).Error("Unable to create file ", workDir, "/jobScript.sh")
			log.G(h.Ctx).Error(err)
			span.AddEvent("Failed to submit the SLURM Job")
			//os.RemoveAll(filesPath)
			return fail(http.StatusInternalServerError, err)
		}

		mode := os.FileMode(0770)

		// Change the file mode
		if err := os.Chmod(workDir+"/jobScript.sh", mode); err != nil {
			panic(err)
		}

		_, err = pathFile.Write([]byte(data.JobScript))
		if errClose := pathFile.Close(); err == nil {
			err = errClose
		}
		if err != nil {
			log.G(h.Ctx).Error("Unable to write to file ", workDir, "/jobScript.sh")
			log.G(h.Ctx).Error(err)
			span.AddEvent("Failed to submit the SLURM Job")
			//os.RemoveAll(filesPath)
			return fail(http.StatusInternalServerError, err)
		}
		runtime_command_pod := append([]ContainerCommand{}, ContainerCommand{
			runtimeCommand:   []string{pathFile.Name()},
			containerName:    "jobScript",
			containerArgs:    []string{},
			containerCommand: []string{},
			isInitContainer:  false,
			readinessProbes:  []ProbeCommand{},
			livenessProbes:   []ProbeCommand{},
			startupProbes:    []ProbeCommand{},
			containerImage:   "n/a",
		})

		path, err = produceSLURMScript(spanCtx, h.Config, data.Pod, workDir, metadata, runtime_command_pod, resourceLimits, isDefaultCPU, isDefaultRam, flavor)
		if err != nil {
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			if workDir != filesPath {
				os.RemoveAll(workDir)
			}
			return CreateStruct{}, &createError{err: err, noReply: true}
		}
	}

	// Gang-scheduling branch: when the feature is enabled AND the pod carries the
	// interlink.eu/gang-name annotation, do NOT submit one sbatch here. Everything
	// above (mounts/envs/runtime_command_pod + this member's own job.sh via
	// produceSLURMScript) has already run, so the member is fully rendered. We
	// buffer it; only the arrival that completes the gang submits ONE
	// `sbatch --nodes=N` for the whole group and back-fills every member's JID.
	// A pod WITHOUT the annotation (or with the feature off) skips this block
	// entirely and takes the unchanged single-pod path below.
	if isGangPod(h.Config, metadata) {
		size, err := gangSizeFromMeta(metadata)
		if err != nil {
			os.RemoveAll(filesPath)
			return fail(http.StatusInternalServerError, err)
		}
		member := gangMemberFromCreate(data.Pod, filesPath, runtime_command_pod, resourceLimits, isDefaultCPU, isDefaultRam, flavor)
		gangJID, submitted, err := h.bufferGangMember(spanCtx, member, size)
		if err != nil {
			span.AddEvent("Failed to submit the gang SLURM Job")
			os.RemoveAll(filesPath)
			return CreateStruct{}, submitFailure(err)
		}

		// Respond 200 either way (contract-legal):
		//   - not yet submitted -> empty PodJID; the VK stamps it unvalidated and
		//     the pod stays Pending until Status sees this UID's JID appear.
		//   - submitted -> the shared gang JID for THIS pod; siblings reconcile via
		//     their own Status re-polls (no VK re-Create needed).
		if submitted {
			span.AddEvent("Gang SLURM Job successfully submitted with ID " + gangJID)
			return CreateStruct{PodUID: string(data.Pod.UID), PodJID: gangJID}, nil
		}
		span.AddEvent("Gang member buffered, awaiting quorum")
		return CreateStruct{PodUID: string(data.Pod.UID), PodJID: ""}, nil
	}

	out, err := SLURMBatchSubmit(h.Ctx, h.Config, path)
	if err != nil {
		span.AddEvent("Failed to submit the SLURM Job")
		os.RemoveAll(filesPath)
		if workDir != filesPath {
			os.RemoveAll(workDir)
		}
		return CreateStruct{}, submitFailure(err)
	}
	log.G(h.Ctx).Info(out)
	jid, err := handleJidAndPodUid(h.Ctx, data.Pod, h.JIDs, out, filesPath, workDir)
	if err != nil {
		os.RemoveAll(filesPath)
		if workDir != filesPath {
			os.RemoveAll(workDir)
		}
		podUID := string(data.Pod.UID)
		if h.JIDs != nil {
			if _, ok := (*h.JIDs)[podUID]; ok {
				if derr := deleteContainer(spanCtx, h.Config, podUID, h.JIDs, workDir); derr != nil {
					log.G(h.Ctx).Error(derr)
				}
			} else {
				log.G(h.Ctx).Info("Skipping deleteContainer cleanup because no JID was registered for pod UID " + podUID)
			}
		}
		return fail(http.StatusGatewayTimeout, err)
	}

	span.AddEvent("SLURM Job successfully submitted with ID " + jid)
	return CreateStruct{PodUID: string(data.Pod.UID), PodJID: jid}, nil
}
