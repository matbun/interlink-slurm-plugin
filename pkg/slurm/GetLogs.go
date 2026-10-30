package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// followPollInterval is how long a followed log waits before reading again.
var followPollInterval = 4 * time.Second

// followNoticeAfter is how long a followed log waits for its file before it tells
// the caller so: the virtual kubelet's request times out after 30 s without a
// byte, while any caller shows the notice as a log line.
var followNoticeAfter = 20 * time.Second

// followReadRetries is how many reads in a row may fail before a followed log
// gives up. A network filesystem (sshfs, rclone) can fail a read of a file the
// job is writing on another host; the next read usually works.
const followReadRetries = 5

// hasJob reports whether the pod has a job, under GangMu so that a delete's
// removal of the job is seen consistently.
func (h *SidecarHandler) hasJob(ctx context.Context, podUID string) bool {
	h.GangMu.Lock()
	defer h.GangMu.Unlock()
	return checkIfJidExists(ctx, h.JIDs, podUID)
}

// containerEnded reports whether a followed container has nothing more to write:
// it wrote its status file, or the last status read reported it terminated, as for
// a job cancelled, timed out or killed before the container could write the file.
func containerEnded(podUID, container, statusPath string) bool {
	if _, err := os.Stat(statusPath); err == nil {
		return true
	}
	return reportedTerminated(podUID, container)
}

// Logs in follow mode (get logs until the death of the container) with "kubectl -f".
func (h *SidecarHandler) GetLogsFollowMode(
	spanCtx context.Context,
	podUid string,
	w http.ResponseWriter,
	r *http.Request,
	path string,
	req commonIL.LogStruct,
	containerOutputPath string,
	containerOutput []byte,
	jobOutputLen int,
	sessionContext string,
) error {
	// Follow until this file exist, that indicates the end of container, thus the end of following.
	containerStatusPath := path + "/run-" + req.ContainerName + ".status"
	// Get the offset of what we read.
	containerOutputLastOffset := len(containerOutput)
	sessionContextMessage := GetSessionContextMessage(sessionContext)
	log.G(h.Ctx).Debug(sessionContextMessage, "Check container status", containerStatusPath, " with current length/offset: ", containerOutputLastOffset)

	hadJob := false
	waitStart, noticed := time.Now(), false
	for {
		_, err := os.Stat(containerOutputPath)
		if err == nil {
			log.G(h.Ctx).Debug(sessionContextMessage, "found for follow mode the container logs ", containerOutputPath)
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			// Case unknown error.
			errWithContext := fmt.Errorf(sessionContextMessage+"could not open file to follow logs at %s error type: %s error: %w", containerOutputPath, fmt.Sprintf("%#v", err), err)
			log.G(h.Ctx).Error(errWithContext)
			w.Write([]byte(errWithContext.Error()))
			return err
		}
		// A container that ended, or a pod deleted, before the log appeared
		// leaves nothing to follow.
		hasJob := h.hasJob(spanCtx, podUid)
		if (hadJob && !hasJob) || containerEnded(podUid, req.ContainerName, containerStatusPath) || r.Context().Err() != nil {
			log.G(h.Ctx).Info(sessionContextMessage, "Container ended before its log ", containerOutputPath, " appeared, exiting following mode...")
			return nil
		}
		hadJob = hadJob || hasJob
		// Case the file does not exist yet, we loop until it exist.
		notFoundMsg := sessionContextMessage + "Cannot open in follow mode the container logs " + containerOutputPath + " because it does not exist yet, sleeping before retrying..."
		log.G(h.Ctx).Debug(notFoundMsg)
		// Warning: if we don't write anything to body before 30s, there will be a timeout in VK to API DoReq(), thus we send an
		// informational message that the log is not ready, once and on its own line, so a short wait leaves the log clean.
		if !noticed && time.Since(waitStart) >= followNoticeAfter {
			noticed = true
			w.Write([]byte(notFoundMsg + "\n"))
			// Flush otherwise it will be as if nothing was written.
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		time.Sleep(followPollInterval)
	}

	// Looping until we get end of job. The file is opened again at every read, at
	// the offset reached so far: on a network filesystem (sshfs, rclone) an open
	// handle may never see what the job appends from another host.
	offset := int64(containerOutputLastOffset)
	// The job's own output (job.out) is followed too, as whole lines between the
	// container's lines, so what the launcher writes after the start shows up.
	jobOutPath, jobOffset := path+"/job.out", int64(jobOutputLen)
	atLineStart := len(containerOutput) == 0 || containerOutput[len(containerOutput)-1] == '\n'
	var isContainerDead bool = false
	failedReads := 0
	for {
		data, errRead := readLogFrom(containerOutputPath, offset)
		n := int64(len(data))
		if n > 0 {
			if _, err := w.Write(data); err != nil {
				return err
			}
			offset += n
			atLineStart = data[n-1] == '\n'
		}
		if errRead != nil {
			// Nothing is written to the stream: it is the job's log. A failed read is
			// tried again from the offset reached, which counts only copied bytes.
			failedReads++
			log.G(h.Ctx).Warning(sessionContextMessage, "reading the log in GetLogsFollowMode (", failedReads, " in a row): ", errRead)
			if failedReads >= followReadRetries || r.Context().Err() != nil {
				return errRead
			}
			time.Sleep(followPollInterval)
			continue
		}
		failedReads = 0
		if atLineStart {
			copyNewLines(w, jobOutPath, &jobOffset, false)
		}
		// Flush otherwise it will take time to appear in kubectl logs.
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		} else {
			log.G(h.Ctx).Error(sessionContextMessage, "wrote some logs but could not flush because server does not support Flusher.")
		}
		if n > 0 {
			continue
		}
		// Nothing more to read, but in follow mode, is the container still alive?
		if isContainerDead {
			// Container already marked as dead, and we tried to get logs one last time. Exiting the loop.
			log.G(h.Ctx).Info(sessionContextMessage, "Container was found dead and no more logs are found at this step, exiting following mode...")
			if !atLineStart {
				_, _ = w.Write([]byte("\n"))
			}
			copyNewLines(w, jobOutPath, &jobOffset, true)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			break
		}
		// Checking if container is dead (meaning the job ID is not in context anymore, OR if the status file exist,
		// OR if the status reported it terminated).
		switch {
		case !h.hasJob(spanCtx, podUid):
			// The JID disappeared, so the container is dead, probably from a POD delete request. Trying to get the latest log one last time.
			isContainerDead = true
			log.G(h.Ctx).Info(sessionContextMessage, "Container is found dead thanks to missing JID, reading last logs...")
		case containerEnded(podUid, req.ContainerName, containerStatusPath):
			// The status file exists or the status reported the container terminated. Trying to get the latest log one last time.
			isContainerDead = true
			log.G(h.Ctx).Info(sessionContextMessage, "Container is found dead thanks to status file or status, reading last logs...")
		case r.Context().Err() != nil:
			// The caller went away.
			return nil
		default:
			// The container is still alive: sleep, so the file system is not read again at once.
			time.Sleep(followPollInterval)
		}
	}
	// No error, err = nil
	return nil
}

// readLogFrom reads the file at path from offset to its current end, opening it
// again each time: on a network filesystem (sshfs, rclone) an open handle may never
// see what the job appends from another host. A file that shrank below offset
// (truncated) is read again from the start; a missing file reads as empty. On a
// failed read it returns what it read before the error.
func readLogFrom(path string, offset int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil && fi.Size() < offset {
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

// copyNewLines writes to w the complete lines of the file at path past *offset
// and moves *offset after them; with all, also a last line without a newline,
// ended by one. A failed read writes nothing, and the next call tries again.
func copyNewLines(w io.Writer, path string, offset *int64, all bool) {
	data, err := readLogFrom(path, *offset)
	if err != nil {
		return
	}
	if !all {
		data = data[:bytes.LastIndexByte(data, '\n')+1]
	}
	if len(data) == 0 {
		return
	}
	*offset += int64(len(data))
	if data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	_, _ = w.Write(data)
}

// GetLogsHandler reads Jobs' output file to return what's logged inside.
// What's returned is based on the provided parameters (Tail/LimitBytes/Timestamps/etc)
func (h *SidecarHandler) GetLogsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "GetLogs", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	// For debugging purpose, when we have many kubectl logs, we can differentiate each one.
	sessionContext := GetSessionContext(r)
	sessionContextMessage := GetSessionContextMessage(sessionContext)

	log.G(h.Ctx).Info(sessionContextMessage, "Docker Sidecar: received GetLogs call")
	var req commonIL.LogStruct

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during ReadAll() in GetLogsHandler request body", spanCtx, w, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during Unmarshal() in GetLogsHandler request body", spanCtx, w, err)
		return
	}

	h.getLogs(spanCtx, start, req, w, r, sessionContext)
}

// getLogs writes the logs req asks for to w, following them when req.Opts.Follow
// is set. start is when the request arrived, for the span's duration.
func (h *SidecarHandler) getLogs(spanCtx context.Context, start int64, req commonIL.LogStruct, w http.ResponseWriter, r *http.Request, sessionContext string) {
	span := trace.SpanFromContext(spanCtx)
	sessionContextMessage := GetSessionContextMessage(sessionContext)
	currentTime := time.Now()

	span.SetAttributes(
		attribute.String("pod.name", req.PodName),
		attribute.String("pod.namespace", req.Namespace),
		attribute.Int("opts.limitbytes", req.Opts.LimitBytes),
		attribute.Int("opts.since", req.Opts.SinceSeconds),
		attribute.Int64("opts.sincetime", req.Opts.SinceTime.UnixMicro()),
		attribute.Int("opts.tail", req.Opts.Tail),
		attribute.Bool("opts.follow", req.Opts.Follow),
		attribute.Bool("opts.previous", req.Opts.Previous),
		attribute.Bool("opts.timestamps", req.Opts.Timestamps),
	)

	// Resolve the job working directory.
	// Priority: 1) in-memory JIDs cache  2) WorkDir.path file on disk (survives sidecar restarts)
	// The metadata directory is always at the default DataRootFolder path and is
	// the reliable location for the WorkDir.path pointer file.
	metadataPath := h.Config.DataRootFolder + req.Namespace + "-" + req.PodUID
	path := metadataPath
	if jid, ok := (*h.JIDs)[req.PodUID]; ok && jid.WorkDir != "" {
		path = jid.WorkDir
	} else if workDirBytes, rdErr := os.ReadFile(metadataPath + "/WorkDir.path"); rdErr == nil && len(workDirBytes) > 0 {
		if workDirPath := strings.TrimSpace(string(workDirBytes)); workDirPath != "" {
			path = workDirPath
		}
	}
	containerOutputPath := path + "/run-" + req.ContainerName + ".out"
	var output []byte
	if req.Opts.Timestamps {
		// h.logErrorVerbose(sessionContextMessage+"unsupported option req.Opts.Timestamps, ignoring it", spanCtx, w, err)
		// TODO: support for timestamps.
		log.G(h.Ctx).Warning(sessionContextMessage, "unsupported option req.Opts.Timestamps, ignoring it")
		// return
	}
	containerOutput, err := h.ReadLogs(containerOutputPath, span, spanCtx, w, sessionContextMessage)
	if err != nil {
		log.G(h.Ctx).Warning(sessionContextMessage, "cannot find any container with this name, falling back to init containers")
		containerOutputPath := path + "/init-" + req.ContainerName + ".out"
		containerOutput, err = h.ReadLogs(containerOutputPath, span, spanCtx, w, sessionContextMessage)
		if err != nil {
			// Error already handled in waitAndReadLogs
			log.G(h.Ctx).Warning(sessionContextMessage, "cannot find any log for this container")
			return
		}
	}
	jobOutput, err := h.ReadLogs(path+"/"+"job.out", span, spanCtx, w, sessionContextMessage)
	if err != nil {
		// Error already handled in waitAndReadLogs
		return
	}

	output = append(output, jobOutput...)
	output = append(output, containerOutput...)

	var returnedLogs string

	if req.Opts.Tail != 0 {
		var lastLines []string

		splittedLines := strings.Split(string(output), "\n")

		if req.Opts.Tail > len(splittedLines) {
			lastLines = splittedLines
		} else {
			lastLines = splittedLines[len(splittedLines)-req.Opts.Tail-1:]
		}

		for _, line := range lastLines {
			returnedLogs += line + "\n"
		}
	} else if req.Opts.LimitBytes != 0 {
		var lastBytes []byte
		if req.Opts.LimitBytes > len(output) {
			lastBytes = output
		} else {
			lastBytes = output[len(output)-req.Opts.LimitBytes-1:]
		}

		returnedLogs = string(lastBytes)
	} else {
		returnedLogs = string(output)
	}

	if req.Opts.Timestamps && (req.Opts.SinceSeconds != 0 || !req.Opts.SinceTime.IsZero()) {
		temp := returnedLogs
		returnedLogs = ""
		splittedLogs := strings.Split(temp, "\n")
		timestampFormat := "2006-01-02T15:04:05.999999999Z"

		for _, Log := range splittedLogs {
			part := strings.SplitN(Log, " ", 2)
			timestampString := part[0]
			timestamp, err := time.Parse(timestampFormat, timestampString)
			if err != nil {
				continue
			}
			if req.Opts.SinceSeconds != 0 {
				if currentTime.Sub(timestamp).Seconds() > float64(req.Opts.SinceSeconds) {
					returnedLogs += Log + "\n"
				}
			} else {
				if timestamp.Sub(req.Opts.SinceTime).Seconds() >= 0 {
					returnedLogs += Log + "\n"
				}
			}
		}
	}

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(http.StatusOK))

	// w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Content-Type", "text/plain")

	log.G(h.Ctx).Info(sessionContextMessage, "writing response headers and OK status")
	w.WriteHeader(http.StatusOK)

	log.G(h.Ctx).Info(sessionContextMessage, "writing response body len: ", len(returnedLogs))
	n, err := w.Write([]byte(returnedLogs))
	log.G(h.Ctx).Info(sessionContextMessage, "written response body len: ", n)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during Write() in GetLogsHandler, could write bytes: "+strconv.Itoa(n), spanCtx, w, err)
		return
	}

	// Flush or else, it could be lost in the pipe.
	if f, ok := w.(http.Flusher); ok {
		log.G(h.Ctx).Debug(sessionContextMessage, "flushing after wrote response body bytes: "+strconv.Itoa(n))
		f.Flush()
	} else {
		log.G(h.Ctx).Error(sessionContextMessage, "wrote response body but could not flush because server does not support Flusher.")
		return
	}

	if req.Opts.Follow {
		err := h.GetLogsFollowMode(spanCtx, req.PodUID, w, r, path, req, containerOutputPath, containerOutput, len(jobOutput), sessionContext)
		if err != nil {
			h.logErrorVerbose(sessionContextMessage+"follow mode error", spanCtx, w, err)
		}
	}
}

// Goal: read the file if it exist. If not, return empty.
// Important to wait because if we don't wait and return empty array, it will generates a JSON unmarshall error in InterLink VK.
// Fail for any error not related to file not existing (eg: permission error will raise an error).
// Already handle error.
func (h *SidecarHandler) ReadLogs(logsPath string, span trace.Span, ctx context.Context, w http.ResponseWriter, sessionContextMessage string) ([]byte, error) {
	var output []byte
	var err error
	log.G(h.Ctx).Info(sessionContextMessage, "reading file ", logsPath)
	output, err = os.ReadFile(logsPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.G(h.Ctx).Info(sessionContextMessage, "file ", logsPath, " not found.")
			output = make([]byte, 0)
		} else {
			span.AddEvent("Error retrieving logs")
			h.logErrorVerbose(sessionContextMessage+"error during ReadFile() of readLogs() in GetLogsHandler of file "+logsPath, ctx, w, err)
			return nil, err
		}
	}
	return output, nil
}
