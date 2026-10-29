package slurm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
)

// logsPollInterval is how often a followed workload log is read again.
var logsPollInterval = time.Second

// logsDrainQuiet is how long the logs of an ended job must stay unchanged before a
// followed stream ends. The head rank's log is completed after the ranks exit, and
// a job directory served over SFTP shows a file's new size a moment late, so the
// first read after the end can miss the last lines.
var logsDrainQuiet = 10 * time.Second

// GetLogsHandler is /getLogs. With workloadUID in the query it answers with the
// workload's logs; without, it is the plugin's per-pod /getLogs.
//
// Query: workloadUID, container (default: each member's first container), rank,
// follow, tail, sinceSeconds. With rank, that member's log as the per-pod path
// reads it. Without rank, the lines of every member that has the container,
// prefixed "[rank N] " when there are more than one member: rank by rank, or as
// they appear with follow, until the job ended and every file is drained.
func (d *Executor) GetLogsHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !q.Has("workloadUID") {
		d.Sidecar.GetLogsHandler(w, r)
		return
	}
	start := time.Now().UnixMicro()
	uid := q.Get("workloadUID")
	rec := d.record(uid)
	if rec == nil {
		http.Error(w, "no job for workload "+uid, http.StatusNotFound)
		return
	}
	container := q.Get("container")
	intParam := func(name string) (int, bool) {
		raw := q.Get(name)
		if raw == "" {
			return 0, true
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			http.Error(w, "invalid "+name+" "+strconv.Quote(raw), http.StatusBadRequest)
			return 0, false
		}
		return n, true
	}
	tail, ok := intParam("tail")
	if !ok {
		return
	}
	since, ok := intParam("sinceSeconds")
	if !ok {
		return
	}
	follow := q.Get("follow") == "true"
	members := memberPods(rec)
	logs := memberLogs(members, container)
	if len(logs) == 0 {
		http.Error(w, "no container "+container+" in workload "+uid, http.StatusBadRequest)
		return
	}

	if q.Has("rank") {
		rank, ok := intParam("rank")
		if !ok {
			return
		}
		if rank >= len(members) {
			http.Error(w, fmt.Sprintf("rank %d: workload %s has %d member(s)", rank, uid, len(members)), http.StatusBadRequest)
			return
		}
		i := slices.IndexFunc(logs, func(l memberLog) bool { return l.rank == rank })
		if i < 0 {
			http.Error(w, fmt.Sprintf("rank %d of workload %s has no container %s", rank, uid, container), http.StatusBadRequest)
			return
		}
		m := logs[i].pod
		req := commonIL.LogStruct{
			Namespace:     m.Namespace,
			PodUID:        string(m.UID),
			PodName:       m.Name,
			ContainerName: logs[i].container,
			Opts:          commonIL.ContainerLogOpts{Tail: tail, Follow: follow, SinceSeconds: since},
		}
		d.Sidecar.getLogs(r.Context(), start, req, w, r, GetSessionContext(r))
		return
	}

	sources := d.logSources(logs, len(members) > 1)
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	flush := func() {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	var first bytes.Buffer
	for _, s := range sources {
		out, err := s.readNew(!follow)
		if err != nil {
			log.G(d.Sidecar.Ctx).Warning("executor: logs of workload ", uid, ": ", err)
		}
		first.Write(out)
	}
	if _, err := w.Write(lastLines(first.Bytes(), tail)); err != nil {
		return
	}
	flush()
	if !follow {
		return
	}

	ticker := time.NewTicker(logsPollInterval)
	defer ticker.Stop()
	var quietSince time.Time
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
		// Checked before reading: whatever the job wrote before it ended is read
		// in this pass.
		ended := d.logsEnded(uid, logs)
		wrote := false
		for _, s := range sources {
			out, err := s.readNew(ended)
			if err != nil {
				log.G(d.Sidecar.Ctx).Warning("executor: logs of workload ", uid, ": ", err)
				continue
			}
			if len(out) == 0 {
				continue
			}
			wrote = true
			if _, err := w.Write(out); err != nil {
				return
			}
		}
		flush()
		if !ended {
			continue
		}
		if wrote || quietSince.IsZero() {
			quietSince = time.Now()
			continue
		}
		if time.Since(quietSince) >= logsDrainQuiet {
			return
		}
	}
}

func hasContainer(list []v1.Container, name string) bool {
	for _, c := range list {
		if c.Name == name {
			return true
		}
	}
	return false
}

// memberLog is the log of one container of one member.
type memberLog struct {
	rank      int
	pod       *v1.Pod
	container string
	isInit    bool
}

// memberLogs lists the members that have the container, or every member's first
// container when none is named: members of different PodSets can have different
// containers.
func memberLogs(members []*v1.Pod, container string) []memberLog {
	var logs []memberLog
	for i, m := range members {
		l := memberLog{rank: i, pod: m, container: container}
		switch {
		case container == "":
			l.container = m.Spec.Containers[0].Name
		case hasContainer(m.Spec.Containers, container):
		case hasContainer(m.Spec.InitContainers, container):
			l.isInit = true
		default:
			continue
		}
		logs = append(logs, l)
	}
	return logs
}

func (l memberLog) kind() string {
	if l.isInit {
		return "init-"
	}
	return "run-"
}

// logSource is one log file of one member, read from where the last read ended.
type logSource struct {
	prefix  string
	path    string
	offset  int64
	partial []byte
}

// logSources lists, rank by rank, the files the per-pod log path reads: the
// member's job.out, then its container's output. With prefix each line names its
// rank.
func (d *Executor) logSources(logs []memberLog, prefix bool) []*logSource {
	var sources []*logSource
	for _, l := range logs {
		p := ""
		if prefix {
			p = "[rank " + strconv.Itoa(l.rank) + "] "
		}
		_, workDir := d.memberDirs(l.pod)
		sources = append(sources,
			&logSource{prefix: p, path: workDir + "/job.out"},
			&logSource{prefix: p, path: workDir + "/" + l.kind() + l.container + ".out"},
		)
	}
	return sources
}

// readNew returns the complete lines written since the last read, each with the
// prefix. With flush the last line counts as complete even without a newline.
func (s *logSource) readNew(flush bool) ([]byte, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		return s.lines(nil, flush), err
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil && fi.Size() < s.offset {
		// Truncated, as srun --open-mode=truncate does: start over.
		s.offset, s.partial = 0, nil
	}
	if _, err := f.Seek(s.offset, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	s.offset += int64(len(data))
	return s.lines(data, flush), err
}

func (s *logSource) lines(data []byte, flush bool) []byte {
	buf := append(s.partial, data...)
	var out bytes.Buffer
	for {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		out.WriteString(s.prefix)
		out.Write(buf[:i+1])
		buf = buf[i+1:]
	}
	s.partial = append([]byte(nil), buf...)
	if flush && len(s.partial) > 0 {
		out.WriteString(s.prefix)
		out.Write(s.partial)
		out.WriteByte('\n')
		s.partial = nil
	}
	return out.Bytes()
}

// lastLines keeps the last n lines of text, all of it when n is 0.
func lastLines(text []byte, n int) []byte {
	if n <= 0 {
		return text
	}
	lines := strings.SplitAfter(string(text), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return []byte(strings.Join(lines, ""))
}

// logsEnded reports whether a followed log has nothing more to come: the
// workload is gone, GET /jobs saw its job end, or every member's container
// wrote its exit status.
func (d *Executor) logsEnded(uid string, logs []memberLog) bool {
	d.mu.Lock()
	rec := d.records[uid]
	ended := rec == nil || rec.Terminal != ""
	d.mu.Unlock()
	if ended {
		return true
	}
	for _, l := range logs {
		_, workDir := d.memberDirs(l.pod)
		if _, err := os.Stat(workDir + "/" + l.kind() + l.container + ".status"); err != nil {
			return false
		}
	}
	return true
}
