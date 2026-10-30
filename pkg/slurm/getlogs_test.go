package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// followEndTimeout is how long a followed log that must end may take.
const followEndTimeout = 5 * time.Second

// streamRecorder records a streamed answer that the test reads while the handler
// still writes. wrote is closed by the first write of some bytes.
type streamRecorder struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	header http.Header
	wrote  chan struct{}
	once   sync.Once
}

func (s *streamRecorder) Header() http.Header { return s.header }

func (s *streamRecorder) WriteHeader(int) {}

func (s *streamRecorder) Flush() {}

func (s *streamRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(p) > 0 {
		s.once.Do(func() { close(s.wrote) })
	}
	return s.buf.Write(p)
}

func (s *streamRecorder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// followed is a /getLogs request with Follow in progress.
type followed struct {
	out    *streamRecorder
	done   chan struct{}
	cancel context.CancelFunc
}

func (f *followed) ended(within time.Duration) bool {
	select {
	case <-f.done:
		return true
	case <-time.After(within):
		return false
	}
}

func fastFollow(t *testing.T) {
	t.Helper()
	saved, savedNotice := followPollInterval, followNoticeAfter
	followPollInterval, followNoticeAfter = 10*time.Millisecond, 0
	t.Cleanup(func() { followPollInterval, followNoticeAfter = saved, savedNotice })
	resetStatusCache()
	t.Cleanup(resetStatusCache)
}

// followLogs starts /getLogs with Follow for one container and returns once the
// handler has written its first bytes: the log, or the note that it does not
// exist yet.
func followLogs(t *testing.T, h *SidecarHandler, pod *v1.Pod, container string) *followed {
	t.Helper()
	body, err := json.Marshal(commonIL.LogStruct{
		Namespace:     pod.Namespace,
		PodUID:        string(pod.UID),
		PodName:       pod.Name,
		ContainerName: container,
		Opts:          commonIL.ContainerLogOpts{Follow: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &followed{out: &streamRecorder{header: http.Header{}, wrote: make(chan struct{})}, done: make(chan struct{}), cancel: cancel}
	t.Cleanup(func() {
		cancel()
		<-f.done
	})
	req := httptest.NewRequest(http.MethodGet, "/getLogs", bytes.NewReader(body)).WithContext(ctx)
	go func() {
		defer close(f.done)
		h.GetLogsHandler(f.out, req)
	}()
	select {
	case <-f.out.wrote:
	case <-time.After(followEndTimeout):
		t.Fatal("the followed log wrote nothing")
	}
	return f
}

func reportStatus(uid string, state v1.ContainerState) {
	storeStatuses([]commonIL.PodStatus{{PodUID: uid, Containers: []v1.ContainerStatus{{Name: testContainer, State: state}}}}, time.Now())
}

// A followed log ends when the container ends, also when the job ended before the
// container could write its status file (cancelled, timed out, a killed rank), and
// when the container ends or the pod is deleted before the log exists.
func TestFollowEndsWithTheContainer(t *testing.T) {
	cancelled := v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 15, Reason: ReasonSlurmJobCancelled}}
	deletePod := func(t *testing.T, h *SidecarHandler, pod *v1.Pod) {
		if err := h.deletePod(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		log    bool // run-main.out exists
		status bool // run-main.status exists
		// during runs once the follow has written its first bytes.
		during func(t *testing.T, h *SidecarHandler, pod *v1.Pod)
	}{
		{name: "status file", log: true, status: true},
		{name: "reported terminated without a status file", log: true, during: func(_ *testing.T, _ *SidecarHandler, pod *v1.Pod) {
			reportStatus(string(pod.UID), cancelled)
		}},
		{name: "reported terminated before the log existed", during: func(_ *testing.T, _ *SidecarHandler, pod *v1.Pod) {
			reportStatus(string(pod.UID), cancelled)
		}},
		{name: "deleted before the log existed", during: deletePod},
		{name: "deleted while running", log: true, during: deletePod},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fastFollow(t)
			h, pod := followedPod(t, tt.log, tt.status)
			f := followLogs(t, h, pod, testContainer)
			if tt.during != nil {
				tt.during(t, h, pod)
			}
			if !f.ended(followEndTimeout) {
				t.Fatal("the followed log did not end")
			}
			if out := f.out.String(); tt.log && !strings.Contains(out, "hello") {
				t.Fatalf("followed log = %q, want hello", out)
			}
		})
	}

	// Negative control: the log of a running container is followed until the
	// caller goes away.
	t.Run("running", func(t *testing.T) {
		fastFollow(t)
		h, pod := followedPod(t, true, false)
		reportStatus(string(pod.UID), v1.ContainerState{Running: &v1.ContainerStateRunning{}})
		f := followLogs(t, h, pod, testContainer)
		if f.ended(200 * time.Millisecond) {
			t.Fatal("the log of a running container stopped being followed")
		}
		f.cancel()
		if !f.ended(followEndTimeout) {
			t.Fatal("the followed log did not end when the caller went away")
		}
	})
}

// A followed log sees what the job writes after the follow started even when the
// file is replaced, as a network filesystem can present appends from another host:
// the file is opened again at every read instead of read through one handle.
func TestFollowSeesAppendsThroughANewFile(t *testing.T) {
	fastFollow(t)
	h, pod := followedPod(t, true, false)
	reportStatus(string(pod.UID), v1.ContainerState{Running: &v1.ContainerStateRunning{}})
	f := followLogs(t, h, pod, testContainer)
	<-f.out.wrote
	dir := memberDir(h.Config.DataRootFolder, string(pod.UID))
	tmp := dir + "/run-main.out.new"
	if err := os.WriteFile(tmp, []byte("hello\nworld\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, dir+"/run-main.out"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	reportStatus(string(pod.UID), v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}})
	if !f.ended(followEndTimeout) {
		t.Fatal("the followed log did not end")
	}
	if out := f.out.String(); out != "hello\nworld\n" {
		t.Fatalf("followed log = %q", out)
	}
}

// A log that appears after a short wait is followed without the plugin's notice
// in it: the notice is only for waits that would time out the virtual kubelet.
func TestFollowShortWaitLeavesNoNotice(t *testing.T) {
	fastFollow(t)
	followNoticeAfter = time.Hour
	h, pod := followedPod(t, false, false)
	reportStatus(string(pod.UID), v1.ContainerState{Running: &v1.ContainerStateRunning{}})
	dir := memberDir(h.Config.DataRootFolder, string(pod.UID))
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.WriteFile(dir+"/run-main.out", []byte("hello\n"), 0o600)
	}()
	f := followLogs(t, h, pod, testContainer)
	reportStatus(string(pod.UID), v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}})
	if !f.ended(followEndTimeout) {
		t.Fatal("the followed log did not end")
	}
	if out := f.out.String(); out != "hello\n" {
		t.Fatalf("followed log = %q", out)
	}
}

// What the job writes to job.out after the follow started (a launcher's exit line)
// is followed too, as whole lines that never split a line of the container.
func TestFollowCarriesJobOutLines(t *testing.T) {
	fastFollow(t)
	h, pod := followedPod(t, true, false)
	reportStatus(string(pod.UID), v1.ContainerState{Running: &v1.ContainerStateRunning{}})
	dir := memberDir(h.Config.DataRootFolder, string(pod.UID))
	appendTo := func(name, text string) {
		f, err := os.OpenFile(dir+"/"+name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteString(text); err != nil {
			t.Fatal(err)
		}
	}
	appendTo("job.out", "job started\n")
	f := followLogs(t, h, pod, testContainer)
	appendTo("run-main.out", "half")
	time.Sleep(50 * time.Millisecond)
	appendTo("job.out", "launcher exited rc=0\n")
	time.Sleep(50 * time.Millisecond)
	appendTo("run-main.out", " line\n")
	time.Sleep(50 * time.Millisecond)
	appendTo("job.out", "last")
	reportStatus(string(pod.UID), v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}})
	if !f.ended(followEndTimeout) {
		t.Fatal("the followed log did not end")
	}
	if out := f.out.String(); out != "job started\nhello\nhalf line\nlauncher exited rc=0\nlast\n" {
		t.Fatalf("followed log = %q", out)
	}
}

// followedPod is a submitted pod whose job directory holds run-main.out ("hello")
// and run-main.status as asked.
func followedPod(t *testing.T, logFile, statusFile bool) (*SidecarHandler, *v1.Pod) {
	t.Helper()
	h, dataRoot, _ := newHandlerWithStubs(t, "7")
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: testNamespace, UID: types.UID("uid-follow")}}
	dir := memberDir(dataRoot, string(pod.UID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	(*h.JIDs)[string(pod.UID)] = &JidStruct{JID: "7", PodUID: string(pod.UID), PodNamespace: pod.Namespace}
	if logFile {
		if err := os.WriteFile(dir+"/run-main.out", []byte("hello\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if statusFile {
		if err := os.WriteFile(dir+"/run-main.status", []byte("0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return h, pod
}

// Every rank of a gang has its own followed log, with its own lines, in both
// launch shapes, and each ends when the shared job ends without the ranks having
// written their status files.
func TestFollowEveryGangRank(t *testing.T) {
	line := func(rank int) string { return "line of rank " + strconv.Itoa(rank) }
	tests := []struct {
		name  string
		state string // what squeue reports once the ranks are followed
		// logs writes rank i's line where the gang script puts it.
		logs func(t *testing.T, headDir, dir string, rank int)
	}{
		{name: "one srun per rank", state: "0:0 CD None", logs: func(t *testing.T, _, dir string, rank int) {
			if err := os.WriteFile(dir+"/run-main.out", []byte(line(rank)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "site launcher", state: "0:15 CA None", logs: func(t *testing.T, headDir, dir string, rank int) {
			rankLog := headDir + "/ranks/rank-" + strconv.Itoa(rank) + ".out"
			if err := os.MkdirAll(filepath.Dir(rankLog), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(rankLog, []byte(line(rank)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(rankLog, dir+"/run-main.out"); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fastFollow(t)
			h, dataRoot, _ := newHandlerWithStubs(t, "88")
			h.Config.Squeuepath = writeStubScript(t, t.TempDir(), "squeue", "echo '"+tt.state+"'")
			pods := []v1.Pod{
				newGangMemberPod("uid-r0", "g", GangRoleHead, 2, nil),
				newGangMemberPod("uid-r1", "g", GangRoleWorker, 2, nil),
			}
			for i := range pods {
				pods[i].Spec.Containers = []v1.Container{{Name: testContainer}}
				if _, _, err := bufferPod(t, h, pods[i], 2); err != nil {
					t.Fatal(err)
				}
			}
			if (*h.JIDs)["uid-r0"] == nil || (*h.JIDs)["uid-r1"] == nil {
				t.Fatal("the gang was not submitted")
			}
			for i := range pods {
				tt.logs(t, memberDir(dataRoot, "uid-r0"), memberDir(dataRoot, string(pods[i].UID)), i)
			}

			follows := make([]*followed, len(pods))
			for i := range pods {
				follows[i] = followLogs(t, h, &pods[i], testContainer)
			}
			body, err := json.Marshal([]*v1.Pod{&pods[0], &pods[1]})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			h.StatusHandler(rec, httptest.NewRequest(http.MethodGet, "/status", bytes.NewReader(body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status code = %d, body %q", rec.Code, rec.Body.String())
			}
			for i, f := range follows {
				if !f.ended(followEndTimeout) {
					t.Fatalf("rank %d: the followed log did not end", i)
				}
				out := f.out.String()
				if !strings.Contains(out, line(i)) || strings.Contains(out, line(1-i)) {
					t.Fatalf("rank %d: followed log = %q, want only %q", i, out, line(i))
				}
			}
		})
	}
}
