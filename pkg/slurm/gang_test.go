package slurm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	trace "go.opentelemetry.io/otel/trace"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
)

// --- test helpers -----------------------------------------------------------

// newGangPod builds a minimal gang-annotated pod.
func newGangPod(uid, gangName, role string, size int, extra map[string]string) v1.Pod {
	ann := map[string]string{
		GangNameAnnotation: gangName,
		GangSizeAnnotation: strconv.Itoa(size),
		GangRoleAnnotation: role,
	}
	for k, v := range extra {
		ann[k] = v
	}
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-" + uid,
			Namespace:   "default",
			UID:         types.UID(uid),
			Annotations: ann,
		},
	}
}

// writeStubScript writes an executable shell script and returns its path.
func writeStubScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
	return p
}

// newHandlerWithStubs returns a SidecarHandler whose sbatch echoes a fixed JID
// and whose scancel appends to a counter file. Returns the handler, the
// DataRootFolder, and the scancel counter file path.
func newHandlerWithStubs(t *testing.T, jid string) (*SidecarHandler, string, string) {
	t.Helper()
	bin := t.TempDir()
	dataRoot := t.TempDir() + string(os.PathSeparator)

	sbatch := writeStubScript(t, bin, "sbatch", "echo \"Submitted batch job "+jid+"\"")
	scancelCount := filepath.Join(bin, "scancel.count")
	scancel := writeStubScript(t, bin, "scancel", "echo \"$@\" >> "+scancelCount)

	jids := make(map[string]*JidStruct)
	h := &SidecarHandler{
		Config: SlurmConfig{
			BashPath:              "/bin/bash",
			Sbatchpath:            sbatch,
			Scancelpath:           scancel,
			DataRootFolder:        dataRoot,
			GangSchedulingEnabled: true,
			GangTimeout:           "10m",
		},
		JIDs:      &jids,
		Ctx:       context.Background(),
		GangTable: make(map[string]*GangEntry),
	}
	return h, dataRoot, scancelCount
}

// scancelCalls returns how many times the stub scancel was invoked.
func scancelCalls(t *testing.T, counterFile string) int {
	t.Helper()
	b, err := os.ReadFile(counterFile)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("read scancel counter: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}
	return len(lines)
}

// memberDir returns the per-pod dir for a uid under dataRoot.
func memberDir(dataRoot, uid string) string {
	return dataRoot + "default-" + uid
}

// bufferPod is a small wrapper mirroring what Create.go does for a gang pod:
// build the BufferedMember and call bufferGangMember.
func bufferPod(t *testing.T, h *SidecarHandler, pod v1.Pod, size int) (string, bool, error) {
	t.Helper()
	filesPath := h.Config.DataRootFolder + pod.Namespace + "-" + string(pod.UID)
	if err := os.MkdirAll(filesPath, 0o755); err != nil {
		t.Fatalf("mkdir member dir: %v", err)
	}
	// A minimal job.sh so the member dir looks like a real rendered member.
	if err := os.WriteFile(filesPath+"/job.sh", []byte("#!/bin/bash\ntrue\n"), 0o755); err != nil {
		t.Fatalf("write member job.sh: %v", err)
	}
	member := gangMemberFromCreate(pod, filesPath, nil, ResourceLimits{}, true, true, nil)
	return h.bufferGangMember(h.Ctx, member, size)
}

// simulateDelete mirrors StopHandler EXACTLY: drop the member from the gang
// buffer, then take GangMu ONLY for the scancel decision (scancelDecideAndRemoveJID),
// release it, and do the filesystem cleanup unlocked (removeJobFilesWithRetry).
// This is the same narrow-lock ordering the real handler uses, so tests exercise
// the shipped delete path rather than a wide-lock stand-in.
func simulateDelete(t *testing.T, h *SidecarHandler, uid, dir string) error {
	t.Helper()
	h.removeGangMemberOnDelete(h.Ctx, uid)
	h.GangMu.Lock()
	jid, scancelErr := scancelDecideAndRemoveJID(h.Ctx, h.Config, uid, h.JIDs)
	h.GangMu.Unlock()
	if scancelErr != nil {
		return scancelErr
	}
	span := trace.SpanFromContext(h.Ctx)
	return removeJobFilesWithRetry(h.Ctx, span, uid, jid, dir)
}

// --- tests ------------------------------------------------------------------

func TestIsGangPod(t *testing.T) {
	on := SlurmConfig{GangSchedulingEnabled: true}
	off := SlurmConfig{GangSchedulingEnabled: false}
	withName := metav1.ObjectMeta{Annotations: map[string]string{GangNameAnnotation: "g1"}}
	noName := metav1.ObjectMeta{Annotations: map[string]string{}}

	if !isGangPod(on, withName) {
		t.Error("expected gang pod when enabled and annotated")
	}
	if isGangPod(off, withName) {
		t.Error("expected NON-gang when feature disabled (single-pod path must stay identical)")
	}
	if isGangPod(on, noName) {
		t.Error("expected NON-gang when annotation absent")
	}
}

// Buffering below quorum returns an empty JID and does not submit.
func TestBufferBelowQuorum(t *testing.T) {
	h, _, scancelCount := newHandlerWithStubs(t, "111")
	pod := newGangPod("a", "g1", GangRoleHead, 2, nil)

	jid, submitted, err := bufferPod(t, h, pod, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if submitted {
		t.Error("gang should not be submitted below quorum")
	}
	if jid != "" {
		t.Errorf("expected empty JID below quorum, got %q", jid)
	}
	if _, ok := (*h.JIDs)["a"]; ok {
		t.Error("no JID should be registered for a buffered-only member")
	}
	if scancelCalls(t, scancelCount) != 0 {
		t.Error("no scancel should occur during buffering")
	}
}

// Reaching quorum submits exactly one sbatch and back-fills all members' JIDs.
func TestQuorumSubmitsOnceAndBackfills(t *testing.T) {
	h, _, _ := newHandlerWithStubs(t, "999")
	head := newGangPod("h", "g1", GangRoleHead, 2, nil)
	worker := newGangPod("w", "g1", GangRoleWorker, 2, nil)

	if _, submitted, err := bufferPod(t, h, head, 2); err != nil || submitted {
		t.Fatalf("head buffer: submitted=%v err=%v (want false,nil)", submitted, err)
	}
	jid, submitted, err := bufferPod(t, h, worker, 2)
	if err != nil {
		t.Fatalf("worker buffer error: %v", err)
	}
	if !submitted {
		t.Fatal("gang should be submitted at quorum")
	}
	if jid != "999" {
		t.Errorf("expected shared JID 999, got %q", jid)
	}
	// Both members must be registered with the shared JID.
	for _, uid := range []string{"h", "w"} {
		e, ok := (*h.JIDs)[uid]
		if !ok {
			t.Errorf("member %s not registered in JIDs after submit", uid)
			continue
		}
		if e.JID != "999" {
			t.Errorf("member %s JID = %q, want 999", uid, e.JID)
		}
	}
}

// cancel during aggregation: buffer 1 of 2, delete it -> buffer cleaned, NO
// scancel, no panic, and the empty GangEntry is dropped.
func TestCancelDuringAggregation(t *testing.T) {
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "111")
	pod := newGangPod("a", "g1", GangRoleHead, 2, nil)

	if _, submitted, err := bufferPod(t, h, pod, 2); err != nil || submitted {
		t.Fatalf("buffer: submitted=%v err=%v", submitted, err)
	}

	dir := memberDir(dataRoot, "a")
	if err := simulateDelete(t, h, "a", dir); err != nil {
		t.Fatalf("delete of buffered member errored: %v", err)
	}

	if scancelCalls(t, scancelCount) != 0 {
		t.Error("scancel must NOT be called for a buffered (unsubmitted) member")
	}
	if _, ok := h.GangTable["g1"]; ok {
		t.Error("empty GangEntry should be dropped after its only member is deleted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Error("member dir should have been removed")
	}
}

// cancel during execution: 2 members share a JID -> 2 deletes -> exactly ONE
// scancel; both dirs removed.
func TestCancelDuringExecutionRefcountsScancel(t *testing.T) {
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "555")
	head := newGangPod("h", "g1", GangRoleHead, 2, nil)
	worker := newGangPod("w", "g1", GangRoleWorker, 2, nil)

	if _, _, err := bufferPod(t, h, head, 2); err != nil {
		t.Fatalf("head buffer: %v", err)
	}
	if _, submitted, err := bufferPod(t, h, worker, 2); err != nil || !submitted {
		t.Fatalf("worker buffer: submitted=%v err=%v", submitted, err)
	}

	deleteMember := func(uid string) {
		if err := simulateDelete(t, h, uid, memberDir(dataRoot, uid)); err != nil {
			t.Fatalf("delete(%s): %v", uid, err)
		}
	}

	// First delete: sibling still references the JID -> NO scancel yet.
	deleteMember("h")
	if got := scancelCalls(t, scancelCount); got != 0 {
		t.Errorf("after first delete scancel calls = %d, want 0 (sibling still live)", got)
	}
	// Second (last) delete: now scancel exactly once.
	deleteMember("w")
	if got := scancelCalls(t, scancelCount); got != 1 {
		t.Errorf("after last delete scancel calls = %d, want exactly 1", got)
	}

	for _, uid := range []string{"h", "w"} {
		if _, ok := (*h.JIDs)[uid]; ok {
			t.Errorf("JID entry for %s should be removed after delete", uid)
		}
		if _, err := os.Stat(memberDir(dataRoot, uid)); !os.IsNotExist(err) {
			t.Errorf("member dir for %s should be removed", uid)
		}
	}
}

// countJIDReferences underpins the refcount decision.
func TestCountJIDReferences(t *testing.T) {
	jids := map[string]*JidStruct{
		"a": {PodUID: "a", JID: "10"},
		"b": {PodUID: "b", JID: "10"},
		"c": {PodUID: "c", JID: "20"},
	}
	if n := countJIDReferences(&jids, "10"); n != 2 {
		t.Errorf("countJIDReferences(10) = %d, want 2", n)
	}
	if n := countJIDReferences(&jids, "20"); n != 1 {
		t.Errorf("countJIDReferences(20) = %d, want 1", n)
	}
	if n := countJIDReferences(&jids, ""); n != 0 {
		t.Errorf("countJIDReferences(\"\") = %d, want 0", n)
	}
}

// firstContainerState returns the first container's state from a PodStatus in
// the Status response, keyed by pod UID.
func firstContainerState(t *testing.T, resp []commonIL.PodStatus, uid string) v1.ContainerState {
	t.Helper()
	for _, ps := range resp {
		if ps.PodUID == uid {
			if len(ps.Containers) == 0 {
				t.Fatalf("pod %s has no container statuses", uid)
			}
			return ps.Containers[0].State
		}
	}
	t.Fatalf("pod %s not found in status response", uid)
	return v1.ContainerState{}
}

// TestGangRunningPerRankStatusIsolated drives StatusHandler END TO END and proves
// the item-#1 behaviour: while the shared SLURM job is R (Running), a gang member
// whose own run-<ctn>.status shows a non-zero exit reports Terminated(Failed) on
// its OWN pod, while a sibling with no status file reports Running. It also
// asserts NO scancel is triggered by the rank failure.
func TestGangRunningPerRankStatusIsolated(t *testing.T) {
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "777")

	// squeue stub: report the shared job as Running ("0 R") for both the --me and
	// the per-job -j queries StatusHandler makes.
	bin := filepath.Dir(h.Config.Sbatchpath)
	h.Config.Squeuepath = writeStubScript(t, bin, "squeue", `echo "0                 R"`)

	head := newGangPod("h", "g1", GangRoleHead, 2, nil)
	worker := newGangPod("w", "g1", GangRoleWorker, 2, nil)
	if _, _, err := bufferPod(t, h, head, 2); err != nil {
		t.Fatalf("head buffer: %v", err)
	}
	if _, submitted, err := bufferPod(t, h, worker, 2); err != nil || !submitted {
		t.Fatalf("worker buffer: submitted=%v err=%v", submitted, err)
	}

	// The failing rank (worker) has written its own non-zero run-main.status; the
	// healthy rank (head) has NOT written any status file yet (still running).
	if err := os.WriteFile(memberDir(dataRoot, "w")+"/run-main.status", []byte("1\n"), 0o644); err != nil {
		t.Fatalf("write worker status: %v", err)
	}

	// Build the pods with a container named "main" (Status iterates pod.Spec.Containers).
	withCtn := func(p v1.Pod) *v1.Pod {
		p.Spec.Containers = []v1.Container{{Name: "main"}}
		return &p
	}
	reqPods := []*v1.Pod{withCtn(head), withCtn(worker)}
	body, _ := json.Marshal(reqPods)

	// Reset the 10s status cache window so the handler recomputes.
	timer = time.Time{}

	req := httptest.NewRequest(http.MethodPost, "/status", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	h.StatusHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("StatusHandler status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp []commonIL.PodStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal status response: %v (body=%s)", err, rec.Body.String())
	}

	// Failed rank -> Terminated with exit code 1 on its own pod.
	wState := firstContainerState(t, resp, "w")
	if wState.Terminated == nil {
		t.Fatalf("worker (failed rank) should be Terminated, got %+v", wState)
	}
	if wState.Terminated.ExitCode != 1 {
		t.Errorf("worker exit code = %d, want 1", wState.Terminated.ExitCode)
	}
	// Healthy rank -> still Running (shared job is R and it has no status file).
	hState := firstContainerState(t, resp, "h")
	if hState.Running == nil {
		t.Errorf("head (healthy rank) should be Running, got %+v", hState)
	}

	// The rank failure must NOT have triggered any scancel.
	if scancelCalls(t, scancelCount) != 0 {
		t.Error("a single-rank failure must NOT trigger scancel")
	}
	// Both pods still share the same live JID.
	if (*h.JIDs)["h"].JID != (*h.JIDs)["w"].JID {
		t.Error("gang members must share one JID")
	}
}

// TestSinglePodRunningStatusUnaffected proves the gang divergence is gated: a
// NON-gang pod (Gang=false) with a stale run-<ctn>.status still reports Running
// under R, exactly as before the gang change (single-pod path byte-identical).
func TestSinglePodRunningStatusUnaffected(t *testing.T) {
	h, dataRoot, _ := newHandlerWithStubs(t, "42")
	bin := filepath.Dir(h.Config.Sbatchpath)
	h.Config.Squeuepath = writeStubScript(t, bin, "squeue", `echo "0                 R"`)

	// A plain single-pod job: register it in JIDs WITHOUT the gang flag and give
	// it a dir with a (would-be-misleading) non-zero status file present.
	uid := "solo"
	dir := memberDir(dataRoot, uid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(dir+"/run-main.status", []byte("1\n"), 0o644); err != nil {
		t.Fatalf("write status: %v", err)
	}
	(*h.JIDs)[uid] = &JidStruct{PodUID: uid, PodNamespace: "default", JID: "42"} // Gang=false

	pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "solo", Namespace: "default", UID: types.UID(uid)}}
	pod.Spec.Containers = []v1.Container{{Name: "main"}}
	body, _ := json.Marshal([]*v1.Pod{&pod})

	timer = time.Time{}
	req := httptest.NewRequest(http.MethodPost, "/status", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	h.StatusHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("StatusHandler status = %d", rec.Code)
	}
	var resp []commonIL.PodStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	state := firstContainerState(t, resp, uid)
	if state.Running == nil {
		t.Errorf("single-pod job under R must report Running (gang divergence must NOT apply), got %+v", state)
	}
}

// late arrival after partial delete: a Create for a gang whose entry was cleaned
// must not panic; it re-buffers into a fresh entry (which would later time out if
// quorum is never met).
func TestLateArrivalAfterCleanup(t *testing.T) {
	h, dataRoot, _ := newHandlerWithStubs(t, "111")
	pod := newGangPod("a", "g1", GangRoleHead, 2, nil)
	if _, submitted, err := bufferPod(t, h, pod, 2); err != nil || submitted {
		t.Fatalf("buffer: %v", err)
	}
	// Delete the only member -> entry dropped.
	_ = simulateDelete(t, h, "a", memberDir(dataRoot, "a"))
	if _, ok := h.GangTable["g1"]; ok {
		t.Fatal("entry should be gone before late arrival")
	}

	// A late sibling arrives for the same gang: must re-create the entry, no panic.
	late := newGangPod("b", "g1", GangRoleWorker, 2, nil)
	jid, submitted, err := bufferPod(t, h, late, 2)
	if err != nil {
		t.Fatalf("late arrival errored: %v", err)
	}
	if submitted || jid != "" {
		t.Errorf("late lone arrival should buffer, not submit (submitted=%v jid=%q)", submitted, jid)
	}
	if e, ok := h.GangTable["g1"]; !ok || len(e.Members) != 1 {
		t.Errorf("expected fresh entry with 1 member, got %+v", h.GangTable["g1"])
	}
}

// idempotent re-Create for an already-buffered UID does not double-count.
func TestIdempotentReCreateDoesNotDoubleCount(t *testing.T) {
	h, _, _ := newHandlerWithStubs(t, "111")
	pod := newGangPod("a", "g1", GangRoleHead, 2, nil)

	// Same UID buffered twice; quorum is 2 but only ONE distinct member exists.
	if _, submitted, err := bufferPod(t, h, pod, 2); err != nil || submitted {
		t.Fatalf("first buffer: submitted=%v err=%v", submitted, err)
	}
	if _, submitted, err := bufferPod(t, h, pod, 2); err != nil || submitted {
		t.Fatalf("re-buffer same UID must not submit: submitted=%v err=%v", submitted, err)
	}
	e := h.GangTable["g1"]
	if e == nil || len(e.Members) != 1 {
		t.Fatalf("re-Create of same UID double-counted: members=%d", len(e.Members))
	}
	if e.Submitted {
		t.Error("gang must not be submitted from a single distinct member")
	}
}

// restart recovery: after back-fill wrote JobID.jid/PodUID.uid/PodNamespace.ns
// into every member dir, LoadJIDs rehydrates all N with the shared JID.
func TestRestartRecoveryLoadsAllGangMembers(t *testing.T) {
	h, dataRoot, _ := newHandlerWithStubs(t, "888")
	head := newGangPod("h", "g1", GangRoleHead, 2, nil)
	worker := newGangPod("w", "g1", GangRoleWorker, 2, nil)
	if _, _, err := bufferPod(t, h, head, 2); err != nil {
		t.Fatalf("head buffer: %v", err)
	}
	if _, submitted, err := bufferPod(t, h, worker, 2); err != nil || !submitted {
		t.Fatalf("worker buffer: submitted=%v err=%v", submitted, err)
	}

	// Fresh handler over the SAME DataRootFolder, empty JIDs (simulating restart).
	fresh := make(map[string]*JidStruct)
	h2 := &SidecarHandler{
		Config: SlurmConfig{DataRootFolder: dataRoot},
		JIDs:   &fresh,
		Ctx:    context.Background(),
	}
	if err := h2.LoadJIDs(); err != nil {
		t.Fatalf("LoadJIDs: %v", err)
	}
	for _, uid := range []string{"h", "w"} {
		e, ok := (*h2.JIDs)[uid]
		if !ok {
			t.Errorf("LoadJIDs did not rehydrate member %s", uid)
			continue
		}
		if e.JID != "888" {
			t.Errorf("rehydrated JID for %s = %q, want 888", uid, e.JID)
		}
	}
}

// TestGangGuaranteeTimeout locks the runtime timeout predicate that config
// validation now mirrors (item #6): empty/unparseable/<=0 all fall back to 10m,
// and "0s" specifically is treated as invalid (not honoured), consistent with
// NewSlurmConfig rejecting it when gang scheduling is enabled.
func TestGangGuaranteeTimeout(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 10 * time.Minute},
		{"5m", 5 * time.Minute},
		{"30s", 30 * time.Second},
		{"0s", 10 * time.Minute},      // non-positive -> fallback (and rejected at config time)
		{"-1m", 10 * time.Minute},     // negative -> fallback
		{"garbage", 10 * time.Minute}, // unparseable -> fallback
	}
	for _, c := range cases {
		got := gangGuaranteeTimeout(SlurmConfig{GangTimeout: c.in})
		if got != c.want {
			t.Errorf("gangGuaranteeTimeout(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

// assignRanks pins head to rank 0 and honors explicit ranks.
func TestAssignRanksHeadIsZero(t *testing.T) {
	members := map[string]*BufferedMember{
		"w1": {PodUID: "w1", Role: GangRoleWorker, Pod: newGangPod("w1", "g", GangRoleWorker, 3, map[string]string{GangRankAnnotation: "2"})},
		"h":  {PodUID: "h", Role: GangRoleHead, Pod: newGangPod("h", "g", GangRoleHead, 3, nil)},
		"w2": {PodUID: "w2", Role: GangRoleWorker, Pod: newGangPod("w2", "g", GangRoleWorker, 3, nil)},
	}
	ordered := assignRanks(members)
	if ordered[0].Role != GangRoleHead || ordered[0].Rank != 0 {
		t.Errorf("rank 0 must be head, got role=%s rank=%d", ordered[0].Role, ordered[0].Rank)
	}
	// Explicit rank 2 must be honored.
	if members["w1"].Rank != 2 {
		t.Errorf("explicit gang-rank=2 not honored, got %d", members["w1"].Rank)
	}
	// All ranks distinct and in 0..2.
	seen := map[int]bool{}
	for _, m := range ordered {
		if m.Rank < 0 || m.Rank >= len(ordered) || seen[m.Rank] {
			t.Errorf("invalid/duplicate rank %d", m.Rank)
		}
		seen[m.Rank] = true
	}
}

// produceGangSLURMScript renders the co-scheduled shape with per-rank isolation.
func TestProduceGangSLURMScriptShape(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	// A non-empty CommandPrefix mirrors a real deployment (e.g. BSC's
	// `module load singularity`); the gang script MUST emit it ONCE at the
	// batch level (see the regression assertions below).
	config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity"}

	mk := func(uid, role string) *BufferedMember {
		dir := dataRoot + "default-" + uid
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		return gangMemberFromCreate(newGangPod(uid, "g1", role, 2, nil), dir, nil, ResourceLimits{}, true, true, nil)
	}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mk("h", GangRoleHead),
		"w": mk("w", GangRoleWorker),
	}}
	ordered := assignRanks(entry.Members)

	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read gang script: %v", err)
	}
	s := string(b)
	for _, want := range []string{
		"#SBATCH --job-name=g1",
		"#SBATCH --nodes=2",
		"#SBATCH --ntasks-per-node=1",
		"scontrol show hostnames",
		"srun --overlap --nodes=1 --ntasks=1",
		"export MASTER_ADDR",
		"WORLD_SIZE=2",
		"DRIVER_RC",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("gang script missing %q\n---\n%s", want, s)
		}
	}
	// No default --mem=1 in the gang path (BSC rejects an explicit --mem).
	if strings.Contains(s, "#SBATCH --mem=1\n") {
		t.Error("gang script must not emit the default --mem=1")
	}
	// Per-rank isolation: each member's own dir referenced for -o.
	if !strings.Contains(s, "default-h/job.out") || !strings.Contains(s, "default-w/job.out") {
		t.Error("each rank must route -o into its own member dir")
	}

	// REGRESSION: the site CommandPrefix (e.g. `module load singularity`) MUST run
	// ONCE at the BATCH level, not inside each per-rank `srun bash -c`. A per-rank
	// srun is a fresh sub-shell where the `module` shell-function is often undefined
	// (interLink submits via a non-login `ssh <host> sbatch`), so a per-rank
	// `module load singularity` no-ops, `singularity` is off PATH, and job.sh dies
	// with "singularity: command not found" (exit 127). Emitting it once at the
	// batch level runs `module load` where `module` is defined; the resulting PATH
	// then propagates to every rank via `srun --export=ALL`.
	if got := strings.Count(s, "module load singularity"); got != 1 {
		t.Errorf("CommandPrefix must run once at the batch level: got %d occurrences of 'module load singularity', want 1\n---\n%s", got, s)
	}
	// It must appear BEFORE the first per-rank srun (i.e. at the batch level). If it
	// came after, it would be inside a rank's bash -c (the 127 regression).
	if cp, sr := strings.Index(s, "module load singularity"), strings.Index(s, "srun --overlap"); cp < 0 || sr < 0 || cp > sr {
		t.Errorf("CommandPrefix must be emitted at the batch level (before the first per-rank srun): module@%d srun@%d\n---\n%s", cp, sr, s)
	}
	// The per-rank inner must be its own compute-node write followed by
	// `exec <job.sh>`, with NO CommandPrefix inlined (batch-level only).
	if !strings.Contains(s, "bash -c 'hostname -f > ") {
		t.Error("per-rank inner must start with its own compute-node write")
	}
	if c := strings.Count(s, "exec "); c < 2 {
		t.Errorf("each rank inner must exec its member job.sh (got %d exec occurrences)", c)
	}
	// REGRESSION: coordination env goes through `srun --export` (so $head_ip is
	// resolved in the batch shell), NOT re-exported inside the rank's single-quoted
	// `bash -c`, where $head_ip is empty and would clobber MASTER_ADDR/RAY_ADDRESS.
	for _, want := range []string{
		"--export=ALL,RANK=0",
		"--export=ALL,RANK=1",
		"MASTER_ADDR=\"$head_ip\"",
		"RAY_ADDRESS=\"$head_ip:6379\"",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("gang script missing coordination export %q\n---\n%s", want, s)
		}
	}
	if strings.Contains(s, "bash -c 'export RANK=") {
		t.Error("rank env must not be re-exported inside the single-quoted bash -c (empty $head_ip clobbers MASTER_ADDR/RAY_ADDRESS)")
	}
	// The readiness barrier must probe the head coordinator PORT (generic: Ray GCS /
	// torch rendezvous), not a bare `ray` binary that only exists inside the SIF.
	if !strings.Contains(s, "/dev/tcp/$head_ip/$MASTER_PORT") {
		t.Error("readiness barrier must probe the head MASTER_PORT, not a bare `ray` command")
	}
}

// TestConcurrentGangBufferAndDelete stresses the narrowed GangMu (item #3) under
// the race detector: many independent 2-member gangs are buffered+submitted and
// then deleted concurrently. Each gang has a distinct JID, so exactly one scancel
// per gang is expected. This exercises the concurrent Create/Delete paths that in
// production are driven by separate net/http goroutines, verifying the lock
// ordering (narrow scancel decision, unlocked cleanup) has no data race and the
// refcount decision stays correct under contention.
func TestConcurrentGangBufferAndDelete(t *testing.T) {
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "0") // jid overridden per-gang below

	const gangs = 25
	// Per-gang UNIQUE sbatch JID with no shared-state race in the stub: use a
	// nanosecond timestamp as the job id. Each sbatch invocation gets a distinct
	// value (SLURMBatchSubmit calls are not simultaneous down to the ns), so every
	// gang ends up with its own JID and the refcount/scancel accounting is exact.
	bin := filepath.Dir(h.Config.Sbatchpath)
	h.Config.Sbatchpath = writeStubScript(t, bin, "sbatch",
		"echo \"Submitted batch job $(date +%s%N)\"")

	buffer := func(uid, gang, role string) error {
		filesPath := h.Config.DataRootFolder + "default-" + uid
		if err := os.MkdirAll(filesPath, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filesPath+"/job.sh", []byte("#!/bin/bash\ntrue\n"), 0o755); err != nil {
			return err
		}
		member := gangMemberFromCreate(newGangPod(uid, gang, role, 2, nil), filesPath, nil, ResourceLimits{}, true, true, nil)
		_, _, err := h.bufferGangMember(h.Ctx, member, 2)
		return err
	}

	errCh := make(chan error, gangs*4)
	done := make(chan struct{}, gangs)
	for g := 0; g < gangs; g++ {
		go func(g int) {
			gang := "g" + strconv.Itoa(g)
			hUID := gang + "-h"
			wUID := gang + "-w"
			if err := buffer(hUID, gang, GangRoleHead); err != nil {
				errCh <- err
			}
			if err := buffer(wUID, gang, GangRoleWorker); err != nil {
				errCh <- err
			}
			// Delete both members concurrently within the gang.
			if err := simulateDeleteConcurrent(h, hUID, memberDir(dataRoot, hUID)); err != nil {
				errCh <- err
			}
			if err := simulateDeleteConcurrent(h, wUID, memberDir(dataRoot, wUID)); err != nil {
				errCh <- err
			}
			done <- struct{}{}
		}(g)
	}
	for i := 0; i < gangs; i++ {
		<-done
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent gang op errored: %v", err)
		}
	}

	// Exactly one scancel per gang (each gang has a distinct JID; the last of its
	// two deletes issues the single scancel).
	if got := scancelCalls(t, scancelCount); got != gangs {
		t.Errorf("scancel calls = %d, want %d (one per gang)", got, gangs)
	}
	// All JIDs cleared and GangTable drained.
	if len(*h.JIDs) != 0 {
		t.Errorf("JIDs not fully cleared: %d remaining", len(*h.JIDs))
	}
	if len(h.GangTable) != 0 {
		t.Errorf("GangTable not drained: %d entries remaining", len(h.GangTable))
	}
}

// simulateDeleteConcurrent is the goroutine-safe twin of simulateDelete (no
// *testing.T so it can run off the test goroutine). It mirrors StopHandler's
// narrow-lock ordering exactly.
func simulateDeleteConcurrent(h *SidecarHandler, uid, dir string) error {
	h.removeGangMemberOnDelete(h.Ctx, uid)
	h.GangMu.Lock()
	jid, scancelErr := scancelDecideAndRemoveJID(h.Ctx, h.Config, uid, h.JIDs)
	h.GangMu.Unlock()
	if scancelErr != nil {
		return scancelErr
	}
	span := trace.SpanFromContext(h.Ctx)
	return removeJobFilesWithRetry(h.Ctx, span, uid, jid, dir)
}

// --- stale submitted-gang regression tests -----------------------------------
//
// Field failure this guards against (BSC, job 42973171): a KubeRay RayCluster
// gang submitted as one sbatch, the job FAILED and aged out of the SLURM
// controller (squeue -j -> "Invalid job id specified"), then the user deleted
// and recreated the RayCluster. The new pods carried the SAME gang-name but NEW
// UIDs; bufferGangMember found the old GangEntry still Submitted and handed the
// DEAD JID to every new pod, pinning them Pending forever. A submitted entry
// whose JID is gone from SLURM must be dropped so the new generation gets a
// fresh sbatch.

// slurmJobGone classifies squeue output; table locks the liveness contract.
func TestSlurmJobGoneClassifier(t *testing.T) {
	// rc matters: go-execute v1 returns err=nil for ANY nonzero exit (the error
	// lives only in ExecResult.ExitCode), so a signal-killed ssh shim (exit
	// 143/255, empty stdout AND stderr) must NOT be classified gone - that
	// would drop a live gang and double-submit (found by adversarial review,
	// reproduced against go-execute v0.6.0).
	cases := []struct {
		name   string
		stdout string
		stderr string
		rc     int
		gone   bool
	}{
		{"invalid job id (aged out of controller)", "", "slurm_load_jobs error: Invalid job id specified\n", 1, true},
		{"running", "R\n", "", 0, false},
		{"pending", "PD\n", "", 0, false},
		{"completing", "CG\n", "", 0, false},
		{"configuring", "CF\n", "", 0, false},
		{"suspended", "S\n", "", 0, false},
		{"failed", "F\n", "", 0, true},
		{"completed", "CD\n", "", 0, true},
		{"cancelled", "CA\n", "", 0, true},
		{"timeout", "TO\n", "", 0, true},
		{"node fail", "NF\n", "", 0, true},
		{"out of memory", "OOM\n", "", 0, true},
		{"empty output with CLEAN exit means unknown job", "", "", 0, true},
		{"signal-killed ssh (rc 143, silent) is NOT gone", "", "", 143, false},
		{"ssh remote death (rc 255, silent) is NOT gone", "", "", 255, false},
		{"nonzero exit distrusts stdout too (partial pipe output)", "F\n", "", 137, false},
		{"transient ssh shim failure is NOT gone", "", "ssh: connect to host alogin1.bsc.es port 22: Connection refused\n", 255, false},
		{"unrecognized state treated alive (never double-submit)", "XX\n", "", 0, false},
		{"padded squeue -O output", "R               \n", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := slurmJobGone(c.stdout, c.stderr, c.rc); got != c.gone {
				t.Errorf("slurmJobGone(%q, %q, %d) = %v, want %v", c.stdout, c.stderr, c.rc, got, c.gone)
			}
		})
	}
}

// THE regression test: new-UID members of a same-named gang whose submitted job
// is dead must start a FRESH generation (fresh sbatch), not inherit the dead JID.
func TestStaleSubmittedGangStartsFreshGeneration(t *testing.T) {
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "111")
	bin := filepath.Dir(h.Config.Sbatchpath)

	// Generation 1 submits as job 111.
	if _, submitted, err := bufferPod(t, h, newGangPod("a1", "g1", GangRoleHead, 2, nil), 2); err != nil || submitted {
		t.Fatalf("gen1 head buffer: submitted=%v err=%v", submitted, err)
	}
	if jid, submitted, err := bufferPod(t, h, newGangPod("b1", "g1", GangRoleWorker, 2, nil), 2); err != nil || !submitted || jid != "111" {
		t.Fatalf("gen1 quorum: jid=%q submitted=%v err=%v", jid, submitted, err)
	}

	// Job 111 dies and ages out: squeue rejects the job id (verbatim BSC output).
	h.Config.Squeuepath = writeStubScript(t, bin, "squeue",
		`echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1`)
	// Any fresh submission must be observable: counting sbatch, new JID 222.
	sbatchCalls := filepath.Join(bin, "sbatch.count")
	h.Config.Sbatchpath = writeStubScript(t, bin, "sbatch",
		"echo x >> "+sbatchCalls+"\necho \"Submitted batch job 222\"")

	// Generation 2: same gang-name, NEW pod UIDs (operator deleted + recreated).
	jid, submitted, err := bufferPod(t, h, newGangPod("a2", "g1", GangRoleHead, 2, nil), 2)
	if err != nil {
		t.Fatalf("gen2 first member errored: %v", err)
	}
	if submitted || jid != "" {
		t.Fatalf("gen2 first member must be BUFFERED for a fresh generation, not bound to the dead JID: jid=%q submitted=%v", jid, submitted)
	}
	if e := h.GangTable["g1"]; e == nil || e.Submitted || len(e.Members) != 1 {
		t.Fatalf("stale entry not replaced by a fresh 1-member buffer: %+v", h.GangTable["g1"])
	}

	jid2, submitted2, err := bufferPod(t, h, newGangPod("b2", "g1", GangRoleWorker, 2, nil), 2)
	if err != nil {
		t.Fatalf("gen2 quorum errored: %v", err)
	}
	if !submitted2 || jid2 != "222" {
		t.Fatalf("gen2 quorum must submit FRESH sbatch: jid=%q submitted=%v (want 222,true)", jid2, submitted2)
	}
	for _, uid := range []string{"a2", "b2"} {
		e, ok := (*h.JIDs)[uid]
		if !ok || e.JID != "222" {
			t.Errorf("gen2 member %s JID = %v, want 222", uid, e)
		}
		b, err := os.ReadFile(memberDir(dataRoot, uid) + "/JobID.jid")
		if err != nil || string(b) != "222" {
			t.Errorf("gen2 member %s JobID.jid on disk = %q err=%v, want 222", uid, string(b), err)
		}
	}
	if b, err := os.ReadFile(sbatchCalls); err != nil || len(strings.Split(strings.TrimSpace(string(b)), "\n")) != 1 {
		t.Errorf("expected exactly one fresh sbatch for gen2, got %q err=%v", string(b), err)
	}
	if scancelCalls(t, scancelCount) != 0 {
		t.Error("dropping a stale (already dead) gang entry must NOT scancel")
	}
}

// A NEW pod UID arriving while the submitted gang's job is still ALIVE is a
// replacement pod (e.g. the operator recreated one member): bind it to the live
// shared JID, do not resubmit.
func TestNewUIDBindsToLiveSubmittedGang(t *testing.T) {
	h, _, _ := newHandlerWithStubs(t, "111")
	bin := filepath.Dir(h.Config.Sbatchpath)

	if _, _, err := bufferPod(t, h, newGangPod("h", "g1", GangRoleHead, 2, nil), 2); err != nil {
		t.Fatalf("head buffer: %v", err)
	}
	if _, submitted, err := bufferPod(t, h, newGangPod("w", "g1", GangRoleWorker, 2, nil), 2); err != nil || !submitted {
		t.Fatalf("quorum: submitted=%v err=%v", submitted, err)
	}

	// The job is alive; any further sbatch would be a bug.
	h.Config.Squeuepath = writeStubScript(t, bin, "squeue", `echo "R"`)
	sbatchCalls := filepath.Join(bin, "sbatch.count")
	h.Config.Sbatchpath = writeStubScript(t, bin, "sbatch",
		"echo x >> "+sbatchCalls+"\necho \"Submitted batch job 333\"")

	jid, submitted, err := bufferPod(t, h, newGangPod("r", "g1", GangRoleWorker, 2, nil), 2)
	if err != nil {
		t.Fatalf("replacement member errored: %v", err)
	}
	if !submitted || jid != "111" {
		t.Errorf("replacement pod must bind to the LIVE shared JID: jid=%q submitted=%v (want 111,true)", jid, submitted)
	}
	if e, ok := (*h.JIDs)["r"]; !ok || e.JID != "111" {
		t.Errorf("replacement pod not registered on shared JID: %v", e)
	}
	if _, err := os.Stat(sbatchCalls); !os.IsNotExist(err) {
		t.Error("no sbatch may be issued while the gang job is alive")
	}
}

// A re-Create of a UID that WAS part of the submitted gang stays idempotent and
// returns the shared JID WITHOUT consulting squeue: even if the job is gone, the
// pod belongs to the old generation and must keep its JID (Status handles the
// terminal reporting). Only never-seen UIDs may trigger the staleness probe.
func TestSubmittedGangSameUIDIdempotentEvenIfJobGone(t *testing.T) {
	h, _, _ := newHandlerWithStubs(t, "111")
	bin := filepath.Dir(h.Config.Sbatchpath)

	head := newGangPod("h", "g1", GangRoleHead, 2, nil)
	if _, _, err := bufferPod(t, h, head, 2); err != nil {
		t.Fatalf("head buffer: %v", err)
	}
	if _, submitted, err := bufferPod(t, h, newGangPod("w", "g1", GangRoleWorker, 2, nil), 2); err != nil || !submitted {
		t.Fatalf("quorum: submitted=%v err=%v", submitted, err)
	}

	// squeue would report the job gone - must be irrelevant for a KNOWN member.
	h.Config.Squeuepath = writeStubScript(t, bin, "squeue",
		`echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1`)

	jid, submitted, err := bufferPod(t, h, head, 2)
	if err != nil {
		t.Fatalf("same-UID re-Create errored: %v", err)
	}
	if !submitted || jid != "111" {
		t.Errorf("same-UID re-Create must return the shared JID: jid=%q submitted=%v (want 111,true)", jid, submitted)
	}
	if e := h.GangTable["g1"]; e == nil || !e.Submitted || len(e.Members) != 2 {
		t.Errorf("submitted entry must be untouched by a same-UID re-Create: %+v", h.GangTable["g1"])
	}
}

// Native per-rank compute-node discovery: EACH rank records ITS OWN node into
// ITS OWN member dir (inside the rank's srun, so `hostname -f` runs on the
// rank's node, not the batch node). This is what a worker's exposed-ports
// shadow tunnel needs - in a --nodes=N gang the worker runs on a DIFFERENT
// node than the head. Replaces the CommandPrefix scontrol-StdOut hack, which
// resolved the job-level output dir (the head dir) for every rank and raced.
func TestGangScriptWritesPerRankComputeNode(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity"}

	mk := func(uid, role string) *BufferedMember {
		dir := dataRoot + "default-" + uid
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		return gangMemberFromCreate(newGangPod(uid, "g1", role, 2, nil), dir, nil, ResourceLimits{}, true, true, nil)
	}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mk("h", GangRoleHead),
		"w": mk("w", GangRoleWorker),
	}}
	ordered := assignRanks(entry.Members)

	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read gang script: %v", err)
	}
	s := string(b)

	// Quoting note: the rank inner goes through nested shellescape (the path is
	// quoted inside the inner, the inner is quoted into bash -c), so assert the
	// SEMANTIC parts - the member-specific target path and the hostname write -
	// rather than exact quote bytes.
	firstSrun := strings.Index(s, "srun --overlap")
	if firstSrun < 0 {
		t.Fatalf("gang script has no per-rank srun\n---\n%s", s)
	}
	for _, uid := range []string{"h", "w"} {
		want := "default-" + uid + "/compute-node"
		idx := strings.Index(s, want)
		if idx < 0 {
			t.Errorf("gang script must write rank compute-node into member %s's OWN dir (%q)\n---\n%s", uid, want, s)
			continue
		}
		// The write must be INSIDE the rank's srun bash -c (i.e. AFTER the first
		// srun of the script), not at the batch level: at batch level `hostname`
		// reports the batch host (nodes[0]) for every member, which is wrong for
		// workers.
		if idx < firstSrun {
			t.Errorf("member %s compute-node write must run inside its rank srun, not at batch level\n---\n%s", uid, s)
		}
	}
	if got := strings.Count(s[firstSrun:], "hostname -f > "); got != 2 {
		t.Errorf("expected one per-rank hostname write per member (2), got %d\n---\n%s", got, s)
	}
}

// Deterministic head discovery for shadow/tunnel pods: the gang batch script
// (which runs on nodes[0] = the head's node) must publish the head node under
// a well-known gang-keyed path <DataRootFolder>/.gang-heads/<ns>-<gangname>.
// The VK's wstunnel template exposes only name/namespace/labels/annotations
// (NO pod UID, verified against interLink 0.6.1), so a shadow pod cannot
// resolve the head member's per-UID dir - but it CAN read its own
// interlink.eu/gang-name annotation and fetch this file. Replaces the
// newest-compute-node-file race for gang heads.
func TestGangScriptPublishesGangHeadDiscoveryFile(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	config := SlurmConfig{BashPath: "/bin/bash", DataRootFolder: dataRoot, Commandprefix: "module load singularity"}

	mk := func(uid, role string) *BufferedMember {
		dir := dataRoot + "default-" + uid
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		return gangMemberFromCreate(newGangPod(uid, "g1", role, 2, nil), dir, nil, ResourceLimits{}, true, true, nil)
	}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mk("h", GangRoleHead),
		"w": mk("w", GangRoleWorker),
	}}
	ordered := assignRanks(entry.Members)

	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read gang script: %v", err)
	}
	s := string(b)

	headFile := dataRoot + ".gang-heads/default-g1"
	idx := strings.Index(s, headFile)
	if idx < 0 {
		t.Fatalf("gang script must publish the head node to %q\n---\n%s", headFile, s)
	}
	// Must run at the BATCH level (before any srun): the batch script executes on
	// nodes[0], which is rank 0's (the head's) node, so `hostname -f` there IS the
	// head node. Inside a rank srun it could be any member's node.
	if firstSrun := strings.Index(s, "srun --overlap"); firstSrun >= 0 && idx > firstSrun {
		t.Errorf("gang-head discovery write must be at the batch level, before the per-rank sruns\n---\n%s", s)
	}
	if !strings.Contains(s, "mkdir -p ") {
		t.Error("the .gang-heads dir must be created before writing into it")
	}
}

// --- MPI-mode + mpi-env hook + mpi-flags tests --------------------------------

// mkGangMemberWithRC builds a gang member carrying ONE fully-rendered
// ContainerCommand (like Create.go's runtime_command_pod[0]): its runtimeCommand
// already includes the SIF path at the end (mirroring prepareRuntimeCommand +
// image), plus an explicit containerCommand/containerArgs so the MPI-mode
// reconstruction has something to rebuild (exactly as runCtn does).
func mkGangMemberWithRC(t *testing.T, dataRoot, uid, role string, size int, extra map[string]string, cmd, args []string) *BufferedMember {
	t.Helper()
	dir := dataRoot + "default-" + uid
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	rc := []ContainerCommand{{
		containerName: "main",
		// runtimeCommand mirrors Create.go: singularity exec <opts> <mounts> <SIF>.
		// The SIF path is the LAST element (rc.runtimeCommand already includes it).
		runtimeCommand:   []string{"singularity", "exec", "--nv", "--no-home", "", "", "/gpfs/images/itwinai.sif"},
		containerCommand: cmd,
		containerArgs:    args,
		containerImage:   "/gpfs/images/itwinai.sif",
	}}
	return gangMemberFromCreate(newGangPod(uid, "g1", role, size, extra), dir, rc, ResourceLimits{}, true, true, nil)
}

// REGRESSION (critical): with gang-mode ABSENT, produceGangSLURMScript output is
// UNCHANGED from the per-rank behavior. Covers a KubeRay-style gang (head+worker,
// Ray env) AND a torch-style gang. Every per-rank srun --overlap loop, the
// readiness barrier, and the DRIVER_RC tail must all still be present, and NO MPI
// launcher (`srun --mpi=`) must appear.
func TestProduceGangSLURMScriptDefaultModeUnchanged(t *testing.T) {
	run := func(t *testing.T, extra map[string]string) string {
		dataRoot := t.TempDir() + string(os.PathSeparator)
		config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity"}
		entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
			"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, extra, []string{"python"}, []string{"train.py"}),
			"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, extra, []string{"python"}, []string{"train.py"}),
		}}
		ordered := assignRanks(entry.Members)
		path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
		if err != nil {
			t.Fatalf("produceGangSLURMScript: %v", err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read gang script: %v", err)
		}
		return string(b)
	}

	// KubeRay-style: head+worker, Ray env is present in the coordination block.
	t.Run("kuberay", func(t *testing.T) {
		s := run(t, nil)
		// Per-rank loop for EACH member: 2 member sruns + 1 barrier probe srun = 3.
		if got := strings.Count(s, "srun --overlap --nodes=1 --ntasks=1"); got != 3 {
			t.Errorf("default mode must emit one per-rank srun per member plus the barrier probe (3), got %d\n---\n%s", got, s)
		}
		// Both member dirs must be routed by their own per-rank srun.
		if !strings.Contains(s, "default-h/job.out") || !strings.Contains(s, "default-w/job.out") {
			t.Errorf("default mode must route each member's -o into its own dir\n---\n%s", s)
		}
		// Readiness barrier still present.
		if !strings.Contains(s, "/dev/tcp/$head_ip/$MASTER_PORT") {
			t.Errorf("default mode must keep the readiness barrier\n---\n%s", s)
		}
		// DRIVER_RC tail still present.
		for _, want := range []string{"DRIVER_RC=0", "wait \"${RANK_PIDS[0]}\"", "exit \"$DRIVER_RC\""} {
			if !strings.Contains(s, want) {
				t.Errorf("default mode must keep the DRIVER_RC tail (%q)\n---\n%s", want, s)
			}
		}
		// Ray coordination env present (RAY_ADDRESS is emitted for all gangs today).
		if !strings.Contains(s, "RAY_ADDRESS=\"$head_ip:6379\"") {
			t.Errorf("default mode must keep the RAY_ADDRESS coordination env\n---\n%s", s)
		}
		// No MPI launcher whatsoever.
		if strings.Contains(s, "srun --mpi=") {
			t.Errorf("default mode must NOT emit an MPI launcher\n---\n%s", s)
		}
		if strings.Contains(s, "SRUN_CPUS_PER_TASK") {
			t.Errorf("default mode must NOT emit the MPI SRUN_CPUS_PER_TASK export\n---\n%s", s)
		}
	})

	// torch-style: same shape, no Ray specifics needed; the per-rank + barrier +
	// DRIVER_RC contract is identical (torch consumes MASTER_ADDR/RANK/WORLD_SIZE).
	t.Run("torch", func(t *testing.T) {
		s := run(t, map[string]string{"slurm-job.vk.io/singularity-options": "--nv"})
		// 2 member sruns + 1 barrier probe srun = 3.
		if got := strings.Count(s, "srun --overlap --nodes=1 --ntasks=1"); got != 3 {
			t.Errorf("torch default mode must emit one per-rank srun per member plus the barrier probe (3), got %d\n---\n%s", got, s)
		}
		if !strings.Contains(s, "/dev/tcp/$head_ip/$MASTER_PORT") {
			t.Errorf("torch default mode must keep the readiness barrier\n---\n%s", s)
		}
		if !strings.Contains(s, "export MASTER_ADDR") || !strings.Contains(s, "WORLD_SIZE=2") {
			t.Errorf("torch default mode must keep MASTER_ADDR/WORLD_SIZE coordination env\n---\n%s", s)
		}
		if strings.Contains(s, "srun --mpi=") {
			t.Errorf("torch default mode must NOT emit an MPI launcher\n---\n%s", s)
		}
	})
}

// MPI mode: with gang-mode=mpi, exactly ONE srun launcher spanning the whole
// allocation, NO per-rank overlap loop, NO readiness barrier, the
// SRUN_CPUS_PER_TASK export, the --mpi= flag, and the singularity exec
// reconstruction (runtimeCommand join + shellescaped command/args) present.
func TestProduceGangSLURMScriptMPIMode(t *testing.T) {
	dataRoot := t.TempDir() + string(os.PathSeparator)
	config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity"}
	mpiExtra := map[string]string{GangModeAnnotation: GangModeMPI}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, mpiExtra, []string{"python"}, []string{"mpi_hello.py"}),
		"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, mpiExtra, []string{"python"}, []string{"mpi_hello.py"}),
	}}
	ordered := assignRanks(entry.Members)
	path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read gang script: %v", err)
	}
	s := string(b)

	// Exactly one MPI launcher srun; zero per-rank overlap sruns.
	if got := strings.Count(s, "srun --mpi="); got != 1 {
		t.Errorf("MPI mode must emit exactly ONE MPI launcher srun, got %d\n---\n%s", got, s)
	}
	if strings.Contains(s, "srun --overlap --nodes=1 --ntasks=1") {
		t.Errorf("MPI mode must NOT emit the per-rank overlap loop\n---\n%s", s)
	}
	// No readiness barrier in MPI mode (PMIx does the rendezvous).
	if strings.Contains(s, "/dev/tcp/$head_ip/$MASTER_PORT") {
		t.Errorf("MPI mode must NOT emit the readiness barrier\n---\n%s", s)
	}
	// No DRIVER_RC / RANK_PIDS tail: the single srun rc is the whole-job rc.
	if strings.Contains(s, "DRIVER_RC") || strings.Contains(s, "RANK_PIDS") {
		t.Errorf("MPI mode must NOT emit the DRIVER_RC/RANK_PIDS per-rank tail\n---\n%s", s)
	}
	// The MN5 SRUN_CPUS_PER_TASK export must be present.
	if !strings.Contains(s, `export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK}"`) {
		t.Errorf("MPI mode must export SRUN_CPUS_PER_TASK from SLURM_CPUS_PER_TASK\n---\n%s", s)
	}
	// The --mpi flag defaults to pmix (overridable via SLURM_MPI_TYPE).
	if !strings.Contains(s, `--mpi="${SLURM_MPI_TYPE:-pmix}"`) {
		t.Errorf("MPI mode must emit --mpi with the pmix default\n---\n%s", s)
	}
	// One task per node: --nodes/--ntasks span the whole allocation, one per node.
	for _, want := range []string{`--nodes="$WORLD_SIZE"`, `--ntasks="$WORLD_SIZE"`, "--ntasks-per-node=1"} {
		if !strings.Contains(s, want) {
			t.Errorf("MPI mode launcher missing %q\n---\n%s", want, s)
		}
	}
	// Output routed to the HEAD member's dir.
	if !strings.Contains(s, "-o "+dataRoot+"default-h/job.out") || !strings.Contains(s, "-e "+dataRoot+"default-h/job.err") {
		t.Errorf("MPI launcher must route -o/-e to the head member dir\n---\n%s", s)
	}
	// The container invocation reconstruction: runtimeCommand joined + the SIF +
	// the shellescaped command/args (exactly as runCtn does).
	if !strings.Contains(s, "singularity exec --nv --no-home") {
		t.Errorf("MPI launcher must reconstruct the singularity exec prefix\n---\n%s", s)
	}
	if !strings.Contains(s, "/gpfs/images/itwinai.sif") {
		t.Errorf("MPI launcher must include the SIF path from runtimeCommand\n---\n%s", s)
	}
	if !strings.Contains(s, "python mpi_hello.py") {
		t.Errorf("MPI launcher must include the shellescaped container command/args\n---\n%s", s)
	}
	// Only ONE srun total (the launcher). scontrol hostnames uses srun for head_ip;
	// count MPI launcher separately above. Non-head members render no srun of
	// their own -> no second --mpi launcher (asserted above by count==1).

	// The CommandPrefix must still run once at the batch level in MPI mode too.
	if got := strings.Count(s, "module load singularity"); got != 1 {
		t.Errorf("MPI mode must still run CommandPrefix once at the batch level, got %d\n---\n%s", got, s)
	}
}

// mpi-env hook: SINGULARITYENV_<K> exports appear for a given annotation, in BOTH
// modes; and are ABSENT when the annotation is unset.
func TestGangScriptMPIEnvHook(t *testing.T) {
	renderWith := func(t *testing.T, extra map[string]string) string {
		dataRoot := t.TempDir() + string(os.PathSeparator)
		config := SlurmConfig{BashPath: "/bin/bash", Commandprefix: "module load singularity"}
		entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
			"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, extra, []string{"python"}, []string{"t.py"}),
			"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, extra, []string{"python"}, []string{"t.py"}),
		}}
		ordered := assignRanks(entry.Members)
		path, err := produceGangSLURMScript(context.Background(), config, entry, ordered)
		if err != nil {
			t.Fatalf("produceGangSLURMScript: %v", err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read gang script: %v", err)
		}
		return string(b)
	}

	// Present in DEFAULT (per-rank) mode.
	t.Run("default-mode", func(t *testing.T) {
		s := renderWith(t, map[string]string{"slurm-job.vk.io/mpi-env": "NCCL_DEBUG=INFO;UCX_TLS=rc,ud"})
		for _, want := range []string{
			`export SINGULARITYENV_NCCL_DEBUG=INFO`,
			`export SINGULARITYENV_UCX_TLS=`, // value shellescaped; prefix is enough
		} {
			if !strings.Contains(s, want) {
				t.Errorf("default mode must emit mpi-env export %q\n---\n%s", want, s)
			}
		}
		// The exports must precede the launch section (per-rank srun).
		if e, sr := strings.Index(s, "SINGULARITYENV_NCCL_DEBUG"), strings.Index(s, "srun --overlap"); e < 0 || sr < 0 || e > sr {
			t.Errorf("mpi-env exports must precede the launch section: env@%d srun@%d\n---\n%s", e, sr, s)
		}
	})

	// Present in MPI mode too.
	t.Run("mpi-mode", func(t *testing.T) {
		s := renderWith(t, map[string]string{
			GangModeAnnotation:        GangModeMPI,
			"slurm-job.vk.io/mpi-env": "UCX_NET_DEVICES=mlx5_0:1",
		})
		if !strings.Contains(s, `export SINGULARITYENV_UCX_NET_DEVICES=mlx5_0:1`) {
			t.Errorf("MPI mode must emit mpi-env export\n---\n%s", s)
		}
		if e, sr := strings.Index(s, "SINGULARITYENV_UCX_NET_DEVICES"), strings.Index(s, "srun --mpi="); e < 0 || sr < 0 || e > sr {
			t.Errorf("mpi-env exports must precede the MPI launcher: env@%d srun@%d\n---\n%s", e, sr, s)
		}
	})

	// Absent when the annotation is unset.
	t.Run("unset", func(t *testing.T) {
		s := renderWith(t, nil)
		if strings.Contains(s, "SINGULARITYENV_") {
			t.Errorf("no mpi-env annotation must emit NO SINGULARITYENV_ exports\n---\n%s", s)
		}
	})

	// Malformed/empty pairs are skipped, valid ones still emitted.
	t.Run("skips-malformed", func(t *testing.T) {
		s := renderWith(t, map[string]string{"slurm-job.vk.io/mpi-env": "NCCL_DEBUG=INFO;;NOEQUALS;=novalue;GOOD=1"})
		if !strings.Contains(s, `export SINGULARITYENV_NCCL_DEBUG=INFO`) || !strings.Contains(s, `export SINGULARITYENV_GOOD=1`) {
			t.Errorf("valid pairs must still be emitted alongside malformed ones\n---\n%s", s)
		}
		if strings.Contains(s, "SINGULARITYENV_NOEQUALS") || strings.Contains(s, "SINGULARITYENV_=") {
			t.Errorf("malformed pairs (no '=', empty key) must be skipped\n---\n%s", s)
		}
	})

	// Keys that are not valid shell env-var names are dropped BEFORE the unquoted
	// `export SINGULARITYENV_<K>` interpolation (hardening the key side; the value
	// is already shellescaped). A hostile key must not reach the export line.
	t.Run("rejects-unsafe-key", func(t *testing.T) {
		s := renderWith(t, map[string]string{"slurm-job.vk.io/mpi-env": "GOOD_1=ok;BAD-KEY=x;E vil=y;X;rm -rf /=z"})
		if !strings.Contains(s, `export SINGULARITYENV_GOOD_1=ok`) {
			t.Errorf("valid key GOOD_1 must still be emitted\n---\n%s", s)
		}
		for _, bad := range []string{"BAD-KEY", "rm -rf", "E vil", "SINGULARITYENV_X="} {
			if strings.Contains(s, bad) {
				t.Errorf("unsafe key fragment %q must not reach the script\n---\n%s", bad, s)
			}
		}
	})
}

// mpi-flags bug fix: mpiexec is now ACTUALLY prepended to each container's
// runtimeCommand (previously ranged over a value copy and was discarded).
func TestBuildSbatchFlagsMPIFlagsPrependsMpiexec(t *testing.T) {
	commands := []ContainerCommand{
		{containerName: "a", runtimeCommand: []string{"singularity", "exec", "img.sif"}},
		{containerName: "b", runtimeCommand: []string{"singularity", "run", "other.sif"}},
	}
	meta := metav1.ObjectMeta{Annotations: map[string]string{
		"slurm-job.vk.io/mpi-flags": "--bind-to core",
	}}
	pod := v1.Pod{ObjectMeta: meta}

	_ = buildSbatchFlags(context.Background(), SlurmConfig{}, pod, meta, commands,
		ResourceLimits{}, true, true, nil, true)

	for i, cc := range commands {
		if len(cc.runtimeCommand) < 3 {
			t.Fatalf("command %d runtimeCommand too short: %v", i, cc.runtimeCommand)
		}
		if cc.runtimeCommand[0] != "mpiexec" || cc.runtimeCommand[1] != "-np" || cc.runtimeCommand[2] != "$SLURM_NTASKS" {
			t.Errorf("command %d: mpiexec not prepended (dead-code bug regressed): %v", i, cc.runtimeCommand)
		}
		// The user-supplied flags follow, then the original runtime command.
		joined := strings.Join(cc.runtimeCommand, " ")
		if !strings.Contains(joined, "--bind-to core") {
			t.Errorf("command %d: mpi-flags args not included: %q", i, joined)
		}
		if !strings.Contains(joined, "singularity") {
			t.Errorf("command %d: original runtime command lost: %q", i, joined)
		}
	}
	// mpiexec must NOT leak into the returned #SBATCH flags.
	// (buildSbatchFlags return value is scheduler flags only.)
}
