package slurm

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
)

const testGroup = "wl-uid"

// podSetsRequest is a group of PodSets of the given sizes, its pods numbered
// across PodSets and labelled with the group, as the executor builds them.
func podSetsRequest(sizes ...int) PodSetsCreateRequest {
	req := PodSetsCreateRequest{GroupUID: testGroup}
	rank := 0
	for p, size := range sizes {
		ps := PodSetData{Name: "ps" + strconv.Itoa(p)}
		for i := 0; i < size; i++ {
			uid := rankUID(rank)
			ps.Pods = append(ps.Pods, commonIL.RetrievedPodData{Pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod-" + strconv.Itoa(rank), Namespace: testNamespace, UID: types.UID(uid),
					Labels: map[string]string{PodGroupNameLabel: testGroup},
				},
				Spec: v1.PodSpec{Containers: []v1.Container{{Name: "main", Image: busyboxImage}}},
			}})
			rank++
		}
		req.PodSets = append(req.PodSets, ps)
	}
	return req
}

// newPodSetsHandler is newHandlerWithStubs with a runtime, and an sbatch that
// counts its calls in the returned file.
func newPodSetsHandler(t *testing.T, sbatchBody string) (*SidecarHandler, string, string, string) {
	t.Helper()
	h, dataRoot, scancelCount := newHandlerWithStubs(t, "42")
	h.Config.ContainerRuntime = singularityRuntime
	h.Config.SingularityPath = singularityRuntime
	sbatchCount := filepath.Join(filepath.Dir(h.Config.Sbatchpath), "sbatch.count")
	h.Config.Sbatchpath = writeStubScript(t, filepath.Dir(h.Config.Sbatchpath), "sbatch-counted",
		"echo x >> "+sbatchCount+"\n"+sbatchBody)
	return h, dataRoot, scancelCount, sbatchCount
}

func postPodSets(t *testing.T, h *SidecarHandler, req PodSetsCreateRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.CreatePodSetsHandler(rec, httptest.NewRequest(http.MethodPost, "/createPodSets", bytes.NewReader(body)))
	return rec
}

// rankUID is the UID podSetsRequest gives to a rank.
func rankUID(rank int) string { return testGroup + "-" + strconv.Itoa(rank) }

// One call submits the whole group as one job, ranks in the order of the
// request; a retried call gets the same job without a second sbatch; the
// deletes of the members cancel the job once.
func TestCreatePodSetsSubmitsOneJob(t *testing.T) {
	h, dataRoot, scancelCount, sbatchCount := newPodSetsHandler(t, "echo 'Submitted batch job 42'")
	req := podSetsRequest(1, 2)

	for attempt := 0; attempt < 2; attempt++ {
		rec := postPodSets(t, h, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("attempt %d: code %d, body %q", attempt, rec.Code, rec.Body.String())
		}
		var created []CreateStruct
		if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
			t.Fatal(err)
		}
		if len(created) != 3 {
			t.Fatalf("attempt %d: %d answers, want 3", attempt, len(created))
		}
		for i, c := range created {
			if want := rankUID(i); c.PodUID != want || c.PodJID != "42" {
				t.Fatalf("attempt %d: answer %d = %+v, want %s with job 42", attempt, i, c, want)
			}
		}
	}
	if n := countLines(t, sbatchCount); n != 1 {
		t.Fatalf("sbatch ran %d times, want 1", n)
	}

	raw, err := os.ReadFile(memberDir(dataRoot, rankUID(0)) + "/job.slurm")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"#SBATCH --nodes=3\n",
		"#SBATCH --job-name=" + testGroup + "\n",
		"# rank 0 (head) pod " + testGroup + "-0\n",
		"# rank 1 (worker) pod " + testGroup + "-1\n",
		"# rank 2 (worker) pod " + testGroup + "-2\n",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("job.slurm lacks %q\n---\n%s", want, script)
		}
	}
	for i := 0; i < 3; i++ {
		uid := rankUID(i)
		if j := (*h.JIDs)[uid]; j == nil || j.JID != "42" || !j.Gang {
			t.Fatalf("JIDs[%s] = %+v, want gang job 42", uid, j)
		}
	}
	if e := h.GangTable[testGroup]; e == nil || !e.Submitted || e.JID != "42" || len(e.Members) != 3 {
		t.Fatalf("gang entry %+v, want submitted job 42 with 3 members", e)
	}

	for i := 0; i < 3; i++ {
		if err := simulateDelete(t, h, rankUID(i), memberDir(dataRoot, rankUID(i))); err != nil {
			t.Fatalf("delete rank %d: %v", i, err)
		}
	}
	if n := scancelCalls(t, scancelCount); n != 1 {
		t.Fatalf("scancel ran %d times, want 1", n)
	}
	if _, ok := h.GangTable[testGroup]; ok {
		t.Fatal("the gang entry outlived its members")
	}
}

func TestCreatePodSetsRefusals(t *testing.T) {
	otherGroup := podSetsRequest(2)
	otherGroup.PodSets[0].Pods[1].Pod.Labels[PodGroupNameLabel] = "other"
	sameUID := podSetsRequest(2)
	sameUID.PodSets[0].Pods[1].Pod.UID = sameUID.PodSets[0].Pods[0].Pod.UID
	noGroup := podSetsRequest(2)
	noGroup.GroupUID = ""

	tests := []struct {
		name    string
		req     PodSetsCreateRequest
		gangOff bool
		buffer  bool
		want    int
	}{
		{name: "gang scheduling off", req: podSetsRequest(2), gangOff: true, want: http.StatusBadRequest},
		{name: "no group UID", req: noGroup, want: http.StatusBadRequest},
		{name: "no pods", req: PodSetsCreateRequest{GroupUID: testGroup}, want: http.StatusBadRequest},
		{name: "pod of another group", req: otherGroup, want: http.StatusBadRequest},
		{name: "two pods with one UID", req: sameUID, want: http.StatusBadRequest},
		{name: "group buffered by /create", req: podSetsRequest(2), buffer: true, want: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, _, sbatchCount := newPodSetsHandler(t, "echo 'Submitted batch job 42'")
			h.Config.GangSchedulingEnabled = !tt.gangOff
			if tt.buffer {
				if _, ok, err := bufferPod(t, h, newGangPod("uid-a", testGroup, "head", 2, nil), 2); err != nil || ok {
					t.Fatalf("buffer: ok=%v err=%v", ok, err)
				}
			}
			if rec := postPodSets(t, h, tt.req); rec.Code != tt.want {
				t.Fatalf("code %d, want %d; body %q", rec.Code, tt.want, rec.Body.String())
			}
			if n := countLines(t, sbatchCount); n != 0 {
				t.Fatalf("sbatch ran %d times", n)
			}
		})
	}
}

// A refused sbatch answers as /create does and leaves nothing behind, so a
// later call starts afresh.
func TestCreatePodSetsSbatchRefused(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   int
	}{
		{name: "invalid account", stderr: "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified", want: http.StatusBadRequest},
		{name: "job limit", stderr: "sbatch: error: QOSMaxSubmitJobPerUserLimit", want: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, dataRoot, _, _ := newPodSetsHandler(t, "echo '"+tt.stderr+"' >&2; exit 1")
			if rec := postPodSets(t, h, podSetsRequest(2)); rec.Code != tt.want {
				t.Fatalf("code %d, want %d; body %q", rec.Code, tt.want, rec.Body.String())
			}
			for i := 0; i < 2; i++ {
				if _, err := os.Stat(memberDir(dataRoot, rankUID(i))); !os.IsNotExist(err) {
					t.Fatalf("rank %d directory left behind: %v", i, err)
				}
			}
			if len(*h.JIDs) != 0 || len(h.GangTable) != 0 {
				t.Fatalf("state left behind: JIDs %v, gangs %v", *h.JIDs, h.GangTable)
			}

			h.Config.Sbatchpath = writeStubScript(t, t.TempDir(), "sbatch", "echo 'Submitted batch job 43'")
			if rec := postPodSets(t, h, podSetsRequest(2)); rec.Code != http.StatusOK {
				t.Fatalf("retry: code %d, body %q", rec.Code, rec.Body.String())
			}
		})
	}
}

// countLines counts the lines of a stub's counter file; none when it is absent.
func countLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(b), "\n")
}
