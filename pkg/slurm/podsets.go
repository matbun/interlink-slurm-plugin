package slurm

// PodSet-level create.
//
// /createPodSets takes the pods of one group in one call and submits them as one
// `sbatch --nodes=N` job, the job /create builds for a gang (gang.go), without
// buffering the members until the last one arrives.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/containerd/containerd/log"
	"k8s.io/apimachinery/pkg/types"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// CapabilityPodSetCreate is listed in the ping answer when the plugin takes the
// pods of a group in one /createPodSets call.
const CapabilityPodSetCreate = "podset-create"

// PodSetsCreateRequest is the body of /createPodSets: the pods of one group, by
// PodSet, each as the interLink API server prepares a pod for /create. Ranks
// follow the order of the pods, PodSet after PodSet; rank 0 is the head.
type PodSetsCreateRequest struct {
	// GroupUID names the group, and the job: the Workload UID.
	GroupUID string       `json:"groupUID"`
	PodSets  []PodSetData `json:"podSets"`
}

// PodSetData is one PodSet of a group.
type PodSetData struct {
	Name string                      `json:"name"`
	Pods []commonIL.RetrievedPodData `json:"pods"`
}

// CreatePodSetsHandler submits the pods of a group as one job. It answers one
// CreateStruct per pod, in the order of the request, all with the group's job
// ID, and fails with the status codes of /create.
func (h *SidecarHandler) CreatePodSetsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "CreatePodSets", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	log.G(h.Ctx).Info("Slurm Sidecar: received CreatePodSets call")
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		h.handleError(spanCtx, w, http.StatusInternalServerError, err)
		return
	}
	var req PodSetsCreateRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		h.handleError(spanCtx, w, http.StatusBadRequest, err)
		return
	}

	created, cerr := h.createPodSets(spanCtx, req)
	if cerr != nil {
		code := cerr.code
		if code == 0 {
			code = http.StatusInternalServerError
		}
		h.handleError(spanCtx, w, code, cerr.err)
		return
	}
	out, err := json.Marshal(created)
	if err != nil {
		h.handleError(spanCtx, w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(http.StatusOK))
	w.Write(out)
}

// createPodSets renders every pod of the group as /create does and submits them
// as one gang job (submitGang). The whole call holds h.GangMu, so that a retried
// call never renders into the directories of one in flight. A retried call whose
// pods already share a job gets that job again.
func (h *SidecarHandler) createPodSets(ctx context.Context, req PodSetsCreateRequest) ([]CreateStruct, *createError) {
	span := trace.SpanFromContext(ctx)
	refuse := func(code int, format string, args ...any) ([]CreateStruct, *createError) {
		return nil, &createError{code: code, err: fmt.Errorf(format, args...)}
	}
	if !h.Config.GangSchedulingEnabled {
		return refuse(http.StatusBadRequest, "this plugin does not run pod groups: GangSchedulingEnabled is off")
	}
	name := strings.TrimSpace(req.GroupUID)
	if name == "" {
		return refuse(http.StatusBadRequest, "groupUID is empty")
	}
	var pods []commonIL.RetrievedPodData
	seen := map[types.UID]bool{}
	for _, ps := range req.PodSets {
		for _, data := range ps.Pods {
			if data.Pod.UID == "" || seen[data.Pod.UID] {
				return refuse(http.StatusBadRequest, "PodSet %s: pod %s has no UID, or the UID of another pod", ps.Name, data.Pod.Name)
			}
			if group := gangNameFromMeta(data.Pod.ObjectMeta); group != "" && group != name {
				return refuse(http.StatusBadRequest, "PodSet %s: pod %s belongs to group %s, not %s", ps.Name, data.Pod.Name, group, name)
			}
			seen[data.Pod.UID] = true
			pods = append(pods, data)
		}
	}
	if len(pods) == 0 {
		return refuse(http.StatusBadRequest, "group %s has no pods", name)
	}
	span.SetAttributes(attribute.String("group.uid", name), attribute.Int("group.pods", len(pods)))

	h.GangMu.Lock()
	defer h.GangMu.Unlock()
	h.sweepGangsLocked(ctx)

	if jid := h.sharedJobLocked(pods); jid != "" {
		log.G(ctx).Infof("Group '%s': already submitted as SLURM job %s", name, jid)
		return groupCreated(pods, jid), nil
	}
	if entry, ok := h.GangTable[name]; ok {
		if !entry.Submitted {
			return refuse(http.StatusServiceUnavailable, "group %s is being created pod by pod (%d/%d)", name, len(entry.Members), entry.Size)
		}
		if !gangJobIsGone(ctx, h.Config, entry.JID) {
			return refuse(http.StatusServiceUnavailable, "group %s still runs as job %s", name, entry.JID)
		}
		delete(h.GangTable, name)
	}

	entry := &GangEntry{
		Name:      name,
		Size:      len(pods),
		Members:   make(map[string]*BufferedMember, len(pods)),
		CreatedAt: time.Now(),
	}
	ordered := make([]*BufferedMember, 0, len(pods))
	var rendered []*renderedPod
	removeRendered := func() {
		for _, p := range rendered {
			os.RemoveAll(p.filesPath)
			if p.workDir != p.filesPath {
				os.RemoveAll(p.workDir)
			}
		}
	}
	for i, data := range pods {
		p, cerr := h.renderPod(ctx, data)
		if cerr != nil {
			removeRendered()
			return nil, cerr
		}
		rendered = append(rendered, p)
		m := gangMemberFromCreate(data.Pod, p.filesPath, p.runtimeCommands, p.resourceLimits, p.isDefaultCPU, p.isDefaultRAM, p.flavor)
		m.Rank, m.Role = i, GangRoleWorker
		if i == 0 {
			m.Role = GangRoleHead
		}
		entry.Members[m.PodUID] = m
		ordered = append(ordered, m)
	}

	jid, err := h.submitGang(ctx, entry, ordered)
	if err != nil {
		span.AddEvent("Failed to submit the group SLURM Job")
		if errors.Is(err, errGangSubmitted) {
			// The job may run: the entry stays, so that the /delete of each pod
			// cancels it, or the sweeper drops it.
			h.GangTable[name] = entry
		} else {
			removeRendered()
		}
		return nil, submitFailure(err)
	}
	h.GangTable[name] = entry
	span.AddEvent("Group SLURM Job successfully submitted with ID " + jid)
	return groupCreated(pods, jid), nil
}

// sharedJobLocked returns the job every pod already has as a gang member, if they
// share one. The caller MUST hold h.GangMu.
func (h *SidecarHandler) sharedJobLocked(pods []commonIL.RetrievedPodData) string {
	if h.JIDs == nil {
		return ""
	}
	jid := ""
	for _, data := range pods {
		j, ok := (*h.JIDs)[string(data.Pod.UID)]
		if !ok || !j.Gang || j.JID == "" || (jid != "" && j.JID != jid) {
			return ""
		}
		jid = j.JID
	}
	return jid
}

// groupCreated is the answer to /createPodSets: every pod with the group's job.
func groupCreated(pods []commonIL.RetrievedPodData, jid string) []CreateStruct {
	created := make([]CreateStruct, len(pods))
	for i, data := range pods {
		created[i] = CreateStruct{PodUID: string(data.Pod.UID), PodJID: jid}
	}
	return created
}
