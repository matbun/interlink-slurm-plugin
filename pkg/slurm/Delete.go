package slurm

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/containerd/containerd/log"
	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// StopHandler runs a scancel command, updating JIDs and cached statuses
func (h *SidecarHandler) StopHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "Delete", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	// For debugging purpose, when we have many kubectl logs, we can differentiate each one.
	sessionContext := GetSessionContext(r)
	sessionContextMessage := GetSessionContextMessage(sessionContext)

	log.G(h.Ctx).Info(sessionContextMessage, "Slurm Sidecar: received Stop call")
	statusCode := http.StatusOK

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	var pod *v1.Pod
	err = json.Unmarshal(bodyBytes, &pod)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	filesPath := h.Config.DataRootFolder + pod.Namespace + "-" + string(pod.UID)

	// Drop this pod from its gang buffer (if any) before deleting the container.
	// interLink /delete is per-pod, so the N members of a gang arrive as N
	// separate deletes. This is safe at ANY lifecycle point:
	//   - buffered member (gang not yet submitted, so no JIDs entry): removed from
	//     the buffer here; deleteContainer's guarded jid read skips scancel; the
	//     GangEntry is dropped when its buffer empties.
	//   - submitted member (shared gang JID present): still removed from the entry;
	//     deleteContainer refcounts the shared JID and scancels only on the last
	//     member. A no-op for non-gang pods.
	h.removeGangMemberOnDelete(spanCtx, string(pod.UID))

	// Split the delete so GangMu is held ONLY for the fast, must-be-consistent
	// scancel decision (refcount read + scancel + removeJID), and RELEASED before
	// the slow filesystem cleanup (RemoveAll, which can sleep 5s in follow mode).
	// Holding GangMu across that sleep would stall every concurrent gang
	// Create/Delete/sweeper. Under the narrow lock exactly one delete sees the JID
	// refcount reach 1 and issues the single scancel.
	h.GangMu.Lock()
	jid, scancelErr := scancelDecideAndRemoveJID(spanCtx, h.Config, string(pod.UID), h.JIDs)
	h.GangMu.Unlock()
	if scancelErr != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, scancelErr)
		return
	}

	// Filesystem cleanup runs unlocked.
	err = removeJobFilesWithRetry(spanCtx, span, string(pod.UID), jid, filesPath)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}
	if os.Getenv("SHARED_FS") != "true" {
		err = os.RemoveAll(filesPath)
		if err != nil {
			statusCode = http.StatusInternalServerError
			h.handleError(spanCtx, w, statusCode, err)
			return
		}
	}

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(statusCode))

	w.WriteHeader(statusCode)
	if statusCode != http.StatusOK {
		w.Write([]byte("Some errors occurred deleting containers. Check Slurm Sidecar's logs"))
	} else {
		w.Write([]byte("All containers for submitted Pods have been deleted"))
	}
}
