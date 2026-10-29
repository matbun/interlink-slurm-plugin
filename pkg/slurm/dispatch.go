package slurm

// interlink-dispatch agent.
//
// With Dispatch.Enabled the plugin also answers the dispatcher, an admission
// check controller that hands it the Kueue Workloads to run: POST /submit turns a
// Workload's PodSet into member pods and runs them through the create path the
// virtual kubelet uses (one pod: the single-pod script; several: one gang job),
// GET /jobs/{workloadUID} maps the members' status to one job state, and DELETE
// /jobs/{workloadUID} cancels the job and removes what the agent made for it.
// The agent keeps one record per workload in Dispatch.StateDir.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	exec2 "github.com/alexellis/go-execute/pkg/v1"
	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
)

// DispatchAPIVersion is carried by every body of the agent API.
const DispatchAPIVersion = "dispatch.interlink.eu/v1alpha1"

// Job states of GET /jobs.
const (
	JobQueued    = "queued"
	JobRunning   = "running"
	JobCompleted = "completed"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
	JobRejected  = "rejected"
)

// maxWorkloadMembers bounds PodSet.count: a member's rank is part of its socket
// directory's name, four digits wide.
const maxWorkloadMembers = 9999

// maxSubmitBody bounds the size of a /submit request.
const maxSubmitBody = 8 << 20

// DispatchOwner is the object a Workload belongs to.
type DispatchOwner struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

// DispatchPodSet is one PodSet of a Workload.
type DispatchPodSet struct {
	Name          string             `json:"name"`
	Count         int32              `json:"count"`
	Partition     string             `json:"partition,omitempty"`
	Template      v1.PodTemplateSpec `json:"template"`
	SecretRefs    []string           `json:"secretRefs,omitempty"`
	ConfigMapRefs []string           `json:"configMapRefs,omitempty"`
}

// SubmitRequest is the body of POST /submit.
type SubmitRequest struct {
	APIVersion   string           `json:"apiVersion"`
	WorkloadUID  string           `json:"workloadUID"`
	WorkloadName string           `json:"workloadName"`
	Namespace    string           `json:"namespace"`
	Owner        DispatchOwner    `json:"owner"`
	Site         string           `json:"site"`
	Account      string           `json:"account"`
	PodSets      []DispatchPodSet `json:"podSets"`
}

// SubmitResponse answers an accepted /submit.
type SubmitResponse struct {
	APIVersion string `json:"apiVersion"`
	JobID      string `json:"jobID"`
	Accepted   bool   `json:"accepted"`
}

// DispatchError answers a refused call. Permanent means retrying cannot help.
type DispatchError struct {
	APIVersion string `json:"apiVersion"`
	Permanent  bool   `json:"permanent"`
	Message    string `json:"message"`
}

// JobStatus is the body of GET /jobs/{workloadUID}.
type JobStatus struct {
	APIVersion     string       `json:"apiVersion"`
	WorkloadUID    string       `json:"workloadUID"`
	JobID          string       `json:"jobID"`
	State          string       `json:"state"`
	Reason         string       `json:"reason,omitempty"`
	Message        string       `json:"message,omitempty"`
	ExitCode       int32        `json:"exitCode"`
	NodeName       string       `json:"nodeName,omitempty"`
	Nodes          []string     `json:"nodes,omitempty"`
	SocketDir      string       `json:"socketDir,omitempty"`
	StartTime      *metav1.Time `json:"startTime"`
	CompletionTime *metav1.Time `json:"completionTime"`
	Ranks          []RankStatus `json:"ranks"`
	Shadow         *ShadowRef   `json:"shadow,omitempty"`
}

// RankStatus is one member of the job.
type RankStatus struct {
	Rank       int                  `json:"rank"`
	PodUID     string               `json:"podUID"`
	State      string               `json:"state"`
	ExitCode   int32                `json:"exitCode"`
	Containers []v1.ContainerStatus `json:"containers"`
}

// workloadRecord is what the agent keeps about a submitted workload, in
// <StateDir>/<workloadUID>.json. It holds the request as received: secret
// values are resolved again from the cluster when needed, never stored.
type workloadRecord struct {
	Request     SubmitRequest `json:"request"`
	Members     []string      `json:"members"`
	JobID       string        `json:"jobID"`
	SubmittedAt time.Time     `json:"submittedAt"`
	// Terminal is the job's final state once GET /jobs has seen it.
	Terminal       string     `json:"terminal,omitempty"`
	Shadow         *ShadowRef `json:"shadow,omitempty"`
	ShadowTerminal bool       `json:"shadowTerminal,omitempty"`
}

// Dispatcher serves the agent API on top of a SidecarHandler.
type Dispatcher struct {
	Sidecar *SidecarHandler
	// Kube reads the workloads' Secrets and ConfigMaps, in the agent's namespace
	// only, and reviews callers' tokens.
	Kube kubernetes.Interface
	// Shadow renders the shadows. Nil: no shadows.
	Shadow ShadowManager

	mu      sync.Mutex
	records map[string]*workloadRecord
	tokens  tokenCache
}

// workloadUIDPattern keeps a workload UID usable as a file name.
var workloadUIDPattern = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z-]{0,62}$`)

// slurmNamePattern is what an account or a partition may contain: they end up
// in the job script's #SBATCH lines.
var slurmNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.,-]*$`)

// InClusterClient returns a clientset for the pod's own ServiceAccount.
func InClusterClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// NewDispatcher returns a Dispatcher with the records found in StateDir.
func NewDispatcher(sidecar *SidecarHandler, kube kubernetes.Interface, shadow ShadowManager) (*Dispatcher, error) {
	d := &Dispatcher{Sidecar: sidecar, Kube: kube, Shadow: shadow, records: map[string]*workloadRecord{}}
	if err := d.loadRecords(); err != nil {
		return nil, err
	}
	return d, nil
}

// RegisterRoutes adds the agent API to mux, /getLogs included: its workload form
// is answered here, the other passed to the SidecarHandler.
func (d *Dispatcher) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /submit", d.SubmitHandler)
	mux.HandleFunc("GET /jobs/{workloadUID}", d.JobStatusHandler)
	mux.HandleFunc("DELETE /jobs/{workloadUID}", d.JobDeleteHandler)
	mux.HandleFunc("/getLogs", d.GetLogsHandler)
	mux.HandleFunc("GET /healthz", HealthzHandler)
}

// HealthzHandler answers 200 ok.
func HealthzHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// apiError is a refused call: its HTTP status, whether it is permanent, and why.
type apiError struct {
	code      int
	permanent bool
	msg       string
}

func permanentError(format string, args ...any) *apiError {
	return &apiError{code: http.StatusBadRequest, permanent: true, msg: fmt.Sprintf(format, args...)}
}

func transientError(format string, args ...any) *apiError {
	return &apiError{code: http.StatusServiceUnavailable, msg: fmt.Sprintf(format, args...)}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func writeAPIError(w http.ResponseWriter, e *apiError) {
	writeJSON(w, e.code, DispatchError{APIVersion: DispatchAPIVersion, Permanent: e.permanent, Message: e.msg})
}

func (d *Dispatcher) config() SlurmConfig { return d.Sidecar.Config }

func (d *Dispatcher) shadowManager() ShadowManager {
	if d.Shadow == nil || !d.config().Shadow.Enabled {
		return noopShadowManager{}
	}
	return d.Shadow
}

func (d *Dispatcher) record(uid string) *workloadRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.records[uid]
}

// SubmitHandler is POST /submit.
func (d *Dispatcher) SubmitHandler(w http.ResponseWriter, r *http.Request) {
	if !d.authorize(w, r) {
		return
	}
	sessionContextMessage := GetSessionContextMessage(GetSessionContext(r))
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSubmitBody+1))
	if err != nil {
		writeAPIError(w, transientError("reading the request: %v", err))
		return
	}
	if len(body) > maxSubmitBody {
		writeAPIError(w, permanentError("request larger than %d bytes", maxSubmitBody))
		return
	}
	var req SubmitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIError(w, permanentError("invalid request: %v", err))
		return
	}
	log.G(d.Sidecar.Ctx).Info(sessionContextMessage, "dispatch: submit of workload ", req.WorkloadUID)

	jobID, created, aerr := d.submit(r.Context(), req)
	if aerr != nil {
		log.G(d.Sidecar.Ctx).Warning(sessionContextMessage, "dispatch: submit of workload ", req.WorkloadUID, " refused: ", aerr.msg)
		writeAPIError(w, aerr)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, SubmitResponse{APIVersion: DispatchAPIVersion, JobID: jobID, Accepted: true})
}

// validateSubmit refuses a request this agent will never run.
func (d *Dispatcher) validateSubmit(req SubmitRequest) *apiError {
	cfg := d.config()
	if req.APIVersion != "" && req.APIVersion != DispatchAPIVersion {
		return permanentError("apiVersion %q: this agent speaks %s", req.APIVersion, DispatchAPIVersion)
	}
	if !workloadUIDPattern.MatchString(req.WorkloadUID) {
		return permanentError("invalid workloadUID %q", req.WorkloadUID)
	}
	if req.Namespace != cfg.Dispatch.Namespace {
		return permanentError("namespace %q is not this agent's (%q)", req.Namespace, cfg.Dispatch.Namespace)
	}
	site, ok := cfg.Sites[req.Site]
	if !ok {
		return permanentError("unknown site %q", req.Site)
	}
	if !slices.Contains(site.AllowedAccounts, req.Account) || !slurmNamePattern.MatchString(req.Account) {
		return permanentError("account %q is not allowed on site %s", req.Account, req.Site)
	}
	if len(req.PodSets) != 1 {
		return permanentError("the workload has %d PodSets: this agent runs exactly one", len(req.PodSets))
	}
	ps := req.PodSets[0]
	if ps.Count < 1 || ps.Count > maxWorkloadMembers {
		return permanentError("PodSet %s: count %d is not between 1 and %d", ps.Name, ps.Count, maxWorkloadMembers)
	}
	if !slurmNamePattern.MatchString(ps.Partition) {
		return permanentError("PodSet %s: invalid partition %q", ps.Name, ps.Partition)
	}
	if len(ps.Template.Spec.Containers) == 0 {
		return permanentError("PodSet %s: the template has no containers", ps.Name)
	}
	if ps.Count > 1 && !cfg.GangSchedulingEnabled {
		return permanentError("PodSet %s: count %d needs GangSchedulingEnabled", ps.Name, ps.Count)
	}
	if ownerName(req) == "" {
		return permanentError("the workload has neither an owner name nor a name")
	}
	return nil
}

// submit runs the workload, or returns the job it already has. created is false
// for the latter.
func (d *Dispatcher) submit(ctx context.Context, req SubmitRequest) (string, bool, *apiError) {
	if aerr := d.validateSubmit(req); aerr != nil {
		return "", false, aerr
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if rec := d.records[req.WorkloadUID]; rec != nil {
		return rec.JobID, false, nil
	}

	ps := req.PodSets[0]
	reverse, aerr := d.reverseTunnel(req)
	if aerr != nil {
		return "", false, aerr
	}
	refs, aerr := d.fetchRefs(ctx, req.Namespace, ps)
	if aerr != nil {
		return "", false, aerr
	}
	if refs.tokens, aerr = d.mintTokens(ctx, req.Namespace, ps.Template.Spec); aerr != nil {
		return "", false, aerr
	}

	count := int(ps.Count)
	members := make([]*v1.Pod, 0, count)
	datas := make([]commonIL.RetrievedPodData, 0, count)
	for i := 0; i < count; i++ {
		pod := buildMember(req, i)
		if i == 0 && reverse != nil {
			addReversePreExec(pod, reverse)
		}
		if aerr := resolveEnv(pod, refs); aerr != nil {
			return "", false, aerr
		}
		data, aerr := retrievedPodData(ctx, pod, refs)
		if aerr != nil {
			return "", false, aerr
		}
		members = append(members, pod)
		datas = append(datas, data)
	}

	jobID := ""
	for i, data := range datas {
		res, cerr := d.Sidecar.createPod(ctx, data)
		if cerr != nil {
			d.removeMembers(ctx, members[:i+1])
			return "", false, classifyCreateError(cerr)
		}
		if res.PodJID != "" {
			jobID = res.PodJID
		}
	}
	if jobID == "" {
		d.removeMembers(ctx, members)
		return "", false, transientError("the job of workload %s was not submitted", req.WorkloadUID)
	}

	rec := &workloadRecord{Request: req, JobID: jobID, SubmittedAt: time.Now().UTC()}
	for _, m := range members {
		rec.Members = append(rec.Members, string(m.UID))
	}
	if err := d.saveRecord(rec); err != nil {
		if cerr := d.scancel(jobID); cerr != nil {
			log.G(d.Sidecar.Ctx).Error("dispatch: cannot cancel job ", jobID, " after failing to record it: ", cerr)
		}
		d.removeMembers(ctx, members)
		return "", false, transientError("recording workload %s: %v", req.WorkloadUID, err)
	}
	d.records[req.WorkloadUID] = rec
	log.G(d.Sidecar.Ctx).Infof("dispatch: workload %s submitted as Slurm job %s (%d member(s))", req.WorkloadUID, jobID, count)
	return jobID, true, nil
}

// sbatchLaterRe matches sbatch refusals that end once other jobs end or the
// controller answers again: limits on the number of submitted jobs, and an
// unreachable or busy controller. A refused account or an invalid script stays
// permanent.
var sbatchLaterRe = regexp.MustCompile(`(QOS|Assoc)MaxSubmitJob[A-Za-z]*|MaxSubmitJobs?Limit|Unable to contact slurm controller|Socket timed out|Slurm temporarily unable to accept job`)

// classifyCreateError tells a refusal by sbatch, which resubmitting does not
// change, from a failure to reach the site or to write the job's files.
func classifyCreateError(cerr *createError) *apiError {
	var se *SbatchError
	if errors.As(cerr.err, &se) {
		if se.ExitCode == 255 {
			return transientError("ssh to the site failed: %s", strings.TrimSpace(se.Stderr))
		}
		if m := sbatchLaterRe.FindString(se.Stderr); m != "" {
			return transientError("Slurm takes no more jobs of this user now (%s): %s", m, strings.TrimSpace(se.Stderr))
		}
		return &apiError{code: http.StatusBadRequest, permanent: true, msg: se.Stderr}
	}
	if cerr.err == nil {
		return transientError("the job could not be created")
	}
	return transientError("%v", cerr.err)
}

// memberDirs returns a member's metadata directory and its job working directory.
func (d *Dispatcher) memberDirs(pod *v1.Pod) (string, string) {
	cfg := d.config()
	uid := string(pod.UID)
	filesPath := cfg.DataRootFolder + pod.Namespace + "-" + uid
	workDir := getJobWorkDir(cfg, pod.Annotations, pod.Namespace, uid)
	if jid, ok := (*d.Sidecar.JIDs)[uid]; ok && jid.WorkDir != "" {
		workDir = jid.WorkDir
	}
	return filesPath, workDir
}

// removeMembers forgets the members and removes their directories. It cancels
// nothing.
func (d *Dispatcher) removeMembers(ctx context.Context, members []*v1.Pod) {
	cfg := d.config()
	uids := make([]string, 0, len(members))
	for _, m := range members {
		uid := string(m.UID)
		uids = append(uids, uid)
		filesPath, workDir := d.memberDirs(m)
		d.Sidecar.removeGangMemberOnDelete(ctx, uid)
		d.Sidecar.GangMu.Lock()
		removeJID(uid, d.Sidecar.JIDs)
		d.Sidecar.GangMu.Unlock()
		for _, dir := range []string{workDir, filesPath, socketDirFor(cfg.SocketDirRoot, uid)} {
			if dir == "" {
				continue
			}
			if err := os.RemoveAll(dir); err != nil {
				log.G(d.Sidecar.Ctx).Warning("dispatch: cannot remove ", dir, ": ", err)
			}
		}
	}
	forgetStatuses(uids)
}

// slurmJobState asks squeue for the job's compact state and reason. gone is set
// when Slurm no longer knows the job.
func (d *Dispatcher) slurmJobState(jid string) (state, reason string, gone bool, err error) {
	shell := exec2.ExecTask{
		Command: d.config().Squeuepath,
		Args:    []string{squeueNoHeader, "-a", "--states=all", "-O", "StateCompact,Reason:128", "-j", jid},
		Shell:   true,
	}
	ret, err := shell.Execute()
	if err != nil {
		return "", "", false, err
	}
	if stderr := strings.TrimSpace(ret.Stderr); stderr != "" {
		if strings.Contains(stderr, "Invalid job id") {
			return "", "", true, nil
		}
		return "", "", false, errors.New(stderr)
	}
	if ret.ExitCode != 0 {
		return "", "", false, fmt.Errorf("squeue exited with code %d", ret.ExitCode)
	}
	for _, line := range strings.Split(ret.Stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		state = fields[0]
		reason = strings.Join(fields[1:], " ")
		if reason == "None" {
			reason = ""
		}
		return state, reason, false, nil
	}
	return "", "", true, nil
}

// Slurm compact states by what they mean for the workload.
var (
	slurmQueuedStates = []string{"PD", "CF", "RQ", "RH", "RF", "RS", "S"}
	slurmFailedStates = []string{"F", "NF", "OOM", "TO", "BF", "DL", "PR", "SE", "RV"}
)

func slurmTerminalState(state string) bool {
	return state == "CD" || state == "CA" || slices.Contains(slurmFailedStates, state)
}

// slurmStateReasons names the terminal states.
var slurmStateReasons = map[string]string{
	"CD":  "Completed",
	"CA":  "Cancelled",
	"F":   "NonZeroExitCode",
	"NF":  "NodeFail",
	"OOM": "OutOfMemory",
	"TO":  "TimeLimit",
	"BF":  "BootFail",
	"DL":  "Deadline",
	"PR":  "Preempted",
	"SE":  "SpecialExit",
	"RV":  "Revoked",
	"CF":  "Configuring",
	"S":   "Suspended",
}

// scancel cancels the job. A job Slurm has already ended or forgotten is not an
// error.
func (d *Dispatcher) scancel(jid string) error {
	out, err := exec.Command(d.config().Scancelpath, jid).CombinedOutput() //nolint:gosec // scancel from the site config, jid is the numeric job ID
	if err == nil {
		return nil
	}
	text := string(out)
	if strings.Contains(text, "Invalid job id") || strings.Contains(text, "already completed") || strings.Contains(text, "has already finished") {
		return nil
	}
	return fmt.Errorf("scancel %s: %v: %s", jid, err, strings.TrimSpace(text))
}

// JobStatusHandler is GET /jobs/{workloadUID}.
func (d *Dispatcher) JobStatusHandler(w http.ResponseWriter, r *http.Request) {
	if !d.authorize(w, r) {
		return
	}
	uid := r.PathValue("workloadUID")
	rec := d.record(uid)
	if rec == nil {
		writeJSON(w, http.StatusNotFound, DispatchError{APIVersion: DispatchAPIVersion, Permanent: true, Message: "no job for workload " + uid})
		return
	}
	st, aerr := d.jobStatus(r.Context(), rec, GetSessionContextMessage(GetSessionContext(r)))
	if aerr != nil {
		writeAPIError(w, aerr)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// memberPods rebuilds the record's members as /submit made them, without the
// values of their references.
func memberPods(rec *workloadRecord) []*v1.Pod {
	n := int(rec.Request.PodSets[0].Count)
	pods := make([]*v1.Pod, 0, n)
	for i := 0; i < n; i++ {
		pods = append(pods, buildMember(rec.Request, i))
	}
	return pods
}

// jobStatus maps the members' status, as the plugin reports it, and Slurm's
// state of the job to one job state.
func (d *Dispatcher) jobStatus(ctx context.Context, rec *workloadRecord, sessionContextMessage string) (JobStatus, *apiError) {
	members := memberPods(rec)
	uids := make([]string, len(members))
	for i, m := range members {
		uids[i] = string(m.UID)
	}
	st := JobStatus{APIVersion: DispatchAPIVersion, WorkloadUID: rec.Request.WorkloadUID, JobID: rec.JobID}

	state, reason, gone, err := d.slurmJobState(rec.JobID)
	if err != nil {
		return st, transientError("querying Slurm job %s: %v", rec.JobID, err)
	}

	var statuses []commonIL.PodStatus
	switch {
	case gone:
		statuses = d.statusesFromFiles(members)
	case state == "CF":
		// The plugin's state pattern reads CF as F; the job has not started.
		for _, m := range members {
			statuses = append(statuses, commonIL.PodStatus{PodUID: string(m.UID)})
		}
	default:
		if slurmTerminalState(state) {
			forgetStatuses(uids)
		}
		out := d.Sidecar.podStatuses(ctx, members, sessionContextMessage)
		if out.err != nil {
			return st, transientError("%v", out.err)
		}
		statuses = out.statuses
	}
	byUID := make(map[string]commonIL.PodStatus, len(statuses))
	for _, s := range statuses {
		byUID[s.PodUID] = s
	}

	anyRunning := false
	allEnded := true
	failedRank := -1
	for i, uid := range uids {
		ps := byUID[uid]
		rank := RankStatus{Rank: i, PodUID: uid, Containers: ps.Containers}
		if rank.Containers == nil {
			rank.Containers = []v1.ContainerStatus{}
		}
		var ended bool
		rank.State, rank.ExitCode, ended = rankState(ps)
		allEnded = allEnded && ended
		anyRunning = anyRunning || rank.State == JobRunning
		if rank.State == JobFailed && failedRank < 0 {
			failedRank = i
		}
		if ps.NodeName != "" {
			st.Nodes = append(st.Nodes, ps.NodeName)
		}
		st.Ranks = append(st.Ranks, rank)
	}
	head := byUID[uids[0]]
	st.NodeName = head.NodeName
	st.SocketDir = head.SocketDir
	st.ExitCode = st.Ranks[0].ExitCode
	if jid := (*d.Sidecar.JIDs)[uids[0]]; jid != nil {
		if !jid.StartTime.IsZero() {
			st.StartTime = &metav1.Time{Time: jid.StartTime}
		}
		if !jid.EndTime.IsZero() {
			st.CompletionTime = &metav1.Time{Time: jid.EndTime}
		}
	}

	rankFailure := ""
	if failedRank >= 0 {
		rankFailure = fmt.Sprintf(": rank %d exited with code %d", failedRank, st.Ranks[failedRank].ExitCode)
		if r := terminatedReason(st.Ranks[failedRank].Containers); r != "" {
			rankFailure += " (" + r + ")"
		}
	}
	switch {
	case gone && allEnded:
		st.State, st.Reason = JobCompleted, "Completed"
		if failedRank >= 0 {
			st.State, st.Reason = JobFailed, "RankFailed"
		}
	case gone:
		st.State, st.Reason = JobCancelled, "Vanished"
		st.Message = fmt.Sprintf("Slurm job %s vanished with no exit status", rec.JobID)
	case state == "CA":
		st.State, st.Reason = JobCancelled, "Cancelled"
		st.Message = fmt.Sprintf("Slurm job %s was cancelled outside the agent", rec.JobID)
	case state == "CD":
		st.State, st.Reason = JobCompleted, "Completed"
		if failedRank >= 0 {
			st.State, st.Reason = JobFailed, "RankFailed"
		}
	case slices.Contains(slurmFailedStates, state):
		st.State, st.Reason = JobFailed, slurmStateReasons[state]
	case slices.Contains(slurmQueuedStates, state):
		st.State, st.Reason = JobQueued, reason
		if st.Reason == "" {
			st.Reason = slurmStateReasons[state]
		}
		st.NodeName, st.SocketDir, st.Nodes = "", "", nil
	case state == "R" || state == "CG" || anyRunning || st.NodeName != "":
		st.State = JobRunning
	default:
		st.State, st.Reason = JobQueued, reason
	}
	if st.Message == "" {
		st.Message = jobMessage(st, rec.JobID, rankFailure)
	}
	if isTerminalJobState(st.State) {
		now := metav1.Now()
		if st.CompletionTime == nil {
			st.CompletionTime = &now
		}
		if st.StartTime == nil {
			st.StartTime = st.CompletionTime
		}
	}

	d.syncShadow(ctx, rec, st)
	d.mu.Lock()
	st.Shadow = rec.Shadow
	d.mu.Unlock()
	return st, nil
}

func isTerminalJobState(state string) bool {
	return state == JobCompleted || state == JobFailed || state == JobCancelled
}

func jobMessage(st JobStatus, jid, rankFailure string) string {
	switch st.State {
	case JobQueued:
		if st.Reason != "" {
			return fmt.Sprintf("Slurm job %s pending (%s)", jid, st.Reason)
		}
		return fmt.Sprintf("Slurm job %s pending", jid)
	case JobRunning:
		msg := fmt.Sprintf("Slurm job %s running", jid)
		if st.NodeName != "" {
			msg += " on " + st.NodeName
		}
		if st.StartTime != nil {
			msg += " since " + st.StartTime.UTC().Format(time.RFC3339)
		}
		return msg
	case JobCompleted:
		return fmt.Sprintf("Slurm job %s completed", jid)
	case JobFailed:
		return fmt.Sprintf("Slurm job %s failed (%s)%s", jid, st.Reason, rankFailure)
	}
	return ""
}

// rankState is one member's state from its containers' status. ended is set
// when every container terminated.
func rankState(ps commonIL.PodStatus) (state string, exitCode int32, ended bool) {
	if len(ps.Containers) == 0 {
		return JobQueued, 0, false
	}
	ended = true
	running := false
	for _, c := range ps.Containers {
		switch {
		case c.State.Terminated != nil:
			if exitCode == 0 {
				exitCode = c.State.Terminated.ExitCode
			}
		case c.State.Running != nil:
			running = true
			ended = false
		default:
			ended = false
		}
	}
	switch {
	case ended && exitCode != 0:
		return JobFailed, exitCode, true
	case ended:
		return JobCompleted, 0, true
	case running || ps.NodeName != "":
		return JobRunning, 0, false
	}
	return JobQueued, 0, false
}

func terminatedReason(containers []v1.ContainerStatus) string {
	for _, c := range containers {
		if t := c.State.Terminated; t != nil && t.Reason != "" {
			return t.Reason
		}
	}
	return ""
}

// statusesFromFiles reads the members' status from the files their job left,
// for a job Slurm no longer knows. A container with no status file did not end
// on its own.
func (d *Dispatcher) statusesFromFiles(members []*v1.Pod) []commonIL.PodStatus {
	out := make([]commonIL.PodStatus, 0, len(members))
	for _, m := range members {
		_, workDir := d.memberDirs(m)
		ps := commonIL.PodStatus{PodName: m.Name, PodUID: string(m.UID), PodNamespace: m.Namespace, NodeName: readComputeNode(workDir)}
		for _, c := range m.Spec.Containers {
			cs := v1.ContainerStatus{Name: c.Name}
			if code, found, err := readMemberContainerExitCode(workDir, c.Name); err == nil && found {
				cs.State.Terminated = &v1.ContainerStateTerminated{ExitCode: code}
			}
			ps.Containers = append(ps.Containers, cs)
		}
		out = append(out, ps)
	}
	return out
}

// templateExposesPorts reports whether a container of the template declares a port.
func templateExposesPorts(t v1.PodTemplateSpec) bool {
	for _, c := range t.Spec.Containers {
		if len(c.Ports) > 0 {
			return true
		}
	}
	return false
}

// syncShadow keeps the workload's shadow in step with the job: one Ensure per
// poll while it runs on a known node, one more with Terminal set once it ended.
func (d *Dispatcher) syncShadow(ctx context.Context, rec *workloadRecord, st JobStatus) {
	cfg := d.config()
	terminal := isTerminalJobState(st.State)
	d.mu.Lock()
	current := d.records[rec.Request.WorkloadUID] == rec
	if current && terminal && rec.Terminal != st.State {
		rec.Terminal = st.State
		if err := d.saveRecord(rec); err != nil {
			log.G(d.Sidecar.Ctx).Warning("dispatch: cannot record the end of workload ", rec.Request.WorkloadUID, ": ", err)
		}
	}
	shadowDone := rec.ShadowTerminal
	d.mu.Unlock()
	if !current || !cfg.Shadow.Enabled || d.Shadow == nil {
		return
	}
	ps := rec.Request.PodSets[0]
	reverse, aerr := d.reverseTunnel(rec.Request)
	if aerr != nil {
		log.G(d.Sidecar.Ctx).Warning("dispatch: shadow of workload ", rec.Request.WorkloadUID, " without its reverse tunnel: ", aerr.msg)
	}
	if !templateExposesPorts(ps.Template) && reverse == nil && !cfg.Shadow.AlwaysShadowEnabled() {
		return
	}
	switch {
	case terminal && !shadowDone:
	case !terminal && st.State == JobRunning && st.NodeName != "":
	default:
		return
	}
	spec := ShadowSpec{
		WorkloadUID:  rec.Request.WorkloadUID,
		WorkloadName: ownerName(rec.Request),
		Namespace:    rec.Request.Namespace,
		Template:     ps.Template,
		NodeName:     st.NodeName,
		SocketDir:    st.SocketDir,
		Terminal:     terminal,
		Reverse:      reverse,
	}
	ref, err := d.Shadow.Ensure(ctx, spec)
	if err != nil {
		log.G(d.Sidecar.Ctx).Warning("dispatch: shadow of workload ", spec.WorkloadUID, ": ", err)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.records[rec.Request.WorkloadUID] != rec {
		return
	}
	changed := rec.Shadow == nil || *rec.Shadow != ref
	rec.Shadow = &ref
	if terminal {
		changed = changed || !rec.ShadowTerminal
		rec.ShadowTerminal = true
	}
	if changed {
		if err := d.saveRecord(rec); err != nil {
			log.G(d.Sidecar.Ctx).Warning("dispatch: cannot record the shadow of workload ", spec.WorkloadUID, ": ", err)
		}
	}
}

// JobDeleteHandler is DELETE /jobs/{workloadUID}.
func (d *Dispatcher) JobDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if !d.authorize(w, r) {
		return
	}
	uid := r.PathValue("workloadUID")
	if !workloadUIDPattern.MatchString(uid) {
		writeAPIError(w, permanentError("invalid workloadUID %q", uid))
		return
	}
	if aerr := d.deleteWorkload(r.Context(), uid); aerr != nil {
		writeAPIError(w, aerr)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteWorkload cancels the workload's job unless it ended, removes its shadow,
// its members' directories and its record. A workload with no record only has
// its shadow removed. On an error the record is kept, so the call can be repeated.
func (d *Dispatcher) deleteWorkload(ctx context.Context, uid string) *apiError {
	d.mu.Lock()
	defer d.mu.Unlock()
	rec := d.records[uid]
	if rec == nil {
		if err := d.shadowManager().Delete(ctx, uid); err != nil {
			return transientError("deleting the shadow of workload %s: %v", uid, err)
		}
		return nil
	}

	state, _, gone, err := d.slurmJobState(rec.JobID)
	if !gone && (err != nil || !slurmTerminalState(state)) {
		if cerr := d.scancel(rec.JobID); cerr != nil {
			return transientError("%v", cerr)
		}
	}
	if err := d.shadowManager().Delete(ctx, uid); err != nil {
		return transientError("deleting the shadow of workload %s: %v", uid, err)
	}
	d.removeMembers(ctx, memberPods(rec))
	if err := os.Remove(d.recordPath(uid)); err != nil && !os.IsNotExist(err) {
		log.G(d.Sidecar.Ctx).Warning("dispatch: cannot remove the record of workload ", uid, ": ", err)
	}
	delete(d.records, uid)
	log.G(d.Sidecar.Ctx).Info("dispatch: workload ", uid, " deleted (Slurm job ", rec.JobID, ")")
	return nil
}
