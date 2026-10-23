package slurm

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // SiteTimeZone works in a minimal image

	"github.com/containerd/containerd/log"

	exec "github.com/alexellis/go-execute/pkg/v1"
)

// EstimateRequest asks when Slurm would start a job of Nodes nodes with GPUsPerNode
// GPUs each, submitted now with the default flavor's flags. Used by a Kueue admission
// check, so that a multi-node job is admitted only when the site can run it soon.
type EstimateRequest struct {
	Nodes       int `json:"nodes"`
	GPUsPerNode int `json:"gpusPerNode"`
}

// EstimateResponse is Slurm's answer: the expected start time, and its raw output.
type EstimateResponse struct {
	Start time.Time `json:"start"`
	Raw   string    `json:"raw"`
}

// testOnlyStartRe reads `sbatch --test-only`'s answer, e.g.
// "sbatch: Job 123 to start at 2026-10-22T20:00:00 using 160 processors on nodes ...".
var testOnlyStartRe = regexp.MustCompile(`to start at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})`)

// estimateFlags builds the sbatch arguments of a test-only submission.
func estimateFlags(config SlurmConfig, req EstimateRequest) []string {
	flags := []string{"--test-only", "--nodes=" + strconv.Itoa(req.Nodes), "--ntasks-per-node=1"}
	if flavor, ok := config.Flavors[config.DefaultFlavor]; ok {
		flags = append(flags, flavor.SlurmFlags...)
	}
	if req.GPUsPerNode > 0 && !hasGPUFlag(flags) {
		switch config.GPUFlag {
		case GPUFlagGPUsPerNode, GPUFlagGPUs:
			n := req.GPUsPerNode
			if config.GPUFlag == GPUFlagGPUs {
				n *= req.Nodes
			}
			flags = append(flags, "--"+config.GPUFlag+"="+strconv.Itoa(n))
		default:
			flags = append(flags, "--gres=gpu:"+strconv.Itoa(req.GPUsPerNode))
		}
	}
	return append(flags, "--wrap=true")
}

func hasGPUFlag(flags []string) bool {
	for _, f := range flags {
		if declaresGPUs(f) {
			return true
		}
	}
	return false
}

// parseTestOnly returns the start time Slurm announced, in the local time zone of the
// site (Slurm prints local time without a zone).
func parseTestOnly(output string, loc *time.Location) (time.Time, error) {
	m := testOnlyStartRe.FindStringSubmatch(output)
	if m == nil {
		return time.Time{}, fmt.Errorf("no start time in sbatch --test-only output: %q", strings.TrimSpace(output))
	}
	return time.ParseInLocation("2006-01-02T15:04:05", m[1], loc)
}

// EstimateHandler answers POST /estimate with when Slurm would start such a job now.
func (h *SidecarHandler) EstimateHandler(w http.ResponseWriter, r *http.Request) {
	var req EstimateRequest
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil || req.Nodes < 1 {
		http.Error(w, fmt.Sprintf("want {\"nodes\": N >= 1, \"gpusPerNode\": M}: %v", err), http.StatusBadRequest)
		return
	}
	task := exec.ExecTask{Command: h.Config.Sbatchpath, Args: estimateFlags(h.Config, req), Shell: true}
	res, err := task.Execute()
	out := res.Stdout + res.Stderr
	if err != nil {
		http.Error(w, fmt.Sprintf("sbatch --test-only: %v: %s", err, out), http.StatusBadGateway)
		return
	}
	loc := time.Local
	if h.Config.SiteTimeZone != "" {
		if loc, err = time.LoadLocation(h.Config.SiteTimeZone); err != nil {
			http.Error(w, fmt.Sprintf("SiteTimeZone: %v", err), http.StatusInternalServerError)
			return
		}
	}
	start, err := parseTestOnly(out, loc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	log.G(h.Ctx).Infof("Estimate for %d node(s) x %d GPU(s): start at %s", req.Nodes, req.GPUsPerNode, start)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(EstimateResponse{Start: start, Raw: strings.TrimSpace(out)}); err != nil {
		log.G(h.Ctx).Error(err)
	}
}
