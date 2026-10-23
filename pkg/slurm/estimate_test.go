package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testFlavor is the default flavor's name.
const testFlavor = "default"

func TestEstimateFlags(t *testing.T) {
	config := SlurmConfig{
		DefaultFlavor: testFlavor,
		Flavors:       map[string]FlavorConfig{testFlavor: {Name: testFlavor, SlurmFlags: []string{"--qos=acc_debug", "-A", "proj"}}},
	}
	got := strings.Join(estimateFlags(config, EstimateRequest{Nodes: 2, GPUsPerNode: 4}), " ")
	for _, want := range []string{"--test-only", "--nodes=2", "--qos=acc_debug", "-A proj", testGres4, "--wrap=true"} {
		if !strings.Contains(got, want) {
			t.Errorf("estimate flags %q missing %q", got, want)
		}
	}
	config.GPUFlag = GPUFlagGPUs
	if got := strings.Join(estimateFlags(config, EstimateRequest{Nodes: 2, GPUsPerNode: 4}), " "); !strings.Contains(got, "--gpus=8") {
		t.Errorf("GPUFlag gpus: want the total --gpus=8 in %q", got)
	}
}

func TestEstimateHandler(t *testing.T) {
	sbatch := filepath.Join(t.TempDir(), "sbatch")
	script := "#!/bin/sh\necho 'sbatch: Job 4242 to start at 2026-10-22T20:15:00 using 160 processors on nodes as01r1b[01-02] in partition acc' >&2\n"
	if err := os.WriteFile(sbatch, []byte(script), 0o700); err != nil { //nolint:gosec // a fake executable
		t.Fatal(err)
	}
	h := &SidecarHandler{Config: SlurmConfig{Sbatchpath: sbatch}, Ctx: context.Background()}

	rec := httptest.NewRecorder()
	h.EstimateHandler(rec, httptest.NewRequest(http.MethodPost, "/estimate", bytes.NewReader([]byte(`{"nodes":2,"gpusPerNode":4}`))))
	if rec.Code != http.StatusOK {
		t.Fatalf("code %d: %s", rec.Code, rec.Body.String())
	}
	var resp EstimateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 10, 22, 20, 15, 0, 0, time.Local)
	if !resp.Start.Equal(want) {
		t.Errorf("start = %s, want %s", resp.Start, want)
	}

	rec = httptest.NewRecorder()
	h.EstimateHandler(rec, httptest.NewRequest(http.MethodPost, "/estimate", bytes.NewReader([]byte(`{"nodes":0}`))))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("nodes 0: code %d, want 400", rec.Code)
	}
}
