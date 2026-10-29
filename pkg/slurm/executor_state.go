package slurm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/log"
)

func (d *Executor) recordPath(uid string) string {
	return filepath.Join(d.config().Executor.StateDir, uid+".json")
}

// saveRecord writes the record through a temporary file. The previous version is
// removed before the rename: on a job directory served over SFTP (a csi-rclone
// volume) a rename onto an existing file fails on the server while the mount
// reports success. A crash in between leaves only the temporary file, which
// loadRecords takes.
func (d *Executor) saveRecord(rec *workloadRecord) error {
	dir := d.config().Executor.StateDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	path := d.recordPath(rec.Request.WorkloadUID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tmp, path)
}

// loadRecords reads every record of StateDir. An unreadable one is skipped.
func (d *Executor) loadRecords() error {
	dir := d.config().Executor.StateDir
	if dir == "" {
		return fmt.Errorf("Executor.StateDir is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	for _, e := range entries {
		name := e.Name()
		// A temporary file without its record is a save interrupted after the
		// remove (see saveRecord): it is the newest version.
		if base, ok := strings.CutSuffix(name, ".tmp"); ok && strings.HasSuffix(base, ".json") && !names[base] {
			if err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, base)); err != nil {
				log.G(d.Sidecar.Ctx).Warning("executor: cannot restore record ", name, ": ", err)
				continue
			}
			name = base
		}
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			log.G(d.Sidecar.Ctx).Warning("executor: cannot read record ", name, ": ", err)
			continue
		}
		var rec workloadRecord
		if err := json.Unmarshal(raw, &rec); err != nil || len(rec.Request.PodSets) != 1 || rec.Request.WorkloadUID+".json" != name {
			log.G(d.Sidecar.Ctx).Warning("executor: skipping invalid record ", name, ": ", err)
			continue
		}
		d.records[rec.Request.WorkloadUID] = &rec
	}
	log.G(d.Sidecar.Ctx).Infof("executor: %d workload record(s) loaded from %s", len(d.records), dir)
	return nil
}
