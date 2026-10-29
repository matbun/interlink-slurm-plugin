package slurm

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// LogsReconnectBanner is printed before the log is copied again after a lost
// stream.
const LogsReconnectBanner = "--- log stream reconnected, full log follows ---"

// logsRetryDelay is how long the logs command waits before it reconnects.
var logsRetryDelay = 5 * time.Second

// RunLogsCommand is "logs --agent <url> --workload <uid> [--container <c>]
// [--follow]": it copies the workload's /getLogs to stdout. When the stream
// breaks before its end it waits, prints LogsReconnectBanner and copies the whole
// log again. With --follow, a stream that ended cleanly leaves the command idle
// until ctx is done, so the container keeps its output and is not restarted.
func RunLogsCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	agent := fs.String("agent", "", "URL of the agent")
	workload := fs.String("workload", "", "UID of the workload")
	container := fs.String("container", "", "container (default: the first one)")
	follow := fs.Bool("follow", false, "follow the log until the job ends")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *agent == "" || *workload == "" {
		return errors.New("logs: --agent and --workload are required")
	}
	q := url.Values{"workloadUID": {*workload}}
	if *container != "" {
		q.Set("container", *container)
	}
	if *follow {
		q.Set("follow", "true")
	}
	target := strings.TrimRight(*agent, "/") + "/getLogs?" + q.Encode()

	connected := false
	for {
		ok, err := copyLogsOnce(ctx, target, stdout)
		if err == nil {
			if *follow {
				<-ctx.Done()
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprintln(stderr, "logs:", err)
		connected = connected || ok
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(logsRetryDelay):
		}
		if connected {
			fmt.Fprintln(stdout, LogsReconnectBanner)
		}
	}
}

// copyLogsOnce copies one /getLogs answer to out. connected is set once the
// agent answered 200; err is nil only for a stream that reached its end.
func copyLogsOnce(ctx context.Context, target string, out io.Writer) (connected bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("agent answered %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		return true, err
	}
	return true, nil
}
