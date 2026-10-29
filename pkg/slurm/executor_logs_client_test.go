package slurm

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer safe to read while the command writes it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunLogsCommandReconnectsThenIdles(t *testing.T) {
	old := logsRetryDelay
	logsRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { logsRetryDelay = old })

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("workloadUID") != executorWorkload || q.Get("follow") != "true" || q.Get("container") != executorCtn {
			http.Error(w, "bad query "+r.URL.RawQuery, http.StatusBadRequest)
			return
		}
		switch calls.Add(1) {
		case 1:
			// Breaks off: the client sees less than the declared length.
			w.Header().Set("Content-Length", strconv.Itoa(100))
			_, _ = w.Write([]byte("line1\n"))
		default:
			_, _ = w.Write([]byte("line1\nline2\n"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- RunLogsCommand(ctx, []string{"--executor", srv.URL + "/", "--workload", executorWorkload, "--container", executorCtn, "--follow"}, &stdout, &stderr)
	}()

	want := "line1\n" + LogsReconnectBanner + "\nline1\nline2\n"
	deadline := time.Now().Add(5 * time.Second)
	for stdout.String() != want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := stdout.String(); got != want {
		t.Fatalf("stdout = %q, want %q (stderr %q)", got, want, stderr.String())
	}
	select {
	case err := <-done:
		t.Fatalf("returned after a clean end of the stream: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d requests, want 2: a clean end must not reconnect", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not return when its context ended")
	}
}

func TestRunLogsCommandWithoutFollowReturns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("all\n"))
	}))
	defer srv.Close()
	var stdout, stderr syncBuffer
	if err := RunLogsCommand(context.Background(), []string{"--executor", srv.URL, "--workload", executorWorkload}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "all\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if err := RunLogsCommand(context.Background(), []string{"--executor", srv.URL}, &stdout, &stderr); err == nil {
		t.Fatal("accepted a call without --workload")
	}
}
