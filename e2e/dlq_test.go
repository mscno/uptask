package e2e

import (
	"context"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/internal/events"
	"github.com/mscno/uptask/testutl"
	"github.com/mscno/uptask/uptaskhttp"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type DummyDlqTask struct {
	DummyArg string `json:"dummy_arg"`
}

func (task DummyDlqTask) Kind() string {
	return "DummyDlqTask"
}

type DummyDlqTaskWorker struct {
	uptask.TaskHandlerDefaults[DummyDlqTask]
}

func (w *DummyDlqTaskWorker) ProcessTask(ctx context.Context, task *uptask.Container[DummyDlqTask]) error {
	return errors.New("fail")
}

type DlqTask map[string]interface {
}

func (task DlqTask) Kind() string {
	return "DlqTask"
}

type DlqTaskWorker struct {
	attempted *int32
	wg        *sync.WaitGroup
	uptask.TaskHandlerDefaults[DlqTask]
}

func (w *DlqTaskWorker) ProcessTask(ctx context.Context, task *uptask.Container[DlqTask]) error {
	atomic.AddInt32(w.attempted, 1)
	if task.Retried == 0 {
		fmt.Println("Retry task failed on first attempt")
		return errors.New("failed dummy")
	}
	defer w.wg.Done()
	return nil
}

type dummyDlqStore struct {
	wg     *sync.WaitGroup
	mux    sync.Mutex
	events []cloudevents.Event
}

func (ds *dummyDlqStore) StoreDlqEvent(event cloudevents.Event) error {
	defer ds.wg.Done()
	ds.mux.Lock()
	defer ds.mux.Unlock()
	ds.events = append(ds.events, event)
	return nil
}

func DlqTaskTest(t *testing.T, port int, tunnelUrl string) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	transport, err := uptask.NewUpstashTransport(testutl.ReadTokenFromEnv(), tunnelUrl, uptask.WithDlq(tunnelUrl+"/dlq"), uptask.WithUpstashLogger(logger))
	require.NoError(t, err)
	tsvc := uptask.NewTaskService(transport, uptask.WithLogger(logger))
	var dlwWg sync.WaitGroup
	dlqStore := &dummyDlqStore{wg: &dlwWg, events: make([]cloudevents.Event, 0)}
	handler := http.NewServeMux()
	handler.HandleFunc("POST /dlq/", uptaskhttp.HandleDlq(dlqStore))
	handler.HandleFunc("POST /tasks/", uptaskhttp.HandleTasks(tsvc))
	handler.HandleFunc("POST /events/", uptaskhttp.HandleTasks(tsvc))

	// Add a 404 handler that logs not found requests
	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("404 not found", "path", r.URL.Path, "method", r.Method)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 page not found"))
	})

	srv := &http.Server{Handler: handler, Addr: ":" + strconv.Itoa(port)}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			if err != http.ErrServerClosed {
				slog.Error(err.Error())
			}
		}
	}()
	t.Cleanup(func() {
		fmt.Println("Shutting down server")
		err := srv.Shutdown(context.Background())
		if err != nil {
			slog.Error(err.Error())
		}
	})

	uptask.AddTaskHandler(tsvc, &DummyDlqTaskWorker{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
	defer cancel()

	taskId, err := tsvc.StartTask(ctx, DummyDlqTask{DummyArg: "dummy"}, &uptask.InsertOpts{
		MaxRetries: 1,
	})
	require.NoError(t, err)

	dlwWg.Add(1)

	select {
	case <-ctx.Done():
		t.Fatal("test timed out")
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			dlwWg.Wait()
			close(ch)
		}()
		return ch
	}():
		// Task completed successfully
	}
	require.Equal(t, 1, len(dlqStore.events))
	require.Equal(t, taskId, dlqStore.events[0].ID())
	require.Equal(t, "DummyDlqTask", dlqStore.events[0].Type())
	require.Equal(t, "{\"dummy_arg\":\"dummy\"}\n", string(dlqStore.events[0].Data()))
	retried, ok := events.GetRetried(&dlqStore.events[0])
	require.True(t, ok)
	require.Equal(t, 1, retried)

	maxRetries, ok := events.GetMaxRetries(&dlqStore.events[0])
	require.True(t, ok)
	require.Equal(t, 1, maxRetries)

}
