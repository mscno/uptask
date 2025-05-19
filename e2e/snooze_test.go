package e2e

import (
	"context"
	"fmt"
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/testutl"
	"github.com/mscno/uptask/uptaskhttp"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type SnoozeTask struct {
}

func (task SnoozeTask) Kind() string {
	return "SnoozeTask"
}

type SnoozeTaskWorker struct {
	mux       sync.Mutex
	events    []*uptask.Container[SnoozeTask]
	attempted *int32
	wg        *sync.WaitGroup
	uptask.TaskHandlerDefaults[SnoozeTask]
}

func (w *SnoozeTaskWorker) ProcessTask(ctx context.Context, task *uptask.Container[SnoozeTask]) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.events = append(w.events, task)
	atomic.AddInt32(w.attempted, 1)
	if task.Retried == 0 {
		fmt.Println("Snooze task retried")
		return uptask.JobSnooze(time.Second * 3)
	}
	defer w.wg.Done()
	return nil
}

func SnoozeTaskTest(t *testing.T, port int, tunnelUrl string) {

	transport, err := uptask.NewUpstashTransport(testutl.ReadTokenFromEnv(), tunnelUrl)
	require.NoError(t, err)
	tsvc := uptask.NewTaskService(transport)

	handler := http.NewServeMux()
	handler.HandleFunc("POST /", uptaskhttp.HandleTasks(tsvc))

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

	var wg sync.WaitGroup
	var attempted int32
	wg.Add(1)
	worker := &SnoozeTaskWorker{events: make([]*uptask.Container[SnoozeTask], 0), attempted: &attempted, wg: &wg}
	uptask.AddTaskHandler(tsvc, worker)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	_, err = tsvc.StartTask(ctx, SnoozeTask{}, nil)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("test timed out")
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			wg.Wait()
			close(ch)
		}()
		return ch
	}():
		// Task completed successfully
	}
	require.Equal(t, int32(2), atomic.LoadInt32(&attempted))
	require.Equal(t, 2, len(worker.events))
	require.Equal(t, 0, worker.events[0].Retried)
	require.Equal(t, 3, worker.events[0].InsertOpts.MaxRetries)
	require.Equal(t, 1, worker.events[1].Retried)
	require.Equal(t, 4, worker.events[1].InsertOpts.MaxRetries)
}

type SnoozeEvent struct {
}

func (task SnoozeEvent) Kind() string {
	return "SnoozeEvent"
}

type SnoozeEventWorker struct {
	mux       sync.Mutex
	events    []*uptask.Container[SnoozeEvent]
	attempted *int32
	wg        *sync.WaitGroup
	uptask.TaskHandlerDefaults[SnoozeEvent]
}

func (w *SnoozeEventWorker) ProcessEvent(ctx context.Context, task *uptask.Container[SnoozeEvent]) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.events = append(w.events, task)
	atomic.AddInt32(w.attempted, 1)
	if task.Retried == 0 {
		fmt.Println("Snooze task retried")
		return uptask.JobSnooze(time.Second * 3)
	}
	defer w.wg.Done()
	return nil
}

func SnoozeEventTest(t *testing.T, port int, tunnelUrl string) {

	transport, err := uptask.NewUpstashTransport(testutl.ReadTokenFromEnv(), tunnelUrl)
	require.NoError(t, err)
	tsvc := uptask.NewTaskService(transport)

	handler := http.NewServeMux()
	handler.HandleFunc("POST /", uptaskhttp.HandleTasks(tsvc))

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

	var wg sync.WaitGroup
	var attempted int32
	wg.Add(1)
	worker := &SnoozeEventWorker{events: make([]*uptask.Container[SnoozeEvent], 0), attempted: &attempted, wg: &wg}
	uptask.AddEventHandler(tsvc, "snoozeHandler", worker)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	_, err = tsvc.PublishEvent(ctx, SnoozeEvent{}, nil)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("test timed out")
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			wg.Wait()
			close(ch)
		}()
		return ch
	}():
		// Task completed successfully
	}
	require.Equal(t, int32(2), atomic.LoadInt32(&attempted))
	require.Equal(t, 2, len(worker.events))
	require.Equal(t, 0, worker.events[0].Retried)
	require.Equal(t, 3, worker.events[0].InsertOpts.MaxRetries)
	require.Equal(t, 1, worker.events[1].Retried)
	require.Equal(t, 4, worker.events[1].InsertOpts.MaxRetries)
}
