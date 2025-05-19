package e2e

import (
	"context"
	"errors"
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

type RetryTask struct {
}

func (task RetryTask) Kind() string {
	return "RetryTask"
}

type RetryTaskWorker struct {
	attempted *int32
	wg        *sync.WaitGroup
	uptask.TaskHandlerDefaults[RetryTask]
}

func (w *RetryTaskWorker) ProcessTask(ctx context.Context, task *uptask.Container[RetryTask]) error {
	atomic.AddInt32(w.attempted, 1)
	if task.Retried == 0 {
		fmt.Println("Retry task failed on first attempt")
		return errors.New("failed dummy")
	}
	defer w.wg.Done()
	return nil
}

func RetryTaskTest(t *testing.T, port int, tunnelUrl string) {

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
	uptask.AddTaskHandler(tsvc, &RetryTaskWorker{attempted: &attempted, wg: &wg})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	_, err = tsvc.StartTask(ctx, RetryTask{}, nil)
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
}

type RetryEvent struct {
}

func (task RetryEvent) Kind() string {
	return "RetryEvent"
}

type RetryEventWorker struct {
	attempted *int32
	wg        *sync.WaitGroup
	uptask.TaskHandlerDefaults[RetryEvent]
}

func (w *RetryEventWorker) ProcessEvent(ctx context.Context, task *uptask.Container[RetryEvent]) error {
	atomic.AddInt32(w.attempted, 1)
	if task.Retried == 0 {
		fmt.Println("Retry event failed on first attempt")
		return errors.New("failed dummy")
	}
	defer w.wg.Done()
	return nil
}

func RetryEventTest(t *testing.T, port int, tunnelUrl string) {

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
	uptask.AddEventHandler(tsvc, "snoozeHandler", &RetryEventWorker{attempted: &attempted, wg: &wg})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	_, err = tsvc.PublishEvent(ctx, RetryEvent{}, nil)
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
}
