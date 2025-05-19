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
	"testing"
	"time"
)

type SingleTask struct {
}

func (task SingleTask) Kind() string {
	return "SingleTask"
}

type SingleTaskWorker struct {
	wg *sync.WaitGroup
	uptask.TaskHandlerDefaults[SingleTask]
}

func (w *SingleTaskWorker) ProcessTask(context.Context, *uptask.Container[SingleTask]) error {
	defer w.wg.Done()
	return nil
}

func SingleTaskTest(t *testing.T, port int, tunnelUrl string) {

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
	wg.Add(1)
	uptask.AddTaskHandler(tsvc, &SingleTaskWorker{wg: &wg})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = tsvc.StartTask(ctx, SingleTask{}, nil)
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

}

type SingleEvent struct {
	Id string `json:"id"`
}

func (task SingleEvent) Kind() string {
	return "SingleEvent"
}

type SingleEventWorker struct {
	wg *sync.WaitGroup
	uptask.TaskHandlerDefaults[SingleEvent]
}

func (w *SingleEventWorker) ProcessEvent(ctx context.Context, event *uptask.Container[SingleEvent]) error {
	if event.Args.Id != "dummy" {
		return errors.New("invalid event")
	}
	defer w.wg.Done()
	return nil
}

func SingleEventTest(t *testing.T, port int, tunnelUrl string) {

	transport, err := uptask.NewUpstashTransport(testutl.ReadTokenFromEnv(), tunnelUrl)
	require.NoError(t, err)
	tsvc := uptask.NewTaskService(transport)
	tsvc.Use()

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

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wg3 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)
	wg3.Add(1)
	uptask.AddEventHandler(tsvc, "myHandler1", &SingleEventWorker{wg: &wg1})
	uptask.AddEventHandler(tsvc, "myHandler2", &SingleEventWorker{wg: &wg2})
	uptask.AddEventHandler(tsvc, "myHandler3", &SingleEventWorker{wg: &wg3})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	_, err = tsvc.PublishEvent(ctx, SingleEvent{Id: "dummy"}, nil)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.FailNow(t, "test timed out")
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			wg1.Wait()
			wg2.Wait()
			wg3.Wait()
			close(ch)
		}()
		return ch
	}():
		// Task completed successfully
	}

}
