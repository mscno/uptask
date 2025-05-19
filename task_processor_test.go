package uptask

import (
	"bufio"
	"context"
	"fmt"
	"github.com/mscno/uptask/internal/events"
	"github.com/mscno/uptask/internal/httputil"
	"github.com/mscno/uptask/testutl"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"regexp"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

/*
	Local Dummy Transport
*/

func dummyTransport() Transport {
	return transportFn(func(ctx context.Context, ce cloudevents.Event, opts *InsertOpts) error {
		slog.Info("dummy transport: printing task to console", "task", ce.Type())
		return nil
	})
}

/*
	Dummy task
*/

type DummyTask struct {
	Name      string
	Snooze    bool
	FailFirst bool
}

func (t DummyTask) Kind() string {
	return "DummyTask"
}

type DummyTaskProcessor struct {
	mux sync.Mutex
	TaskHandlerDefaults[DummyTask]
	Tasks []taskContainer
}

type taskContainer struct {
	Id          string
	Retried     int
	CompletedAt time.Time
	CreatedAt   time.Time
	DummyTask   DummyTask
}

func (svc *DummyTaskProcessor) ProcessTask(ctx context.Context, task *Container[DummyTask]) error {
	svc.mux.Lock()
	defer svc.mux.Unlock()
	fmt.Printf("Processing task: %s\n", task.Args.Name)
	fmt.Printf("Task type: %s\n", task.Args.Kind())
	fmt.Printf("Task ID: %s\n", task.Id)
	fmt.Printf("Task Retried: %d\n", task.Retried)
	fmt.Printf("Task Created At: %s\n", task.CreatedAt)
	fmt.Println("")

	if task.Args.FailFirst && task.Retried == 0 {
		return fmt.Errorf("failing on first")
	}

	if task.Args.Snooze && time.Now().Sub(task.CreatedAt) < time.Second*10 {
		fmt.Printf("Snoozing for 15 seconds\n")
		return JobSnooze(15 * time.Second)
	}

	fmt.Printf("Task completed: %s\n", task.Args.Name)

	svc.Tasks = append(svc.Tasks, taskContainer{
		Id:          task.Id,
		Retried:     task.Retried,
		CompletedAt: time.Now(),
		CreatedAt:   task.CreatedAt,
		DummyTask:   task.Args})
	return nil
}

type DummyEventProcessor struct {
	mux sync.Mutex
	TaskHandlerDefaults[DummyTask]
	Tasks []taskContainer
}

func (svc *DummyEventProcessor) ProcessEvent(ctx context.Context, task *Container[DummyTask]) error {
	svc.mux.Lock()
	defer svc.mux.Unlock()
	fmt.Printf("Processing task: %s\n", task.Args.Name)
	fmt.Printf("Task type: %s\n", task.Args.Kind())
	fmt.Printf("Task ID: %s\n", task.Id)
	fmt.Printf("Task Retried: %d\n", task.Retried)
	fmt.Printf("Task Created At: %s\n", task.CreatedAt)
	fmt.Println("")

	if task.Args.FailFirst && task.Retried == 0 {
		return fmt.Errorf("failing on first")
	}

	if task.Args.Snooze && time.Now().Sub(task.CreatedAt) < time.Second*10 {
		fmt.Printf("Snoozing for 15 seconds\n")
		return JobSnooze(15 * time.Second)
	}

	fmt.Printf("Task completed: %s\n", task.Args.Name)

	svc.Tasks = append(svc.Tasks, taskContainer{
		Id:          task.Id,
		Retried:     task.Retried,
		CompletedAt: time.Now(),
		CreatedAt:   task.CreatedAt,
		DummyTask:   task.Args})
	return nil
}

func TestEventHandler(t *testing.T) {

	tsvc := NewTaskService(dummyTransport())
	AddEventHandler(tsvc, "myHandler", &DummyEventProcessor{})

	dummyTask := DummyTask{Name: "test"}
	ce := cloudevents.NewEvent()
	ce.SetID(uuid.NewString())
	ce.SetSource("defensedata")
	ce.SetData("application/json", dummyTask)
	ce.SetExtension(events.TaskRetriedExtension, "0")
	ce.SetExtension(events.ScheduledTaskExtension, "false")
	ce.SetExtension(events.QstashMessageIdExtension, "123")
	ce.SetExtension(events.ScheduleIdExtension, "456")
	ctx := context.Background()
	err := tsvc.handlersMap["myHandler/DummyTask"].handler.HandleEvent(ctx, ce)
	require.NoError(t, err)
}

func TestTaskHandler(t *testing.T) {

	tsvc := NewTaskService(dummyTransport())
	AddTaskHandler(tsvc, &DummyTaskProcessor{})

	dummyTask := DummyTask{Name: "test"}
	ce := cloudevents.NewEvent()
	ce.SetID(uuid.NewString())
	ce.SetSource("defensedata")
	ce.SetData("application/json", dummyTask)
	ce.SetExtension(events.TaskRetriedExtension, "0")
	ce.SetExtension(events.ScheduledTaskExtension, "false")
	ce.SetExtension(events.QstashMessageIdExtension, "123")
	ce.SetExtension(events.ScheduleIdExtension, "456")
	ctx := context.Background()
	err := tsvc.handlersMap["DummyTask"].handler.HandleEvent(ctx, ce)
	require.NoError(t, err)
}

func TestTaskClient(t *testing.T) {

	var hit bool
	handler := http.NewServeMux()
	handler.Handle("POST /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	tsvc := NewTaskClient(newHttpTransport(srv.URL))
	id, err := tsvc.StartTask(context.Background(), DummyTask{Name: "test"}, nil)
	require.NoError(t, err)
	require.True(t, hit)
	_, err = uuid.Parse(id)
	require.NoError(t, err)
}

func TestTaskHandlerAndClient(t *testing.T) {

	tsvc := NewTaskService(dummyTransport())

	handler := http.NewServeMux()
	handler.Handle("POST /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ce, err := httputil.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ce.SetExtension(events.QstashMessageIdExtension, uuid.NewString())
		err = tsvc.HandleEvent(r.Context(), ce)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	worker := &DummyTaskProcessor{}
	AddTaskHandler(tsvc, worker)

	tsvcClient := NewTaskClient(newHttpTransport(srv.URL))

	dummyTask := DummyTask{Name: "test"}
	id, err := tsvcClient.StartTask(context.Background(), dummyTask, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(worker.Tasks))
	require.NotEmpty(t, id)
	_, err = uuid.Parse(id)
	require.NoError(t, err)
}

func TestTaskHandlerAndClientWithMiddleware(t *testing.T) {

	tsvc := NewTaskService(dummyTransport())

	handler := http.NewServeMux()
	handler.Handle("POST /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ce, err := httputil.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ce.SetExtension(events.QstashMessageIdExtension, uuid.NewString())
		err = tsvc.HandleEvent(r.Context(), ce)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	worker := &DummyTaskProcessor{}
	tsvc.Use(func(handlerFunc HandlerFunc) HandlerFunc {
		return func(ctx context.Context, event cloudevents.Event) error {
			if value, ok := event.Extensions()["test"].(string); !ok {
				t.Errorf("test extension is not present")
			} else {
				fmt.Printf("test extension: %s\n", value)
			}

			return handlerFunc(ctx, event)
		}
	})
	AddTaskHandler(tsvc, worker)

	tsvcClient := NewTaskClient(newHttpTransport(srv.URL))
	tsvcClient.Use(func(handlerFunc HandlerFunc) HandlerFunc {
		return func(ctx context.Context, event cloudevents.Event) error {
			ext := ctx.Value("test")
			event.SetExtension("test", ext)
			return handlerFunc(ctx, event)
		}
	})
	ctx := context.Background()
	ctx = context.WithValue(ctx, "test", "hello")
	dummyTask := DummyTask{Name: "test"}
	id, err := tsvcClient.StartTask(ctx, dummyTask, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(worker.Tasks))
	require.NotEmpty(t, id)
	_, err = uuid.Parse(id)
	require.NoError(t, err)
}

func TestLocalTunnelStreamingOutput(t *testing.T) {
	t.Skip()
	// Start the command
	cmd := exec.Command("npx", "localtunnel", "--port", "8080")
	t.Cleanup(func() {
		cmd.Process.Kill()
	})
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)

	err = cmd.Start()
	require.NoError(t, err)

	// Create a scanner to read the command's output line by line
	scanner := bufio.NewScanner(stdout)
	re := regexp.MustCompile(`your url is:\s*(https://[^\s]+)`)

	var tunnelURL string

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line) // For debugging purposes

		// Check if the line contains the URL
		matches := re.FindStringSubmatch(line)
		if len(matches) > 1 {
			tunnelURL = matches[1]
			break
		}
	}

	// Ensure the URL was found
	require.NotEmpty(t, tunnelURL, "Tunnel URL was not found")

	// Print the extracted URL
	fmt.Printf("Tunnel URL: %s\n", tunnelURL)

	// Optionally, wait for the command to finish
	err = cmd.Process.Kill()
	require.NoError(t, err)
}

func TestEnqueueTask(t *testing.T) {
	transport, err := NewUpstashTransport(testutl.ReadTokenFromEnv(), "https://zzz.requestcatcher.com/")
	require.NoError(t, err)
	tclient := NewTaskClient(transport)
	_, err = tclient.StartTask(context.Background(), DummyTask{Name: "test"}, &InsertOpts{Queue: "missing-queue"})
	require.NoError(t, err)
}

func TestPublishEventWithNoHandlers(t *testing.T) {
	tsvc := NewTaskService(dummyTransport())
	_, err := tsvc.PublishEvent(context.Background(), DummyTask{}, nil)
	require.Error(t, err)
}

func TestPublishEventWithHandlers(t *testing.T) {
	tsvc := NewTaskService(dummyTransport())
	AddEventHandler(tsvc, "dummyHandler", &DummyEventProcessor{})
	_, err := tsvc.PublishEvent(context.Background(), DummyTask{}, nil)
	require.NoError(t, err)
}
