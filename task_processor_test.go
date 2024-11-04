package uptask

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"regexp"
	"sync"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type DummyTask struct {
	Name string
}

func (t DummyTask) Kind() string {
	return "DummyTask"
}

type DummyTaskProcessor struct {
	mux sync.Mutex
	TaskHandlerDefaults[DummyTask]
	Tasks []DummyTask
}

func (svc *DummyTaskProcessor) ProcessTask(ctx context.Context, task *Task[DummyTask]) error {
	svc.mux.Lock()
	defer svc.mux.Unlock()
	fmt.Printf("Processing task: %s\n", task.Args.Name)
	fmt.Printf("Task type: %s\n", task.Args.Kind())
	svc.Tasks = append(svc.Tasks, task.Args)
	return nil
}

func TestTaskHandler(t *testing.T) {

	tsvc := NewTaskService()
	AddTaskHandler(tsvc, &DummyTaskProcessor{})

	dummyTask := DummyTask{Name: "test"}
	ce := cloudevents.NewEvent()
	ce.SetID(uuid.NewString())
	ce.SetSource("defensedata")
	ce.SetData("application/json", dummyTask)
	ctx := context.Background()
	err := tsvc.handlersMap["DummyTask"].handler.HandleEvent(ctx, &ce)
	require.NoError(t, err)
}

func TestTaskClient(t *testing.T) {

	var hit bool
	handler := http.NewServeMux()
	handler.Handle("POST "+TaskRoute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)

	tsvc := NewTaskClient(NewHttpTransport(srv.URL))
	id, err := tsvc.StartTask(context.Background(), DummyTask{Name: "test"})
	require.NoError(t, err)
	require.True(t, hit)
	_, err = uuid.Parse(id)
	require.NoError(t, err)
}

func TestTaskHandlerAndClient(t *testing.T) {

	tsvc := NewTaskService()

	handler := http.NewServeMux()
	handler.Handle("POST "+TaskRoute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ce, err := cloudevents.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = tsvc.HandleEvent(r.Context(), ce)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	worker := &DummyTaskProcessor{}
	AddTaskHandler(tsvc, worker)

	tsvcClient := NewTaskClient(NewHttpTransport(srv.URL))

	dummyTask := DummyTask{Name: "test"}
	id, err := tsvcClient.StartTask(context.Background(), dummyTask)
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
