package uptask

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"log/slog"
	"os"
	"sync"
)

type Handler interface {
	HandleEvent(context.Context, cloudevents.Event) error
}

type HandlerFunc func(context.Context, cloudevents.Event) error

type Logger interface {
	Debug(string, ...any)
	Warn(string, ...any)
	Info(string, ...any)
	Error(string, ...any)
}

type TaskService struct {
	client        *TaskClient
	log           Logger
	mux           sync.Mutex
	handlersAdded bool
	middlewares   []Middleware
	handlersMap   map[string]handlerInfo // task kind -> handler info
}

// NewTaskService initializes a new registry of available task handlers.
//
// Use the top-level AddTaskHandler function combined with a TaskService registry to
// register each available task handler.
func NewTaskService(client *TaskClient) *TaskService {
	l := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return &TaskService{
		log:         l,
		client:      client,
		handlersMap: make(map[string]handlerInfo),
		middlewares: make([]Middleware, 0),
	}
}

// handlerInfo bundles information about a registered task handler for later lookup
// in a TaskService bundle.
type handlerInfo struct {
	taskArgs TaskArgs
	handler  HandlerFunc
}

func (w *TaskService) add(taskArgs TaskArgs, taskUnitFactory TaskUnitFactory) error {
	kind := taskArgs.Kind()
	w.mux.Lock()
	defer w.mux.Unlock()

	if _, ok := w.handlersMap[kind]; ok {
		return fmt.Errorf("handler for kind %q is already registered", kind)
	}

	if kind == "" {
		return fmt.Errorf("taskKind cannot be empty")
	}
	if taskUnitFactory == nil {
		return fmt.Errorf("taskUnitFactory cannot be nil")
	}

	// Create the base handler for this task type
	baseHandler := func(ctx context.Context, ce cloudevents.Event) error {
		taskUnit := taskUnitFactory.MakeUnit(ce)
		anyTask, insertOpts, err := taskUnit.UnmarshalTask()
		if err != nil {
			return fmt.Errorf("failed to unmarshal task: %w", err)
		}
		w.log.Info("processing task", "kind", kind, "id", anyTask.Id, "attempt", anyTask.Attempt, "retried", anyTask.Retried, "args", anyTask.Args, "insertOpts", insertOpts)
		err = taskUnit.ProcessTask(ctx)
		if err != nil {
			return fmt.Errorf("failed to process task: %w", err)
		}

		return nil
	}

	// Apply snooze middleware to the base handler
	snoozeMw := handleSnooze(w.client, w.log)
	baseHandler = snoozeMw(baseHandler)

	// Apply all middleware to the base handler
	handler := baseHandler
	for i := len(w.middlewares) - 1; i >= 0; i-- {
		handler = w.middlewares[i](handler)
	}

	w.handlersMap[kind] = handlerInfo{
		taskArgs: taskArgs,
		handler:  handler,
	}

	w.handlersAdded = true
	w.log.Info("task handler registered", "kind", kind)

	return nil
}

func (t *TaskService) Use(middlewares ...Middleware) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if t.handlersAdded {
		panic("cannot add middleware after handlers are added")
	}
	t.middlewares = append(t.middlewares, middlewares...)
}

// TaskService is a list of available task handlers. A TaskHandler must be registered for
// each type of Task to be handled.
//
// Use the top-level AddTaskHandler function combined with a TaskService to register a
// task handler.

// HandleEvent processes a CloudEvent with all registered middleware
func (w *TaskService) HandleEvent(ctx context.Context, ce cloudevents.Event) error {
	w.log.Debug("handling event", "type", ce.Type(), "source", ce.Source(), "id", ce.ID())
	handlerInfo, ok := w.handlersMap[ce.Type()]
	if !ok {
		return fmt.Errorf("no handler registered for task type: %s", ce.Type())
	}
	return handlerInfo.handler(ctx, ce)
}
