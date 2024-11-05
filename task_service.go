package uptask

import (
	"context"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/samber/oops"
	"sync"
	"time"
)

type Handler interface {
	HandleEvent(context.Context, cloudevents.Event) error
}

type HandlerFunc func(context.Context, cloudevents.Event) error

type TaskService struct {
	client      *TaskClient
	mux         sync.Mutex
	middlewares []Middleware
	handlersMap map[string]handlerInfo // task kind -> handler info
}

// NewTaskService initializes a new registry of available task handlers.
//
// Use the top-level AddTaskHandler function combined with a TaskService registry to
// register each available task handler.
func NewTaskService(client *TaskClient) *TaskService {

	return &TaskService{
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
		if err := taskUnit.UnmarshalTask(); err != nil {
			return oops.Wrapf(err, "failed to unmarshal job")
		}

		err := taskUnit.ProcessTask(ctx)
		var snoozeErr *jobSnoozeError
		if errors.As(err, &snoozeErr) {
			if snoozeErr.duration > 0 {
				opts, err := insertInsertOptsFromEvent(ce)
				if err != nil {
					return oops.Wrapf(err, "failed to get insert options from event")
				}
				// Sleep for the snooze duration
				// Requeue the task
				fmt.Printf("Snoozing for %s\n", snoozeErr.duration)
				opts.ScheduledAt = time.Now().Add(snoozeErr.duration)
				return w.client.transport.Send(ctx, ce, opts)
			}
		}

		if err != nil {
			return oops.Wrapf(err, "failed to process task")
		}

		return nil
	}

	// Apply all middleware to the base handler
	handler := baseHandler
	for i := len(w.middlewares) - 1; i >= 0; i-- {
		handler = w.middlewares[i](handler)
	}

	w.handlersMap[kind] = handlerInfo{
		taskArgs: taskArgs,
		handler:  handler,
	}

	return nil
}

func (t *TaskService) Use(middlewares ...Middleware) {
	t.mux.Lock()
	defer t.mux.Unlock()
	t.middlewares = append(t.middlewares, middlewares...)
}

// TaskService is a list of available task handlers. A TaskHandler must be registered for
// each type of Task to be handled.
//
// Use the top-level AddTaskHandler function combined with a TaskService to register a
// task handler.

// HandleEvent processes a CloudEvent with all registered middleware
func (w *TaskService) HandleEvent(ctx context.Context, ce cloudevents.Event) error {
	handlerInfo, ok := w.handlersMap[ce.Type()]
	if !ok {
		return fmt.Errorf("no handler registered for task type: %s", ce.Type())
	}

	return handlerInfo.handler(ctx, ce)
}
