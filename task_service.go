package uptask

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"log/slog"
	"os"
	"sync"
	"time"
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
	*TaskClient
	transport     Transport
	log           Logger
	mux           sync.Mutex
	handlersAdded bool
	store         TaskStore
	storeEnabled  bool
	middlewares   []Middleware
	handlersMap   map[string]handlerInfo // task kind -> handler info
}

type ServiceOption func(*TaskService)

func WithLogger(l Logger) ServiceOption {
	return func(t *TaskService) {
		t.log = l
	}
}

func WithStore(s TaskStore) ServiceOption {
	return func(t *TaskService) {
		t.store = s
		t.storeEnabled = true
	}
}

// NewTaskService initializes a new registry of available task handlers.
//
// Use the top-level AddTaskHandler function combined with a TaskService registry to
// register each available task handler.
func NewTaskService(transport Transport, opts ...ServiceOption) *TaskService {
	l := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	svc := &TaskService{
		log:         l,
		TaskClient:  NewTaskClient(transport),
		transport:   transport,
		handlersMap: make(map[string]handlerInfo),
		middlewares: make([]Middleware, 0),
	}

	for _, opt := range opts {
		opt(svc)
	}

	return svc
}

// handlerInfo bundles information about a registered task handler for later lookup
// in a TaskService bundle.
type handlerInfo struct {
	taskArgs TaskArgs
	handler  HandlerFunc
}

func (w *TaskService) add(taskArgs TaskArgs, taskUnitFactory taskUnitFactory) error {
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

		if w.storeEnabled {
			// Check if task is first attempt and scheduled
			// If so, we need to create a new task execution
			// and update the task status to running
			if anyTask.Retried == 0 && anyTask.Scheduled {
				w.log.Debug("creating new task execution from cron source", "kind", kind, "id", anyTask.Id, "args", anyTask.Args, "insertOpts", insertOpts)
				err = w.store.CreateTaskExecution(ctx, &TaskExecution{
					ID:              ce.ID(),
					TaskKind:        ce.Type(),
					Status:          TaskStatusPending,
					Args:            anyTask.Args,
					AttemptID:       "",
					Attempt:         0,
					MaxAttempts:     insertOpts.MaxRetries,
					QstashMessageID: "",
					ScheduleID:      "",
					CreatedAt:       time.Now(),
					AttemptedAt:     time.Time{},
					ScheduledAt:     insertOpts.ScheduledAt,
					FinalizedAt:     time.Time{},
					Errors:          nil,
					Queue:           insertOpts.Queue,
				})
				if err != nil {
					return fmt.Errorf("failed to create task execution: %w", err)
				}
			}

			// Update task status to running
			err = w.store.UpdateTaskStatus(ctx, anyTask.Id, TaskStatusRunning)
			if err != nil {
				return fmt.Errorf("failed to create task execution: %w", err)
			}
		}

		w.log.Info("processing task", "kind", kind, "id", anyTask.Id, "attempt", anyTask.Attempt, "retried", anyTask.Retried, "args", anyTask.Args, "insertOpts", insertOpts)
		err = taskUnit.ProcessTask(ctx)
		if err != nil {
			taskErr := TaskError{
				Message:   err.Error(),
				Details:   nil,
				Timestamp: time.Now(),
			}
			if w.storeEnabled {
				if err := w.handleTaskError(ctx, anyTask.Id, err, taskErr, insertOpts, anyTask.Attempt); err != nil {
					return err
				}
			}
			return fmt.Errorf("failed to process task: %w", err)
		}

		if w.storeEnabled {
			err = w.store.UpdateTaskStatus(ctx, anyTask.Id, TaskStatusSuccess)
			if err != nil {
				return fmt.Errorf("failed to update task status: %w", err)
			}
		}

		return nil
	}

	// Apply snooze middleware to the base handler
	snoozeMw := snoozeMw(w.transport, w.log, w.store)
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

func (w *TaskService) handleTaskError(ctx context.Context, taskID string, err error, taskErr TaskError, opts *TaskInsertOpts, attempt int) error {
	if retryErr, ok := err.(*jobSnoozeError); ok {
		if err := w.store.UpdateTaskStatus(ctx, taskID, TaskStatusPending); err != nil {
			return fmt.Errorf("failed to update task status: %w", err)
		}
		if err := w.store.UpdateTaskSnoozedTask(ctx, taskID, time.Now().Add(retryErr.duration)); err != nil {
			return fmt.Errorf("failed to update snoozed task: %w", err)
		}
		return nil
	}

	if err := w.store.AddTaskError(ctx, taskID, taskErr); err != nil {
		return fmt.Errorf("failed to add task error: %w", err)
	}

	newStatus := TaskStatusFailed
	if opts.MaxRetries > 0 && attempt < opts.MaxRetries {
		newStatus = TaskStatusPending
	}

	if err := w.store.UpdateTaskStatus(ctx, taskID, newStatus); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

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
