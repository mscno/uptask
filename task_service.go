package uptask

import (
	"context"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/samber/oops"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"strings"
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
	addSystemHandlers bool
	client            *TaskClient
	log               Logger
	mux               sync.Mutex
	handlersAdded     bool
	store             TaskStore
	storeEnabled      bool
	middlewares       []Middleware
	handlersMap       map[string]handlerInfo // task kind -> handler info

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
	svc := &TaskService{
		handlersMap: make(map[string]handlerInfo),
		middlewares: make([]Middleware, 0),
	}
	if svc.log == nil {
		svc.log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	for _, opt := range opts {
		opt(svc)
	}

	var clientsopts []ClientOption
	if svc.storeEnabled {
		clientsopts = append(clientsopts, WithClientStore(svc.store))
	}
	if svc.log != nil {
		clientsopts = append(clientsopts, WithClientLogger(svc.log))
	}
	svc.client = NewTaskClient(transport, clientsopts...)

	return svc
}

// handlerInfo bundles information about a registered task handler for later lookup
// in a TaskService bundle.
type handlerInfo struct {
	taskArgs TaskArgs
	handler  HandlerFunc
}

func (w *TaskService) addTask(taskArgs TaskArgs, handlerName string, taskUnitFactory taskUnitFactory) error {
	kind := taskArgs.Kind()
	if kind == "" {
		return fmt.Errorf("taskKind cannot be empty")
	}
	if taskUnitFactory == nil {
		return fmt.Errorf("taskUnitFactory cannot be nil")
	}
	if !w.addSystemHandlers {
		w.addSystemHandlers = true
		AddTaskHandler[EventFanoutArgs](w, &EventFanoutWorker{c: w.client})
	}

	w.mux.Lock()
	defer w.mux.Unlock()

	if _, ok := w.handlersMap[kind]; ok {
		return fmt.Errorf("handler for kind %q is already registered", kind)
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
			alreadyExists, _ := w.store.TaskExists(context.WithoutCancel(ctx), anyTask.Id)
			if anyTask.Scheduled && !alreadyExists {
				if insertOpts.MaxRetries == 0 {
					w.log.Warn("max retries not set, defaulting to 3", "kind", kind, "id", anyTask.Id)
					insertOpts.MaxRetries = 3
				}
				w.log.Debug("creating new task execution from cron source", "kind", kind, "id", anyTask.Id, "args", anyTask.Args, "insertOpts", insertOpts)
				err = w.store.CreateTaskExecution(context.WithoutCancel(ctx), &TaskExecution{
					ID:              ce.ID(),
					TaskKind:        ce.Type(),
					Status:          TaskStatusPending,
					Args:            anyTask.Args,
					AttemptID:       "",
					Retried:         0, // todo decide if this should be 0 or 1
					MaxRetries:      insertOpts.MaxRetries,
					QstashMessageID: anyTask.QstashMessageId,
					ScheduleID:      anyTask.ScheduleId,
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
			err = w.store.UpdateTaskStatus(context.WithoutCancel(ctx), anyTask.Id, TaskStatusRunning)
			if err != nil {
				return fmt.Errorf("failed to update task execution: %w", err)
			}
		}

		w.log.Info("processing task", "kind", kind, "id", anyTask.Id, "retried", anyTask.Retried, "retried", anyTask.Retried, "maxRetries", insertOpts.MaxRetries)
		err = taskUnit.ProcessTask(ctx)
		err = oops.With("taskArgs", taskArgs, "task", anyTask).Wrap(err)

		if err != nil {
			//w.log.Error(err.Error(), "taskId", anyTask.Id, "error", err)
			taskErr := TaskError{
				Message:   err.Error(),
				Details:   nil,
				Timestamp: time.Now(),
			}
			if w.storeEnabled {
				if err := w.handleTaskError(context.WithoutCancel(ctx), anyTask.Id, err, taskErr, insertOpts, anyTask.Retried); err != nil {
					return err
				}
			}
			return fmt.Errorf("failed to process task: %w", err)
		}

		if w.storeEnabled {
			err = w.store.UpdateTaskStatus(context.WithoutCancel(ctx), anyTask.Id, TaskStatusSuccess)
			if err != nil {
				return fmt.Errorf("failed to update task status: %w", err)
			}
		}

		return nil
	}

	// Apply snooze middleware to the base handler
	snoozeMw := snoozeMw(w.client.transport, w.log, w.storeEnabled, w.store)
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

func (w *TaskService) handleTaskError(ctx context.Context, taskID string, err error, taskErr TaskError, opts *InsertOpts, retries int) error {
	//slog.Error("handleTaskError", "taskID", taskID, "err", err, "taskErr", taskErr, "attempt", attempt, "opts", opts)
	var retryErr *jobSnoozeError
	if errors.As(err, &retryErr) {
		if err := w.store.UpdateTaskSnoozedTask(ctx, taskID, time.Now().Add(retryErr.duration)); err != nil {
			return fmt.Errorf("failed to update snoozed task: %w", err)
		}
		return nil
	}

	if err := w.store.AddTaskError(ctx, taskID, taskErr); err != nil {
		return fmt.Errorf("failed to add task error: %w", err)
	}

	newStatus := TaskStatusFailed

	if opts.MaxRetries > 0 && retries < opts.MaxRetries {
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

func (t *TaskService) UseSend(middlewares ...Middleware) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if t.handlersAdded {
		panic("cannot add middleware after handlers are added")
	}
	t.client.Use(middlewares...)
}

// TaskService is a list of available task handlers. A TaskHandler must be registered for
// each type of Task to be handled.
//
// Use the top-level AddTaskHandler function combined with a TaskService to register a
// task handler.

// HandleEvent processes a CloudEvent with all registered middleware
func (w *TaskService) HandleEvent(ctx context.Context, ce cloudevents.Event) error {
	w.log.Debug("handling event", "type", ce.Type(), "source", ce.Source(), "id", ce.ID())
	h, ok := w.handlersMap[ce.Type()]
	if !ok {
		return fmt.Errorf("no handler registered for task type: %s", ce.Type())
	}
	return h.handler(ctx, ce)
}

type EventFanoutArgs struct {
	Handlers  []string
	EventType string
	Payload   any
}

func (e EventFanoutArgs) Kind() string {
	return "_UptaskFanoutTask"
}

type EventFanoutWorker struct {
	c *TaskClient
	TaskHandlerDefaults[EventFanoutArgs]
}

type TaskEvent struct {
	PayloadData any
	EventType   string
}

func (e TaskEvent) Kind() string {
	return e.EventType
}

func (e TaskEvent) Payload() any {
	return e.PayloadData
}

type TaskEventGen[T Event] struct {
	Event       T
	HandlerName string
}

func (e TaskEventGen[T]) Kind() string {
	return fmt.Sprintf("%s/%s", e.HandlerName, e.Event.Kind())
}

func (w *EventFanoutWorker) ProcessTask(ctx context.Context, task *Container[EventFanoutArgs]) error {
	var errGroup errgroup.Group
	for _, handlerKind := range task.Args.Handlers {
		errGroup.Go(func() error {
			eventTask := TaskEvent{task.Args.Payload, handlerKind}
			_, err := w.c.StartTask(ctx, eventTask, &InsertOpts{
				MaxRetries:  task.InsertOpts.MaxRetries,
				Queue:       task.InsertOpts.Queue,
				ScheduledAt: task.InsertOpts.ScheduledAt,
				Tags:        task.InsertOpts.Tags,
			})
			return err
		})
	}

	return errGroup.Wait()
}

func (c *TaskService) PublishEvent(ctx context.Context, event Event, opts *InsertOpts) (string, error) {
	var handlers []string
	for k, _ := range c.handlersMap {
		parts := strings.SplitN(k, "/", 2)
		if len(parts) > 1 {
			if event.Kind() == parts[1] {
				handlers = append(handlers, k)
			}
		}
	}
	if handlers == nil {
		return "", fmt.Errorf("no handler registered for event: %s", event.Kind())
	}
	return c.client.StartTask(ctx, EventFanoutArgs{
		Handlers:  handlers,
		EventType: event.Kind(),
		Payload:   event,
	}, opts)
}

func (c *TaskService) StartTask(ctx context.Context, args TaskArgs, opts *InsertOpts) (string, error) {
	return c.client.StartTask(ctx, args, opts)
}
