package uptask

import (
	"context"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TaskHandler is an interface that can perform a task with args of type T. A typical
// implementation will be a JSON-serializable `TaskArgs` struct that implements
// `Kind()`, along with a TaskHandler that embeds TaskHandlerDefaults and implements `ProcessTask()`.
// TaskHandlers may optionally override other methods to provide task-specific
// configuration for all tasks of that type:
//
//	type SleepArgs struct {
//		Duration time.Duration `json:"duration"`
//	}
//
//	func (SleepArgs) Kind() string { return "sleep" }
//
//	type SleepTaskHandler struct {
//		TaskHandlerDefaults[SleepArgs]
//	}
//
//	func (w *SleepTaskHandler) ProcessTask(ctx context.Context, task *Task[SleepArgs]) error {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		case <-time.After(task.Args.Duration):
//			return nil
//		}
//	}
//
// In addition to fulfilling the TaskHandler interface, task handlers must be registered
// with the service using the AddTaskHandler function.
type TaskHandler[T TaskArgs] interface {
	// NextRetry calculates when the next retry for a failed task should take
	// place given when it was last attempted and its number of attempts, or any
	// other of the task's properties a user-configured retry policy might want
	// to consider.
	//
	// Note that this method on a task handler overrides any service-level retry policy.
	// To use the service-level retry policy, return an empty `time.Time{}` or
	// include TaskHandlerDefaults to do this for you.
	//NextRetry(task *Task[T]) time.Time

	// Timeout is the maximum amount of time the task is allowed to run before
	// its context is cancelled. A timeout of zero (the default) means the task
	// will inherit the Service-level timeout. A timeout of -1 means the task's
	// context will never time out.
	Timeout(task *Task[T]) time.Duration

	// ProcessTask performs the task and returns an error if the task failed. The context
	// will be configured with a timeout according to the task handler settings and may
	// be cancelled for other reasons.
	//
	// If no error is returned, the task is assumed to have succeeded and will be
	// marked completed.
	//
	// It is important for any task handler to respect context cancellation to enable
	// the service to respond to shutdown requests; there is no way to cancel a
	// running task that does not respect context cancellation, other than
	// terminating the process.
	ProcessTask(ctx context.Context, task *Task[T]) error
}

// TaskHandlerDefaults is an empty struct that can be embedded in your task handler
// struct to make it fulfill the TaskHandler interface with default values.
type TaskHandlerDefaults[T TaskArgs] struct{}

// NextRetry returns an empty time.Time{} to avoid setting any task or
// TaskHandler-specific overrides on the next retry time. This means that the
// Service-level retry policy schedule will be used instead.
func (w TaskHandlerDefaults[T]) NextRetry(*Task[T]) time.Time { return time.Time{} }

// Timeout returns the task-specific timeout. Override this method to set a
// task-specific timeout, otherwise the Service-level timeout will be applied.
func (w TaskHandlerDefaults[T]) Timeout(*Task[T]) time.Duration { return time.Minute * 60 }

// AddTaskHandler registers a TaskHandler on the provided TaskService bundle. Each TaskHandler must
// be registered so that the Service knows it should handle a specific kind of
// task (as returned by its `Kind()` method).
//
// Use by explicitly specifying a TaskArgs type and then passing an instance of a
// task handler for the same type:
//
//	taskservice.AddTaskHandler(taskservice, &SortTaskHandler{})
//
// Note that AddTaskHandler can panic in some situations, such as if the task handler is
// already registered or if its configuration is otherwise invalid. This default
// probably makes sense for most applications because you wouldn't want to start
// an application with invalid hardcoded runtime configuration. If you want to
// avoid panics, use AddTaskHandlerSafely instead.
func AddTaskHandler[T TaskArgs](service *TaskService, handler TaskHandler[T]) {
	if err := AddTaskHandlerSafely[T](service, handler); err != nil {
		panic(err)
	}
}

// AddTaskHandlerSafely registers a task handler on the provided TaskService bundle. Unlike AddTaskHandler,
// AddTaskHandlerSafely does not panic and instead returns an error if the task handler
// is already registered or if its configuration is invalid.
//
// Use by explicitly specifying a TaskArgs type and then passing an instance of a
// task handler for the same type:
//
//	taskservice.AddTaskHandlerSafely[SortArgs](service, &SortTaskHandler{}).
func AddTaskHandlerSafely[T TaskArgs](service *TaskService, handler TaskHandler[T]) error {
	var taskArgs T
	return service.add(taskArgs, &taskUnitFactoryWrapper[T]{tasker: handler})
}

type tasker[T TaskArgs] interface {
	Timeout(task *Task[T]) time.Duration
	ProcessTask(ctx context.Context, job *Task[T]) error
}

// processTaskFunc implements TaskArgs and is used to wrap a function given to ProcessTaskFunc.
type processTaskFunc[T TaskArgs] struct {
	TaskHandlerDefaults[T]
	kind string
	f    func(context.Context, *Task[T]) error
}

func (tf *processTaskFunc[T]) Kind() string {
	return tf.kind
}

func (tf *processTaskFunc[T]) ProcessTask(ctx context.Context, task *Task[T]) error {
	return tf.f(ctx, task)
}

// ProcessTaskFunc wraps a function to implement the TaskHandler interface. A task args
// struct implementing TaskArgs will still be required to specify a Kind.
//
// For example:
//
//	taskservice.AddTaskHandler(service, taskservice.ProcessTaskFunc(func(ctx context.Context, task *taskservice.Task[ProcessTaskFuncArgs]) error {
//		fmt.Printf("Message: %s", task.Args.Message)
//		return nil
//	}))
func ProcessTaskFunc[T TaskArgs](f func(context.Context, *Task[T]) error) TaskHandler[T] {
	return &processTaskFunc[T]{f: f, kind: (*new(T)).Kind()}
}

// workUnitFactoryWrapper wraps a Worker to implement workUnitFactory.
type taskUnitFactoryWrapper[T TaskArgs] struct {
	tasker tasker[T]
}

func (w *taskUnitFactoryWrapper[T]) MakeUnit(ce cloudevents.Event) taskUnit {
	return &wrapperTaskUnit[T]{ce: ce, tasker: w.tasker}
}

// wrapperTaskUnit implements taskUnit for a task and Worker.
type wrapperTaskUnit[T TaskArgs] struct {
	ce     cloudevents.Event
	task   *Task[T] // not set until after UnmarshalJob is invoked
	tasker tasker[T]
}

func (w *wrapperTaskUnit[T]) Timeout() time.Duration { return w.tasker.Timeout(w.task) }
func (w *wrapperTaskUnit[T]) ProcessTask(ctx context.Context) error {
	return executeWithTimeout(ctx, w.tasker, w.task)
}

func (w *wrapperTaskUnit[T]) ExtractJob() *Task[T] {
	return w.task
}

func executeWithTimeout[T TaskArgs](ctx context.Context, handler TaskHandler[T], task *Task[T]) error {
	timeout := handler.Timeout(task)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return handler.ProcessTask(ctx, task)
}

func (w *wrapperTaskUnit[T]) UnmarshalTask() (*AnyTask, *TaskInsertOpts, error) {
	insertOpts := new(TaskInsertOpts)
	err := insertOpts.FromCloudEvent(w.ce)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract task insert options: %w", err)
	}

	w.task, err = unmarshalTask[T](w.ce)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	anytask := &AnyTask{
		Id:              w.task.Id,
		CreatedAt:       w.task.CreatedAt,
		Attempt:         w.task.Attempt,
		Retried:         w.task.Retried,
		Args:            w.task.Args,
		Scheduled:       w.task.Scheduled,
		QstashMessageId: w.task.qstashMessageId,
		ScheduleId:      w.task.scheduleId,
	}

	return anytask, insertOpts, nil
}
