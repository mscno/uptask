package uptask

import (
	"context"
	"time"
)

type EventHandler[T Event] interface {
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
	Timeout(task *Container[T]) time.Duration

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
	ProcessEvent(ctx context.Context, task *Container[T]) error
}
