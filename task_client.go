package uptask

import (
	"context"
	"fmt"
	"github.com/mscno/uptask/internal/events"
	"log/slog"
	"os"
	"time"
)

type ClientOption func(*TaskClient)

func WithClientLogger(l Logger) ClientOption {
	return func(c *TaskClient) {
		c.log = l
	}
}

func WithClientStore(s TaskStore) ClientOption {
	return func(c *TaskClient) {
		c.store = s
		c.storeEnabled = true
	}
}

type TaskClient struct {
	store        TaskStore
	storeEnabled bool
	log          Logger
	transport    Transport
}

func NewTaskClient(transport Transport, opts ...ClientOption) *TaskClient {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := &TaskClient{
		log:       log,
		transport: transport,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

func (c *TaskClient) StartTask(ctx context.Context, args TaskArgs, opts *TaskInsertOpts) (string, error) {
	if opts == nil {
		opts = &TaskInsertOpts{MaxRetries: 3}
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	ce, err := events.SerializeWithExt(ctx, args, events.TaskRetriedExtension, "0")
	if err != nil {
		return "", fmt.Errorf("failed to serialize task: %v", err)
	}
	c.log.Info("enqueueing task", "task", ce.Type(), "id", ce.ID())

	if c.storeEnabled {
		err = c.store.CreateTaskExecution(ctx, &TaskExecution{
			ID:              ce.ID(),
			TaskKind:        ce.Type(),
			Status:          TaskStatusPending,
			Args:            args,
			AttemptID:       "",
			Attempt:         0,
			MaxAttempts:     opts.MaxRetries,
			QstashMessageID: "",
			ScheduleID:      "",
			CreatedAt:       time.Now(),
			AttemptedAt:     time.Time{},
			ScheduledAt:     opts.ScheduledAt,
			FinalizedAt:     time.Time{},
			Errors:          nil,
			Queue:           opts.Queue,
		})
		if err != nil {
			return "", fmt.Errorf("failed to create task execution: %w", err)
		}
	}

	err = c.transport.Send(ctx, ce, opts)
	if err != nil {
		if c.storeEnabled {
			go func() {
				err := c.store.DeleteTaskExecution(context.Background(), ce.ID())
				if err != nil {
					c.log.Error("failed to cleanup and delete task", "task", ce.ID(), "error", err)
				}
			}()
		}
		return "", fmt.Errorf("failed to send task: %v", err)
	}
	return ce.ID(), nil
}
