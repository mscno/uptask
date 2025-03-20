package uptask

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/events"
	"log/slog"
	"os"
	"sync"
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
	mux          sync.Mutex
	middlewares  []Middleware
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

func (t *TaskClient) Use(middlewares ...Middleware) {
	t.mux.Lock()
	defer t.mux.Unlock()
	t.middlewares = append(t.middlewares, middlewares...)
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

	// Apply all middleware to the base handler
	//handler := baseHandler

	c.log.Info("enqueueing task", "task", ce.Type(), "id", ce.ID())

	if c.storeEnabled {
		err := c.store.CreateTaskExecution(ctx, &TaskExecution{
			ID:              ce.ID(),
			TaskKind:        ce.Type(),
			Status:          TaskStatusPending,
			Args:            args,
			AttemptID:       "",
			Retried:         0,
			MaxRetries:      opts.MaxRetries,
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

	var handler HandlerFunc = func(ctx context.Context, event cloudevents.Event) error {
		err = c.transport.Send(ctx, ce, opts)
		if err != nil {
			if c.storeEnabled {
				go func() {
					c.log.Debug("cleaning up and deleting task", "task", ce.ID())
					err := c.store.DeleteTaskExecution(context.Background(), ce.ID())
					if err != nil {
						c.log.Error("failed to cleanup and delete task", "task", ce.ID(), "error", err)
					}
				}()
			}
			return err
		}
		return nil
	}

	for i := len(c.middlewares) - 1; i >= 0; i-- {
		handler = c.middlewares[i](handler)
	}

	err = handler(ctx, ce)
	if err != nil {
		return "", fmt.Errorf("failed to send task: %w", err)
	}

	c.log.Debug("task enqueued", "task", ce.ID(), "kind", ce.Type(), "args", args)
	return ce.ID(), nil
}
