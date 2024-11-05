package uptask

import (
	"context"
	"fmt"
	"github.com/mscno/uptask/events"
	"log/slog"
	"os"
)

type TaskClient struct {
	log       Logger
	transport Transport
}

func NewTaskClient(transport Transport) *TaskClient {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &TaskClient{
		log:       log,
		transport: transport,
	}
}

func (c *TaskClient) StartTask(ctx context.Context, args TaskArgs, opts *TaskInsertOpts) (string, error) {
	if opts == nil {
		opts = &TaskInsertOpts{MaxRetries: 3}
	}
	ce, err := events.SerializeWithExt(ctx, args, taskRetriedExtension, "0")
	if err != nil {
		return "", fmt.Errorf("failed to serialize task: %v", err)
	}
	c.log.Info("enqueueing task", "task", ce.Type(), "id", ce.ID())

	err = c.transport.Send(ctx, ce, opts)
	if err != nil {
		return "", fmt.Errorf("failed to send task: %v", err)
	}
	return ce.ID(), nil
}
