package events

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Handler interface {
	ProcessEvent(context.Context, *cloudevents.Event) error
}

type HandleFunc func(context.Context, *cloudevents.Event) error

// ProcessTask calls fn(ctx, task)
func (fn HandleFunc) ProcessEvent(ctx context.Context, task *cloudevents.Event) error {
	return fn(ctx, task)
}
