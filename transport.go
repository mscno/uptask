package uptask

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Transport interface {
	Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error
}

type transportFn func(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error

func (f transportFn) Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error {
	return f(ctx, event, opts)
}
