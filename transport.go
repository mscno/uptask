package uptask

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Transport interface {
	Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error
}

type TransportFn func(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error

func (f TransportFn) Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error {
	return f(ctx, event, opts)
}
