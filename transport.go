package uptask

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Transport interface {
	Send(ctx context.Context, event cloudevents.Event, opts *InsertOpts) error
}

type transportFn func(ctx context.Context, event cloudevents.Event, opts *InsertOpts) error

func (f transportFn) Send(ctx context.Context, event cloudevents.Event, opts *InsertOpts) error {
	return f(ctx, event, opts)
}
