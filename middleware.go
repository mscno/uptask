package uptask

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Middleware defines a function that wraps a HandlerFunc
type Middleware func(HandlerFunc) HandlerFunc

func (f HandlerFunc) HandleEvent(ctx context.Context, event cloudevents.Event) error {
	return f(ctx, event)
}

type HandlerMiddlewareFunc func(handler Handler) Handler

func applyMiddleware(middlewares []func(Handler) Handler, handler Handler) Handler {
	for i := range middlewares {
		handler = middlewares[len(middlewares)-1-i](handler)
	}
	return handler
}
