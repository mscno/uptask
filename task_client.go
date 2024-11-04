package uptask

import (
	"context"
	"fmt"
	"log/slog"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/events"
	"github.com/samber/oops"
	"github.com/upstash/qstash-go"
)

const TaskRoute = "/background-tasks"

type TaskStarter interface {
	StartTask(ctx context.Context, args TaskArgs) error
}

type TaskClient struct {
	transport Transport
}

type Transport interface {
	Send(ctx context.Context, event cloudevents.Event) error
}

func NewTaskClient(transport Transport) *TaskClient {
	return &TaskClient{
		transport: transport,
	}
}

func (c *TaskClient) StartTask(ctx context.Context, args TaskArgs) (string, error) {
	ce, err := events.Serialize(ctx, args)
	if err != nil {
		return "", oops.Wrap(err)
	}
	slog.Info("enqueueing task", "task", ce.Type())

	err = c.transport.Send(ctx, ce)
	if err != nil {
		return "", oops.Wrap(err)
	}
	return ce.ID(), nil
}

/*
	Upstash Transport
*/

type UpstashTransport struct {
	*qstash.Client
	qstashToken string
	targetUrl   string
}

const upstashBaseUrl = "https://qstash.upstash.io/v2/publish"

func NewUpstashTransport(qstashToken, targetUrl string) *UpstashTransport {
	return &UpstashTransport{
		Client:      qstash.NewClient(qstashToken),
		qstashToken: qstashToken,
		targetUrl:   targetUrl,
	}
}

func (c *UpstashTransport) Send(ctx context.Context, ce cloudevents.Event) error {
	targetUrl := fmt.Sprintf("%s/%s", upstashBaseUrl, c.targetUrl)
	transportFn := NewHttpTransport(targetUrl, "Authorization", fmt.Sprintf("Bearer %s", c.qstashToken))
	return transportFn.Send(ctx, ce)
}

type TransportFn func(ctx context.Context, event cloudevents.Event) error

func (f TransportFn) Send(ctx context.Context, event cloudevents.Event) error {
	return f(ctx, event)
}

func NewHttpTransport(targetUrl string, headers ...string) Transport {
	return TransportFn(func(ctx context.Context, ce cloudevents.Event) error {
		targetUrlWithPath := fmt.Sprintf("%s%s", targetUrl, TaskRoute)
		ctx = cloudevents.ContextWithTarget(ctx, targetUrlWithPath)
		ctx = cloudevents.WithEncodingStructured(ctx)
		slog.Info("Dispatching task to target", "target", targetUrlWithPath, "task", ce.Type())
		headerOptions := make([]cloudevents.HTTPOption, 0)
		for i := 0; i < len(headers); i += 2 {
			headerOptions = append(headerOptions, cloudevents.WithHeader(headers[i], headers[i+1]))
		}

		p, err := cloudevents.NewHTTP(
			headerOptions...,
		)
		if err != nil {
			return oops.Wrap(err)
		}
		transport, err := cloudevents.NewClient(
			p,
			cloudevents.WithTimeNow(),
			cloudevents.WithUUIDs(),
		)
		if err != nil {
			return oops.Wrap(err)
		}

		res := transport.Send(ctx, ce)
		if cloudevents.IsUndelivered(res) {
			return oops.In("taskserver").
				Tags("StartTask", "failed to send task").
				With("event", ce).
				Wrap(res)
		}
		return nil
	})
}

/*
	Local Transport
*/

func DummyTransport() Transport {
	return TransportFn(func(ctx context.Context, ce cloudevents.Event) error {
		slog.Info("dummy transport: printing task to console", "task", ce.Type())
		return nil
	})
}
