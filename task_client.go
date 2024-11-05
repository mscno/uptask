package uptask

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/events"
	"github.com/samber/oops"
	"github.com/upstash/qstash-go"
)

type TaskInsertOpts struct {
	MaxRetries  int
	Queue       string
	ScheduledAt time.Time
	Tags        []string
}

type TaskStarter interface {
	StartTask(ctx context.Context, args TaskArgs, opts *TaskInsertOpts) error
}

type TaskClient struct {
	transport Transport
}

type Transport interface {
	Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error
}

func NewTaskClient(transport Transport) *TaskClient {
	return &TaskClient{
		transport: transport,
	}
}

func (c *TaskClient) StartTask(ctx context.Context, args TaskArgs, opts *TaskInsertOpts) (string, error) {
	if opts == nil {
		opts = &TaskInsertOpts{MaxRetries: 3}
	}
	ce, err := events.SerializeWithExt(ctx, args, taskRetriedExtension, "0")
	if err != nil {
		return "", oops.Wrap(err)
	}
	slog.Info("enqueueing task", "task", ce.Type())

	err = c.transport.Send(ctx, ce, opts)
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
const upstashQueueUrl = "https://qstash.upstash.io/v2/enqueue"

func NewUpstashTransport(qstashToken, targetUrl string) *UpstashTransport {
	return &UpstashTransport{
		Client:      qstash.NewClient(qstashToken),
		qstashToken: qstashToken,
		targetUrl:   targetUrl,
	}
}

type HeaderOption func(*cloudevents.Event)

func (c *UpstashTransport) Send(ctx context.Context, ce cloudevents.Event, opts *TaskInsertOpts) error {
	targetUrl := fmt.Sprintf("%s/%s", upstashBaseUrl, c.targetUrl)
	if opts.Queue != "" {
		targetUrl = fmt.Sprintf("%s/%s/%s", upstashQueueUrl, opts.Queue, c.targetUrl)
	}
	transportFn := newHttpTransport(targetUrl, "Authorization", fmt.Sprintf("Bearer %s", c.qstashToken))
	return transportFn.Send(ctx, ce, opts)
}

type TransportFn func(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error

func (f TransportFn) Send(ctx context.Context, event cloudevents.Event, opts *TaskInsertOpts) error {
	return f(ctx, event, opts)
}

func newHttpTransport(targetUrl string, headers ...string) Transport {
	if len(headers)%2 != 0 {
		panic("headers must be key-value pairs")
	}
	return TransportFn(func(ctx context.Context, ce cloudevents.Event, opts *TaskInsertOpts) error {
		targetUrlWithPath := fmt.Sprintf("%s", targetUrl)
		ctx = cloudevents.ContextWithTarget(ctx, targetUrlWithPath)
		ctx = cloudevents.WithEncodingStructured(ctx)
		slog.Info("Dispatching task to target", "target", targetUrlWithPath, "task", ce.Type())
		headerOptions := make([]cloudevents.HTTPOption, 0)
		for i := 0; i < len(headers); i += 2 {
			headerOptions = append(headerOptions, cloudevents.WithHeader(headers[i], headers[i+1]))
		}

		if opts.MaxRetries >= 0 {
			headerOptions = append(headerOptions, cloudevents.WithHeader("Upstash-Retries", fmt.Sprintf("%d", opts.MaxRetries)))
			ce.SetExtension(taskMaxRetriesExtension, fmt.Sprintf("%d", opts.MaxRetries))
		}

		if !opts.ScheduledAt.IsZero() {
			if opts.ScheduledAt.Before(time.Now()) {
				return oops.In("taskserver").
					Tags("StartTask", "invalid scheduled time").
					With("time", opts.ScheduledAt).
					Errorf("scheduled time must be in the future")
			}
			headerOptions = append(headerOptions, cloudevents.WithHeader("Upstash-Not-Before", fmt.Sprintf("%d", opts.ScheduledAt.Unix())))
			ce.SetExtension(taskNotBeforeExtension, fmt.Sprintf("%d", opts.ScheduledAt.Unix()))
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

func insertInsertOptsFromEvent(ce cloudevents.Event) (*TaskInsertOpts, error) {
	opts := TaskInsertOpts{}
	exts := ce.Extensions()

	// Helper function combines retrieval, type assertion and parsing
	parseExtension := func(key string, parser func(string) error) error {
		if v, ok := exts[key]; ok {
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("invalid type for %s: expected string, got %T", key, v)
			}
			if str == "" {
				return nil
			}
			return parser(str)
		}
		return nil
	}

	// Parse all extensions using a single error variable
	var err error
	if err = parseExtension(taskQueueExtension, func(s string) error {
		opts.Queue = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("queue parsing error: %w", err)
	}

	if err = parseExtension(taskNotBeforeExtension, func(s string) error {
		scheduledAt, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid scheduled time format: %w", err)
		}
		opts.ScheduledAt = time.Unix(scheduledAt, 0)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scheduled time parsing error: %w", err)
	}

	if err = parseExtension(taskMaxRetriesExtension, func(s string) error {
		maxRetries, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid max retries format: %w", err)
		}
		opts.MaxRetries = maxRetries
		return nil
	}); err != nil {
		return nil, fmt.Errorf("max retries parsing error: %w", err)
	}

	return &opts, nil
}
