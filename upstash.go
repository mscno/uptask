package uptask

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2"
	"github.com/samber/oops"
	"github.com/upstash/qstash-go"
	"log/slog"
	"time"
)

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

func (c *UpstashTransport) Send(ctx context.Context, ce v2.Event, opts *TaskInsertOpts) error {
	targetUrl := fmt.Sprintf("%s/%s", upstashBaseUrl, c.targetUrl)
	if opts.Queue != "" {
		targetUrl = fmt.Sprintf("%s/%s/%s", upstashQueueUrl, opts.Queue, c.targetUrl)
	}
	transportFn := newHttpTransport(targetUrl, "Authorization", fmt.Sprintf("Bearer %s", c.qstashToken))
	return transportFn.Send(ctx, ce, opts)
}

func newHttpTransport(targetUrl string, headers ...string) Transport {
	if len(headers)%2 != 0 {
		panic("headers must be key-value pairs")
	}
	return TransportFn(func(ctx context.Context, ce v2.Event, opts *TaskInsertOpts) error {
		targetUrlWithPath := fmt.Sprintf("%s", targetUrl)
		ctx = v2.ContextWithTarget(ctx, targetUrlWithPath)
		ctx = v2.WithEncodingStructured(ctx)
		slog.Info("Dispatching task to target", "target", targetUrlWithPath, "task", ce.Type())
		headerOptions := make([]v2.HTTPOption, 0)
		for i := 0; i < len(headers); i += 2 {
			headerOptions = append(headerOptions, v2.WithHeader(headers[i], headers[i+1]))
		}

		if opts.MaxRetries >= 0 {
			headerOptions = append(headerOptions, v2.WithHeader("Upstash-Retries", fmt.Sprintf("%d", opts.MaxRetries)))
			ce.SetExtension(taskMaxRetriesExtension, fmt.Sprintf("%d", opts.MaxRetries))
		}

		if !opts.ScheduledAt.IsZero() {
			if opts.ScheduledAt.Before(time.Now()) {
				return fmt.Errorf("scheduled time must be in the future")
			}
			headerOptions = append(headerOptions, v2.WithHeader("Upstash-Not-Before", fmt.Sprintf("%d", opts.ScheduledAt.Unix())))
			ce.SetExtension(taskNotBeforeExtension, fmt.Sprintf("%d", opts.ScheduledAt.Unix()))
		}

		p, err := v2.NewHTTP(
			headerOptions...,
		)
		if err != nil {
			return oops.Wrap(err)
		}

		transport, err := v2.NewClient(
			p,
			v2.WithTimeNow(),
			v2.WithUUIDs(),
		)
		if err != nil {
			return oops.Wrap(err)
		}

		res := transport.Send(ctx, ce)
		if v2.IsUndelivered(res) {
			return oops.In("taskserver").
				Tags("StartTask", "failed to send task").
				With("event", ce).
				Wrap(res)
		}
		return nil
	})
}
