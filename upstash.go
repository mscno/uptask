package uptask

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/mscno/uptask/internal/events"
	"time"
)

// UpstashTransport handles communication with Upstash QStash
type UpstashTransport struct {
	qstashToken string
	targetUrl   string
}

const upstashBaseUrl = "https://qstash.upstash.io/v2/publish"
const upstashQueueUrl = "https://qstash.upstash.io/v2/enqueue"

// ErrorCode represents specific error types
type ErrorCode string

const (
	ErrInvalidRequest    ErrorCode = "INVALID_REQUEST"
	ErrTransportCreation ErrorCode = "TRANSPORT_CREATION_FAILED"
	ErrClientCreation    ErrorCode = "CLIENT_CREATION_FAILED"
	ErrDeliveryFailed    ErrorCode = "DELIVERY_FAILED"
	ErrInvalidSchedule   ErrorCode = "INVALID_SCHEDULE"
	ErrBadResponse       ErrorCode = "BAD_RESPONSE"
)

// UpstashTaskError provides detailed information about task operation errors
type UpstashTaskError struct {
	Code      ErrorCode
	Operation string
	Message   string
	Cause     error
	Event     *v2.Event
	Metadata  map[string]interface{}
}

// Error implements the error interface
func (e *UpstashTaskError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %s - %v", e.Code, e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Code, e.Operation, e.Message)
}

// Unwrap provides access to the underlying error
func (e *UpstashTaskError) Unwrap() error {
	return e.Cause
}

// NewUpstashTaskError creates a new UpstashTaskError
func NewUpstashTaskError(code ErrorCode, operation string, message string, cause error) *UpstashTaskError {
	return &UpstashTaskError{
		Code:      code,
		Operation: operation,
		Message:   message,
		Cause:     cause,
		Metadata:  make(map[string]interface{}),
	}
}

// WithEvent adds event data to the error
func (e *UpstashTaskError) WithEvent(event v2.Event) *UpstashTaskError {
	e.Event = &event

	// Add useful extension information to metadata for easier debugging
	if e.Event != nil {
		if events.IsScheduled(e.Event) {
			e.WithMetadata("scheduled", true)
			scheduleID := events.GetScheduleID(e.Event)
			if scheduleID != "" {
				e.WithMetadata("schedule_id", scheduleID)
			}
		}

		retried, _ := events.GetRetried(e.Event)
		maxRetries, _ := events.GetMaxRetries(e.Event)
		if maxRetries > 0 {
			e.WithMetadata("retries", fmt.Sprintf("%d/%d", retried, maxRetries))
		}

		qstashMsgID := events.GetQstashMessageID(e.Event)
		if qstashMsgID != "" {
			e.WithMetadata("qstash_message_id", qstashMsgID)
		}
	}

	return e
}

// WithMetadata adds additional context to the error
func (e *UpstashTaskError) WithMetadata(key string, value interface{}) *UpstashTaskError {
	e.Metadata[key] = value
	return e
}

// NewUpstashTransport creates a new UpstashTransport instance
func NewUpstashTransport(qstashToken, targetUrl string) *UpstashTransport {
	return &UpstashTransport{
		qstashToken: qstashToken,
		targetUrl:   targetUrl,
	}
}

// Send dispatches a CloudEvent to Upstash
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
	return transportFn(func(ctx context.Context, ce v2.Event, opts *TaskInsertOpts) error {
		targetUrlWithPath := fmt.Sprintf("%s", targetUrl)
		ctx = v2.ContextWithTarget(ctx, targetUrlWithPath)
		ctx = v2.WithEncodingStructured(ctx)

		headerOptions := make([]v2.HTTPOption, 0)
		for i := 0; i < len(headers); i += 2 {
			headerOptions = append(headerOptions, v2.WithHeader(headers[i], headers[i+1]))
		}

		if opts.MaxRetries >= 0 {
			// Subtract any previous retries from max retries
			// TODO Verify that this is correct
			if retried, ok := events.GetRetried(&ce); ok {
				opts.MaxRetries = opts.MaxRetries - retried
			}

			headerOptions = append(headerOptions, v2.WithHeader("Upstash-Retries", fmt.Sprintf("%d", opts.MaxRetries)))
			events.SetMaxRetries(&ce, opts.MaxRetries)
		}

		if !opts.ScheduledAt.IsZero() {
			if opts.ScheduledAt.Before(time.Now()) {
				return NewUpstashTaskError(
					ErrInvalidSchedule,
					"ScheduleTask",
					"scheduled time must be in the future",
					nil,
				).WithEvent(ce)
			}
			headerOptions = append(headerOptions, v2.WithHeader("Upstash-Not-Before", fmt.Sprintf("%d", opts.ScheduledAt.Unix())))
			events.SetNotBefore(&ce, opts.ScheduledAt)
		}

		p, err := v2.NewHTTP(headerOptions...)
		if err != nil {
			return NewUpstashTaskError(
				ErrTransportCreation,
				"CreateTransport",
				"failed to create HTTP transport",
				err,
			)
		}

		transport, err := v2.NewClient(
			p,
			v2.WithTimeNow(),
			v2.WithUUIDs(),
		)
		if err != nil {
			return NewUpstashTaskError(
				ErrClientCreation,
				"CreateClient",
				"failed to create HTTP client",
				err,
			)
		}

		deliveryErr := transport.Send(ctx, ce)

		// Handle delivery errors
		if v2.IsUndelivered(deliveryErr) {
			return NewUpstashTaskError(
				ErrDeliveryFailed,
				"SendTask",
				"task is undelivered",
				deliveryErr,
			).WithEvent(ce)
		}

		switch x := deliveryErr.(type) {
		case *http.Result:
			if x.StatusCode >= 400 {
				return NewUpstashTaskError(
					ErrBadResponse,
					"SendTask",
					"task enqueuing failed",
					deliveryErr,
				).WithEvent(ce).WithMetadata("status_code", x.StatusCode)
			}
			return nil
		default:
			if deliveryErr != nil {
				return NewUpstashTaskError(
					ErrDeliveryFailed,
					"SendTask",
					"unexpected delivery error",
					deliveryErr,
				).WithEvent(ce)
			}
			return nil
		}
	})
}
