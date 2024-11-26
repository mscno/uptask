package uptask

import (
	"context"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"time"
)

func JobSnooze(duration time.Duration) error {
	if duration < 0 {
		panic("JobSnooze: duration must be >= 0")
	}
	return &jobSnoozeError{duration: duration}
}

type jobSnoozeError struct {
	duration time.Duration
}

func (e *jobSnoozeError) Error() string {
	// should not ever be called, but add a prefix just in case:
	return fmt.Sprintf("jobSnoozeError: %s", e.duration)
}

func (e *jobSnoozeError) Is(target error) bool {
	_, ok := target.(*jobSnoozeError)
	return ok
}

func snoozeMw(transport Transport, log Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, ce cloudevents.Event) error {
			err := next(ctx, ce)
			var snoozeErr *jobSnoozeError
			if errors.As(err, &snoozeErr) {
				if snoozeErr.duration > 0 {
					opts, err := insertInsertOptsFromEvent(ce)
					if err != nil {
						return err
					}
					// Requeue the task with a new scheduled time
					log.Info("snoozing task", "duration", snoozeErr.duration, "task", ce.Type(), "id", ce.ID())
					opts.ScheduledAt = time.Now().Add(snoozeErr.duration)
					return transport.Send(ctx, ce, opts)
				}
			}
			return err
		}
	}
}
