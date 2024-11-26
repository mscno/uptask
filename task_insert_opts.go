package uptask

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/events"
	"strconv"
	"time"
)

type TaskInsertOpts struct {
	MaxRetries  int
	Queue       string
	ScheduledAt time.Time
	Tags        []string
}

func (o *TaskInsertOpts) FromCloudEvent(ce cloudevents.Event) error {
	opts, err := insertInsertOptsFromEvent(ce)
	if err != nil {
		return err
	}
	*o = *opts
	return nil
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
	if err = parseExtension(events.TaskQueueExtension, func(s string) error {
		opts.Queue = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("queue parsing error: %w", err)
	}

	if err = parseExtension(events.TaskNotBeforeExtension, func(s string) error {
		scheduledAt, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid scheduled time format: %w", err)
		}
		opts.ScheduledAt = time.Unix(scheduledAt, 0)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scheduled time parsing error: %w", err)
	}

	if err = parseExtension(events.TaskMaxRetriesExtension, func(s string) error {
		maxRetries, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid max retries format: %w", err)
		}
		opts.MaxRetries = maxRetries
		if opts.MaxRetries == 0 {
			opts.MaxRetries = 3
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("max retries parsing error: %w", err)
	}

	return &opts, nil
}
