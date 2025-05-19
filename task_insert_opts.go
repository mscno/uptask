package uptask

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/events"
	"strconv"
	"time"
)

type InsertOpts struct {
	MaxRetries  int
	Queue       string
	ScheduledAt time.Time
	Tags        []string
}

func (o *InsertOpts) FromCloudEvent(ce cloudevents.Event) error {
	opts, err := insertInsertOptsFromEvent(ce)
	if err != nil {
		return err
	}
	*o = opts
	return nil
}

func insertInsertOptsFromEvent(ce cloudevents.Event) (InsertOpts, error) {
	opts := InsertOpts{}
	//exts := ce.Extensions()

	opts.Queue = events.GetQueue(&ce)
	if scheduledAt, ok := events.GetNotBefore(&ce); ok {
		opts.ScheduledAt = scheduledAt
	}

	if maxRetries, ok := events.GetMaxRetries(&ce); ok {
		opts.MaxRetries = maxRetries
	}

	return opts, nil
}

func retriedFromEvent(ce cloudevents.Event) (int, error) {
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
	var retried int
	if err = parseExtension(events.TaskRetriedExtension, func(s string) error {
		retried, err = strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid retried format: %w", err)
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("retried parsing error: %w", err)
	}

	return retried, nil
}
