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
	*o = opts
	return nil
}

func insertInsertOptsFromEvent(ce cloudevents.Event) (TaskInsertOpts, error) {
	opts := TaskInsertOpts{}
	//exts := ce.Extensions()

	// Helper function combines retrieval, type assertion and parsing
	//parseExtension := func(key string, parser func(string) error) error {
	//	if v, ok := exts[key]; ok {
	//		str, ok := v.(string)
	//		if !ok {
	//			return fmt.Errorf("invalid type for %s: expected string, got %T", key, v)
	//		}
	//		if str == "" {
	//			return nil
	//		}
	//		return parser(str)
	//	}
	//	return nil
	//}

	// Parse all extensions using a single error variable
	var err error
	//if err = parseExtension(events.TaskQueueExtension, func(s string) error {
	//	opts.Queue = s
	//	return nil
	//}); err != nil {
	//	return TaskInsertOpts{}, fmt.Errorf("queue parsing error: %w", err)
	//}
	if _, ok := ce.Extensions()[events.TaskQueueExtension]; ok {
		err = ce.ExtensionAs(events.TaskQueueExtension, &opts.Queue)
		if err != nil {
			return TaskInsertOpts{}, fmt.Errorf("extension parsing queue error: %w", err)
		}
	}
	//if err = parseExtension(events.TaskNotBeforeExtension, func(s string) error {
	//	scheduledAt, err := strconv.ParseInt(s, 10, 64)
	//	if err != nil {
	//		return fmt.Errorf("invalid scheduled time format: %w", err)
	//	}
	//	opts.ScheduledAt = time.Unix(scheduledAt, 0)
	//	return nil
	//}); err != nil {
	//	return TaskInsertOpts{}, fmt.Errorf("scheduled time parsing error: %w", err)
	//}
	//
	//if err = parseExtension(events.TaskMaxRetriesExtension, func(s string) error {
	//	maxRetries, err := strconv.Atoi(s)
	//	if err != nil {
	//		return fmt.Errorf("invalid max retries format: %w", err)
	//	}
	//	opts.MaxRetries = maxRetries
	//	if opts.MaxRetries == 0 {
	//		opts.MaxRetries = 3
	//	}
	//	return nil
	//}); err != nil {
	//	return TaskInsertOpts{}, fmt.Errorf("max retries parsing error: %w", err)
	//}
	if scheduledAt, ok := events.GetNotBefore(&ce); ok {
		opts.ScheduledAt = scheduledAt
	}
	//if _, ok := ce.Extensions()[events.TaskNotBeforeExtension]; ok {
	//	var scheduledAt string
	//	if err := ce.ExtensionAs(events.TaskNotBeforeExtension, &scheduledAt); err != nil {
	//		return TaskInsertOpts{}, fmt.Errorf("scheduledAt parsing error: %w", err)
	//	}
	//	scheduledAtTime, err := strconv.ParseInt(scheduledAt, 10, 64)
	//	if err != nil {
	//		return TaskInsertOpts{}, fmt.Errorf("invalid scheduled time format: %w", err)
	//	}
	//	opts.ScheduledAt = time.Unix(scheduledAtTime, 0)
	//}
	if maxRetries, ok := events.GetMaxRetries(&ce); ok {
		opts.MaxRetries = maxRetries
	}
	if _, ok := ce.Extensions()[events.TaskMaxRetriesExtension]; ok {
		var maxRetries string
		if err := ce.ExtensionAs(events.TaskMaxRetriesExtension, &maxRetries); err != nil {
			return TaskInsertOpts{}, fmt.Errorf("max retries parsing error: %w", err)
		}
		opts.MaxRetries, _ = strconv.Atoi(maxRetries)
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
