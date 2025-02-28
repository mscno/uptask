package events

import (
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// GetStringExtension safely retrieves a string extension
func GetStringExtension(event *cloudevents.Event, key string) (string, bool) {
	if val, ok := event.Extensions()[key]; ok {
		if str, ok := val.(string); ok {
			return str, true
		}
	}
	return "", false
}

// GetIntExtension safely retrieves an int extension
func GetIntExtension(event *cloudevents.Event, key string) (int, bool) {
	if val, ok := event.Extensions()[key]; ok {
		switch v := val.(type) {
		case int:
			return v, true
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

// GetBoolExtension safely retrieves a bool extension
func GetBoolExtension(event *cloudevents.Event, key string) (bool, bool) {
	if val, ok := event.Extensions()[key]; ok {
		switch v := val.(type) {
		case bool:
			return v, true
		case string:
			if b, err := strconv.ParseBool(v); err == nil {
				return b, true
			}
		}
	}
	return false, false
}

// GetTimeExtension safely retrieves a time.Time extension
func GetTimeExtension(event *cloudevents.Event, key string) (time.Time, bool) {
	if val, ok := event.Extensions()[key]; ok {
		switch v := val.(type) {
		case time.Time:
			return v, true
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return t, true
			}
			if scheduledAtTime, err := strconv.ParseInt(v, 10, 64); err == nil {
				return time.Unix(scheduledAtTime, 0), true
			}
		}
	}
	return time.Time{}, false
}

// IsScheduled returns true if the event was scheduled
func IsScheduled(event *cloudevents.Event) bool {
	val, ok := GetBoolExtension(event, ScheduledTaskExtension)
	if !ok {
		return false
	}
	return val
}

// GetRetried returns the number of times this task has been retried
func GetRetried(event *cloudevents.Event) (int, bool) {
	return GetIntExtension(event, TaskRetriedExtension)
}

// GetMaxRetries returns the maximum number of retries for this task
func GetMaxRetries(event *cloudevents.Event) (int, bool) {
	return GetIntExtension(event, TaskMaxRetriesExtension)
}

// GetQueue returns the queue this task is in
func GetQueue(event *cloudevents.Event) string {
	val, ok := GetStringExtension(event, TaskQueueExtension)
	if !ok {
		return "default"
	}
	return val
}

// GetNotBefore returns the time before which this task should not be processed
func GetNotBefore(event *cloudevents.Event) (time.Time, bool) {
	return GetTimeExtension(event, TaskNotBeforeExtension)
}

// GetScheduleID returns the schedule ID for this task
func GetScheduleID(event *cloudevents.Event) string {
	val, ok := GetStringExtension(event, ScheduleIdExtension)
	if !ok {
		return ""
	}
	return val
}

// GetQstashMessageID returns the Qstash message ID for this task
func GetQstashMessageID(event *cloudevents.Event) string {
	val, ok := GetStringExtension(event, QstashMessageIdExtension)
	if !ok {
		return ""
	}
	return val
}

// SetQueue sets the queue for this task
func SetQueue(event *cloudevents.Event, queue string) {
	event.SetExtension(TaskQueueExtension, queue)
}

// SetNotBefore sets the time before which this task should not be processed
func SetNotBefore(event *cloudevents.Event, notBefore time.Time) {
	event.SetExtension(TaskNotBeforeExtension, notBefore.Format(time.RFC3339))
}

// SetRetried sets the number of times this task has been retried
func SetRetried(event *cloudevents.Event, retried int) {
	event.SetExtension(TaskRetriedExtension, strconv.Itoa(retried))
}

// SetMaxRetries sets the maximum number of retries for this task
func SetMaxRetries(event *cloudevents.Event, maxRetries int) {
	event.SetExtension(TaskMaxRetriesExtension, strconv.Itoa(maxRetries))
}

// SetScheduled sets whether this task was scheduled
func SetScheduled(event *cloudevents.Event, scheduled bool) {
	event.SetExtension(ScheduledTaskExtension, strconv.FormatBool(scheduled))
}

// SetScheduleID sets the schedule ID for this task
func SetScheduleID(event *cloudevents.Event, scheduleID string) {
	event.SetExtension(ScheduleIdExtension, scheduleID)
}

// SetQstashMessageID sets the Qstash message ID for this task
func SetQstashMessageID(event *cloudevents.Event, messageID string) {
	event.SetExtension(QstashMessageIdExtension, messageID)
}
