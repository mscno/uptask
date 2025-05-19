package uptask

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/events"
	"strconv"
	"time"
)

type TaskArgs interface {
	// Kind is a string that uniquely identifies the type of job. This must be
	// provided on your job arguments struct.
	Kind() string
}

type Container[T any] struct {
	Id              string
	qstashMessageId string
	scheduleId      string
	CreatedAt       time.Time
	InsertOpts      InsertOpts
	Retried         int
	Scheduled       bool
	Args            T
}

type AnyTask struct {
	Id        string
	CreatedAt time.Time
	//MaxRetries      int
	//ScheduledAt     time.Time
	//Queue           string
	//Tags            []string
	Retried         int
	Scheduled       bool
	ScheduleId      string
	QstashMessageId string
	Args            interface{}
}

func unmarshalTask[T any](ce cloudevents.Event) (*Container[T], error) {
	// Create a new task with the event ID and time.
	var task = Container[T]{
		Id:        ce.ID(),
		CreatedAt: ce.Time(),
	}

	// Extract the task retried number from the event and set it on the task.
	var retried string
	err := ce.ExtensionAs(events.TaskRetriedExtension, &retried)
	if err != nil {
		return nil, fmt.Errorf("failed to get task retried extension: %w", err)
	}
	task.Retried, _ = strconv.Atoi(retried)

	var scheduleId string
	ce.ExtensionAs(events.ScheduleIdExtension, &scheduleId)
	task.scheduleId = scheduleId

	var upstashMessageId string
	err = ce.ExtensionAs(events.QstashMessageIdExtension, &upstashMessageId)
	if err != nil {
		return nil, fmt.Errorf("failed to get task qstash message ID extension: %w", err)
	}
	task.qstashMessageId = upstashMessageId

	// Extract the task scheduled extension from the event and set it on the task.
	var scheduled string
	err = ce.ExtensionAs(events.ScheduledTaskExtension, &scheduled)
	if err != nil {
		return nil, fmt.Errorf("failed to get task scheduled extension: %w", err)
	}
	task.Scheduled, _ = strconv.ParseBool(scheduled)

	opts, err := insertInsertOptsFromEvent(ce)
	if err != nil {
		return nil, fmt.Errorf("failed to parse insertOpts from event: %w", err)
	}

	task.InsertOpts = opts

	err = events.Deserialize(ce, &task.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize task: %w", err)
	}

	return &task, nil
}
