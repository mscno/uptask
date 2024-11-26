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

type Task[T any] struct {
	Id        string
	CreatedAt time.Time
	Attempt   int
	Retried   int
	Scheduled bool
	Args      T
}

type AnyTask struct {
	Id        string
	CreatedAt time.Time
	Attempt   int
	Retried   int
	Scheduled bool
	Args      interface{}
}

func unmarshalTask[T any](ce cloudevents.Event) (*Task[T], error) {
	// Create a new task with the event ID and time.
	var task = Task[T]{
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
	// Set the task attempt number to the retried number plus one.
	task.Attempt = task.Retried + 1

	// Extract the task scheduled extension from the event and set it on the task.
	var scheduled string
	err = ce.ExtensionAs(events.ScheduledTaskExtension, &scheduled)
	if err != nil {
		return nil, fmt.Errorf("failed to get task scheduled extension: %w", err)
	}
	task.Scheduled, _ = strconv.ParseBool(scheduled)

	err = events.Deserialize(ce, &task.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize task: %w", err)
	}

	return &task, nil
}
