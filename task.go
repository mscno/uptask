package uptask

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/events"
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
	Args      T
}

func UnmarshalTask[T any](ce cloudevents.Event) (*Task[T], error) {
	var task = Task[T]{
		Id:        ce.ID(),
		CreatedAt: ce.Time(),
	}

	var retried string
	err := ce.ExtensionAs(taskRetriedExtension, &retried)
	if err != nil {
		return nil, fmt.Errorf("failed to get task retried extension: %w", err)
	}
	task.Retried, _ = strconv.Atoi(retried)

	task.Attempt = task.Retried + 1

	err = events.Deserialize(ce, &task.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize task: %w", err)
	}

	return &task, nil
}
