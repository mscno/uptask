package uptask

import "time"

type TaskArgs interface {
	// Kind is a string that uniquely identifies the type of job. This must be
	// provided on your job arguments struct.
	Kind() string
}

type Task[T any] struct {
	Timestamp time.Time
	Args      T
}
