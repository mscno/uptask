package uptask

import (
	"context"
	"time"
)

//
//type Event interface {
//	// Kind is a string that uniquely identifies the type of job. This must be
//	// provided on your job arguments struct.
//	EventType() string
//}
//
//type EventContainer[T any] struct {
//	Id              string
//	QstashMessageId string
//	ScheduleId      string
//	CreatedAt       time.Time
//	MaxRetries      int
//	ScheduledAt     time.Time
//	Queue           string
//	Tags            []string
//	Retried         int
//	Scheduled       bool
//	Args            T
//}
//
//type AnyEvent struct {
//	Id              string
//	QstashMessageId string
//	ScheduleId      string
//	CreatedAt       time.Time
//	MaxRetries      int
//	ScheduledAt     time.Time
//	Queue           string
//	Tags            []string
//	Retried         int
//	Scheduled       bool
//	Args            interface{}
//}

type Event TaskArgs

//func AddEventHandler[T Event](service *TaskService, handlerName string, handler EventHandler[T]) {
//	if err := AddEventHandlerSafely[T](service, handlerName, handler); err != nil {
//		panic(err)
//	}
//}

func AddEventHandler[E Event](service *TaskService, handlerName string, handler EventHandler[E]) {
	if err := AddEventHandlerSafely(service, handlerName, handler); err != nil {
		panic(err)
	}
}

func AddEventHandlerSafely[E Event](service *TaskService, handlerName string, handler EventHandler[E]) error {
	var event E
	var task = TaskEventGen[E]{event, handlerName}
	return service.addTask(task, handlerName, &taskUnitFactoryWrapper[E]{tasker: &taskEventHandler[E]{handler}})
}

type taskEventHandler[E Event] struct {
	h EventHandler[E]
}

// // ServeHTTP calls f(w, r).
func (th *taskEventHandler[E]) ProcessTask(ctx context.Context, taskContainer *Container[E]) error {
	return th.h.ProcessEvent(ctx, taskContainer)
}

func (th *taskEventHandler[E]) Timeout(eventContainer *Container[E]) time.Duration {
	return th.h.Timeout(eventContainer)
}
