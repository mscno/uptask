package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/testutl"
	"github.com/mscno/uptask/uptaskhttp"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type BookRoom struct {
	RoomId string `json:"room_id"`
}

func (t BookRoom) Kind() string {
	return "BookRoom"
}

type BookRoomWorker struct {
	tsvc *uptask.TaskService
	uptask.TaskHandlerDefaults[BookRoom]
}

func (w *BookRoomWorker) ProcessTask(ctx context.Context, task *uptask.Container[BookRoom]) error {
	fmt.Println("Booking Room")
	_, err := w.tsvc.PublishEvent(ctx, &RoomBooked{RoomId: task.Args.RoomId}, nil)
	return err
}

type RoomBooked struct {
	RoomId string `json:"room_id"`
}

func (task RoomBooked) Kind() string {
	return "RoomBooked"
}

type OrderBeerOnRoomBooked struct {
	wg   *sync.WaitGroup
	beer *uint32
	uptask.TaskHandlerDefaults[RoomBooked]
}

func (w *OrderBeerOnRoomBooked) ProcessEvent(ctx context.Context, event *uptask.Container[RoomBooked]) error {
	defer w.wg.Done()
	atomic.AddUint32(w.beer, 1)
	fmt.Println("OrderBeerOnRoomBooked", event.Args.RoomId)
	return nil
}

type OrderSnacksOnRoomBooked struct {
	wg     *sync.WaitGroup
	snacks *uint32
	uptask.TaskHandlerDefaults[RoomBooked]
}

func (w *OrderSnacksOnRoomBooked) ProcessEvent(ctx context.Context, event *uptask.Container[RoomBooked]) error {
	if v, ok := ctx.Value("user").(string); !ok || v != "admin" {
		fmt.Println("Missing permissions to book snacks")
		return fmt.Errorf("Missing permissions to book snacks")
	}
	defer w.wg.Done()
	atomic.AddUint32(w.snacks, 1)
	fmt.Println("OrderSnacksOnRoomBooked", event.Args.RoomId)
	return nil
}

type OrderCleaningOnRoomBooked struct {
	tsvc *uptask.TaskService
	uptask.TaskHandlerDefaults[RoomBooked]
}

func (w *OrderCleaningOnRoomBooked) ProcessEvent(ctx context.Context, event *uptask.Container[RoomBooked]) error {
	_, err := w.tsvc.StartTask(ctx, BookCleaning{event.Args.RoomId}, nil)
	return err
}

type BookCleaning struct {
	RoomId string `json:"room_id"`
}

func (t BookCleaning) Kind() string {
	return "BookCleaning"
}

type BookCleaningWorker struct {
	wg *sync.WaitGroup
	uptask.TaskHandlerDefaults[BookCleaning]
}

func (w *BookCleaningWorker) ProcessTask(ctx context.Context, event *uptask.Container[BookCleaning]) error {
	if v, ok := ctx.Value("user").(string); !ok || v != "admin" {
		fmt.Println("Missing permissions to book cleaning")
		return fmt.Errorf("Missing permissions to book cleaning")
	}
	fmt.Println("Cleaning booked", event.Args.RoomId)
	defer w.wg.Done()
	return nil
}

func RoomBookFlow(t *testing.T, port int, tunnelUrl string) {

	transport, err := uptask.NewUpstashTransport(testutl.ReadTokenFromEnv(), tunnelUrl)
	require.NoError(t, err)
	tsvc := uptask.NewTaskService(transport)
	tsvc.Use(func(handlerFunc uptask.HandlerFunc) uptask.HandlerFunc {
		return func(ctx context.Context, event cloudevents.Event) error {
			if v, ok := event.Extensions()["user"].(string); ok && v == "admin" {
				ctx = context.WithValue(ctx, "user", "admin")
			}
			return handlerFunc(ctx, event)
		}
	})
	tsvc.UseSend(func(handlerFunc uptask.HandlerFunc) uptask.HandlerFunc {
		return func(ctx context.Context, event cloudevents.Event) error {
			if v, ok := ctx.Value("user").(string); ok {
				event.SetExtension("user", v)
			}
			return handlerFunc(ctx, event)
		}
	})

	handler := http.NewServeMux()
	handler.HandleFunc("POST /", uptaskhttp.HandleTasks(tsvc))

	srv := &http.Server{Handler: handler, Addr: ":" + strconv.Itoa(port)}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			if err != http.ErrServerClosed {
				slog.Error(err.Error())
			}
		}
	}()
	t.Cleanup(func() {
		fmt.Println("Shutting down server")
		err := srv.Shutdown(context.Background())
		if err != nil {
			slog.Error(err.Error())
		}
	})

	var snacksWg sync.WaitGroup
	var beerWg sync.WaitGroup
	var cleaningWg sync.WaitGroup
	snacksWg.Add(1)
	beerWg.Add(1)
	cleaningWg.Add(1)

	var snacks uint32
	var beer uint32

	uptask.AddTaskHandler(tsvc, &BookRoomWorker{tsvc: tsvc})
	uptask.AddTaskHandler(tsvc, &BookCleaningWorker{wg: &cleaningWg})
	uptask.AddEventHandler(tsvc, "bookSnacks", &OrderSnacksOnRoomBooked{snacks: &snacks, wg: &snacksWg})
	uptask.AddEventHandler(tsvc, "bookBeer", &OrderBeerOnRoomBooked{beer: &beer, wg: &beerWg})
	uptask.AddEventHandler(tsvc, "bookCleaning", &OrderCleaningOnRoomBooked{tsvc: tsvc})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	ctx = context.WithValue(ctx, "user", "admin")
	_, err = tsvc.StartTask(ctx, BookRoom{RoomId: rand.Text()}, nil)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("test timed out")
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			snacksWg.Wait()
			beerWg.Wait()
			cleaningWg.Wait()
			close(ch)
		}()
		return ch
	}():
		// Task completed successfully
	}

	require.Equal(t, uint32(1), atomic.LoadUint32(&snacks))
	require.Equal(t, uint32(1), atomic.LoadUint32(&beer))

}
