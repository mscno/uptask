package uptaskhttp

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/httputil"
	"log/slog"
	"net/http"
)

type CloudEventHandler interface {
	HandleEvent(ctx context.Context, event cloudevents.Event) error
}

func HandleTasks(service CloudEventHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ce, err := httputil.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		err = service.HandleEvent(r.Context(), ce)
		if err != nil {
			slog.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

type DlqStorer interface {
	StoreDlqEvent(ce cloudevents.Event) error
}

func HandleDlq(store DlqStorer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		fmt.Printf("Handling Dlq event: %s\n", r.URL.Path)

		ce, err := httputil.NewDlqEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		err = store.StoreDlqEvent(ce)
		if err != nil {
			slog.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		w.WriteHeader(http.StatusOK)
	}
}
