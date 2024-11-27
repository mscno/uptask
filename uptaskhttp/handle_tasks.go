package uptaskhttp

import (
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/internal/httputil"
	"log/slog"
	"net/http"
)

func HandleTasks(service *uptask.TaskService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ce, err := httputil.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = service.HandleEvent(r.Context(), ce)
		if err != nil {
			slog.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func HandleDeadletterQueue(service *uptask.TaskService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ce, err := httputil.NewEventFromHTTPRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = service.HandleEvent(r.Context(), ce)
		if err != nil {
			slog.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
