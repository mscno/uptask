package uptaskhttp

import (
	"encoding/json"
	"github.com/mscno/uptask"
	"github.com/upstash/qstash-go"
	"net/http"
	"strconv"
)

type taskApi struct {
	client *uptask.TaskClient
	store  uptask.TaskStore
	*qstash.Client
}

func NewTaskApi(client *uptask.TaskClient) *taskApi {
	return &taskApi{client: client}
}

type ListTasksResponse struct {
	Tasks []*uptask.TaskExecution `json:"tasks"`
}

func (a *taskApi) ListTasks(w http.ResponseWriter, r *http.Request) {
	var err error
	var limit = 100
	limitQueryParam := r.URL.Query().Get("limit")
	if limitQueryParam != "" {
		limit, err = strconv.Atoi(limitQueryParam)
		if err != nil {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
	}

	tasks, err := a.store.GetMostRecentTaskExecutions(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := ListTasksResponse{Tasks: tasks}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
