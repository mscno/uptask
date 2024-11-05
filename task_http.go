package uptask

import (
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"net/http"
)

const upstashAttemptHeader = "Upstash-Retried"

const taskRetriedExtension = "taskretried"
const taskMaxRetriesExtension = "taskmaxretries"
const taskQueueExtension = "taskqueue"
const taskNotBeforeExtension = "tasknotbefore"

func NewEventFromHTTPRequest(r *http.Request) (cloudevents.Event, error) {
	var retried string
	if retried = r.Header.Get(upstashAttemptHeader); retried == "" {
		retried = "0"
	}

	ce, err := cloudevents.NewEventFromHTTPRequest(r)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to parse cloudevent from http request: %w", err)
	}
	ce.SetExtension(taskRetriedExtension, retried)
	return *ce, nil
}
