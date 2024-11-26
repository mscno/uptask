package httputil

import (
	"crypto/sha256"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/mscno/uptask/internal/events"
	"net/http"
)

const (
	upstashAttemptHeader     = "Upstash-Retried"
	upstashScheduledIdHeader = "Upstash-Schedule-Id"
	upstashMessageIdHeader   = "Upstash-Message-Id"
)

func NewEventFromHTTPRequest(r *http.Request) (cloudevents.Event, error) {
	ce, err := cloudevents.NewEventFromHTTPRequest(r)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to parse cloudevent from uptaskhttp request: %w", err)
	}

	// If the event ID is nil, we need to create a stable UUID from the message ID
	// and set the source to "upstash".
	// This happens when the event originates from an Upstash scheduled task.
	var scheduled bool
	if id, err := uuid.Parse(ce.ID()); err == nil && id == uuid.Nil {
		if msgIdHeader := r.Header.Get(upstashMessageIdHeader); msgIdHeader != "" {
			ce.SetID(stableUUID(msgIdHeader).String())
			ce.SetSource("upstash")
			scheduled = true
		}
	}

	// Set the scheduled task extension if the task was scheduled.
	ce.SetExtension(events.ScheduledTaskExtension, fmt.Sprintf("%t", scheduled))

	// Extract the upstash attempt number from the request header and set it as an extension.
	var retried string
	if retried = r.Header.Get(upstashAttemptHeader); retried == "" {
		retried = "0"
	}
	ce.SetExtension(events.TaskRetriedExtension, retried)

	return *ce, nil
}

func stableUUID(input string) uuid.UUID {
	hash := sha256.Sum256([]byte(input))
	fmt.Println("stableUUID", input, uuid.Must(uuid.FromBytes(hash[:16])).String())
	// Use the first 16 bytes to create a UUID
	return uuid.Must(uuid.FromBytes(hash[:16]))
}
