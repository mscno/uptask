package httputil

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/mscno/uptask/internal/events"
)

const (
	upstashRetriesHeader     = "Upstash-Retries"
	upstashRetriedHeader     = "Upstash-Retried"
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

	// Set schedule ID if available
	if scheduleID := r.Header.Get(upstashScheduledIdHeader); scheduleID != "" {
		events.SetScheduleID(ce, scheduleID)
	}

	// Set message ID if available
	if messageID := r.Header.Get(upstashMessageIdHeader); messageID != "" {
		events.SetQstashMessageID(ce, messageID)
	}

	// Set scheduled flag
	events.SetScheduled(ce, scheduled)

	// Extract and set retry information
	retried := r.Header.Get(upstashRetriedHeader)
	if retried == "" {
		retried = "0"
	}

	// TODO Verify this retried number
	// Extract retried from qstash headers.
	retriedInt := 0
	if r, err := strconv.Atoi(retried); err == nil {
		retriedInt = r
	}
	// Check for preexisting retried in the task. This should take precedence.
	events.SetRetried(ce, retriedInt)
	if retried, ok := events.GetRetried(ce); ok {
		events.SetRetried(ce, retried)
	}

	// Set max retries if available
	if maxRetries := r.Header.Get(upstashRetriesHeader); maxRetries != "" {
		maxRetriesInt := 0
		if mr, err := strconv.Atoi(maxRetries); err == nil {
			maxRetriesInt = mr
		}
		events.SetMaxRetries(ce, maxRetriesInt)
	}

	return *ce, nil
}

func stableUUID(input string) uuid.UUID {
	hash := sha256.Sum256([]byte(input))
	// Use the first 16 bytes to create a UUID
	return uuid.Must(uuid.FromBytes(hash[:16]))
}
