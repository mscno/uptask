package httputil

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
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
	upstashDlqId             = "dlqId"
	upstashDlqRetried        = "retried"
	upstashSourceMsgId       = "sourceMessageId"
	upstashDlqMaxRetried     = "maxRetries"
	upstashDlqscheduleId     = "scheduleId"
)

func NewEventFromHTTPRequest(r *http.Request) (cloudevents.Event, error) {
	ce, err := cloudevents.NewEventFromHTTPRequest(r)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to parse cloudevent from uptaskhttp request: %w", err)
	}
	//slog.Debug("upstash header", "headers", r.Header)

	//slog.Debug("extenstions in", "ext", ce.Extensions())
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
	if retriedExist, ok := events.GetRetried(ce); !ok {
		events.SetRetried(ce, retriedInt)
	} else {
		events.SetRetried(ce, retriedExist+retriedInt)
	}

	// Set max retries if available
	if maxRetries := r.Header.Get(upstashRetriesHeader); maxRetries != "" {
		maxRetriesInt := 0
		if mr, err := strconv.Atoi(maxRetries); err == nil {
			maxRetriesInt = mr
		}
		slog.Debug("setting upstash retries header", "max-retries", maxRetries)
		events.SetMaxRetries(ce, maxRetriesInt)
	}

	//slog.Debug("extenstions out", "ext", ce.Extensions())
	return *ce, nil
}

func NewDlqEventFromHTTPRequest(r *http.Request) (cloudevents.Event, error) {
	var v map[string]interface{}

	err := json.NewDecoder(r.Body).Decode(&v)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to parse upstash-dlq event: %w", err)
	}

	dataStr := v["sourceBody"].(string)
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to decdode upstash-dlq event body: %w", err)
	}
	ce := cloudevents.NewEvent(cloudevents.VersionV1)
	err = ce.UnmarshalJSON(data)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to unmarshal cloudevent body: %w", err)
	}
	//slog.Debug("upstash header", "headers", r.Header)

	//slog.Debug("extenstions in", "ext", ce.Extensions())
	// If the event ID is nil, we need to create a stable UUID from the message ID
	// and set the source to "upstash".
	// This happens when the event originates from an Upstash scheduled task.
	var scheduled bool
	if id, err := uuid.Parse(ce.ID()); err == nil && id == uuid.Nil {
		if msgIdHeader, ok := v[upstashSourceMsgId].(string); ok {
			ce.SetID(stableUUID(msgIdHeader).String())
			ce.SetSource("upstash")
			scheduled = true
		}
	}

	// Set schedule ID if available
	if scheduleID, ok := v[upstashScheduledIdHeader].(string); ok {
		events.SetScheduleID(&ce, scheduleID)
	}

	// Set message ID if available
	if messageID, ok := v[upstashSourceMsgId].(string); ok {
		events.SetQstashMessageID(&ce, messageID)
	}

	// Set scheduled flag
	events.SetScheduled(&ce, scheduled)

	// Extract and set retry information
	retried, ok := v[upstashDlqRetried].(float64)
	if ok && retried == 0 {
		retried = 0
	}

	// Extract retried from qstash headers.
	retriedInt := int(retried)
	// Check for preexisting retried in the task. This should take precedence.
	if retriedExist, ok := events.GetRetried(&ce); !ok {
		events.SetRetried(&ce, retriedInt)
	} else {
		events.SetRetried(&ce, retriedExist+retriedInt)
	}

	// Set max retries if available
	if maxRetries, ok := v[upstashDlqMaxRetried].(float64); ok {
		slog.Debug("setting upstash retries header", "max-retries", maxRetries)
		events.SetMaxRetries(&ce, int(maxRetries))
	}

	//slog.Debug("extenstions out", "ext", ce.Extensions())
	return ce, nil
}

func stableUUID(input string) uuid.UUID {
	hash := sha256.Sum256([]byte(input))
	// Use the first 16 bytes to create a UUID
	return uuid.Must(uuid.FromBytes(hash[:16]))
}
