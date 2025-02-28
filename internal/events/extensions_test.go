package events

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestExtensionUtilities(t *testing.T) {
	// Create a test event
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("test")
	event.SetType("com.example.test")
	event.SetData(cloudevents.ApplicationJSON, []byte(`{"hello":"world"}`))

	// Test setting and getting string extension
	SetQueue(&event, "test-queue")
	assert.Equal(t, "test-queue", GetQueue(&event))
	val, ok := GetStringExtension(&event, TaskQueueExtension)
	assert.True(t, ok)
	assert.Equal(t, "test-queue", val)

	// Test setting and getting int extension
	SetRetried(&event, 3)
	retried, _ := GetRetried(&event)
	assert.Equal(t, 3, retried)
	intVal, intOk := GetIntExtension(&event, TaskRetriedExtension)
	assert.True(t, intOk)
	assert.Equal(t, 3, intVal)

	// Test setting and getting bool extension
	SetScheduled(&event, true)
	assert.Equal(t, true, IsScheduled(&event))
	boolVal, boolOk := GetBoolExtension(&event, ScheduledTaskExtension)
	assert.True(t, boolOk)
	assert.Equal(t, true, boolVal)

	// Test setting and getting time extension
	now := time.Now().UTC().Truncate(time.Second)
	SetNotBefore(&event, now)
	scheduledAt, _ := GetNotBefore(&event)
	assert.Equal(t, now, scheduledAt)
	timeVal, timeOk := GetTimeExtension(&event, TaskNotBeforeExtension)
	assert.True(t, timeOk)
	assert.Equal(t, now, timeVal)

	// Test default values
	emptyEvent := cloudevents.NewEvent()
	assert.Equal(t, "default", GetQueue(&emptyEvent))
	retried, _ = GetRetried(&emptyEvent)
	assert.Equal(t, 0, retried)
	assert.Equal(t, false, IsScheduled(&emptyEvent))
	scheduledAt, _ = GetNotBefore(&emptyEvent)
	assert.Equal(t, time.Time{}, scheduledAt)

	// Test missing values return false for second return value
	_, strOk := GetStringExtension(&emptyEvent, TaskQueueExtension)
	assert.False(t, strOk)
	_, intOk2 := GetIntExtension(&emptyEvent, TaskRetriedExtension)
	assert.False(t, intOk2)
	_, boolOk2 := GetBoolExtension(&emptyEvent, ScheduledTaskExtension)
	assert.False(t, boolOk2)
	_, timeOk2 := GetTimeExtension(&emptyEvent, TaskNotBeforeExtension)
	assert.False(t, timeOk2)
}
