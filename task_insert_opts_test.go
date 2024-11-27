package uptask

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/mscno/uptask/internal/events"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInsertOpts(t *testing.T) {
	ce := cloudevents.NewEvent()
	ce.SetExtension(events.ScheduledTaskExtension, "true")
	ce.SetExtension(events.TaskMaxRetriesExtension, "3")
	ce.SetExtension(events.TaskRetriedExtension, "1")
	ce.SetExtension(events.TaskQueueExtension, "default")
	ce.SetExtension(events.TaskNotBeforeExtension, "1615766400")

	var opts TaskInsertOpts
	err := opts.FromCloudEvent(ce)
	require.NoError(t, err)
	require.Equal(t, 3, opts.MaxRetries)
	require.Equal(t, "default", opts.Queue)
	require.Equal(t, "2021-03-15", opts.ScheduledAt.Format("2006-01-02"))

}
