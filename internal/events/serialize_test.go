package events_test

import (
	"context"
	"github.com/mscno/uptask/internal/events"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type dummyArgs struct {
	ArgString string
	ArgInt    int
	ArgMap    map[string]string
	ArgSlice  []string
	ArgBool   bool
}

func (d dummyArgs) Kind() string {
	return "dummyArgs"
}

func fixtureArgs() dummyArgs {
	return dummyArgs{
		ArgString: "test",
		ArgInt:    42,
		ArgMap:    map[string]string{"key": "value"},
		ArgSlice:  []string{"a", "b", "c"},
		ArgBool:   true,
	}
}

func TestSerialize(t *testing.T) {

	event, err := events.Serialize(context.Background(), fixtureArgs())
	require.NoError(t, err)
	_, err = uuid.Parse(event.ID())
	require.NoError(t, err)
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {

	event, err := events.Serialize(context.Background(), fixtureArgs())
	require.NoError(t, err)
	_, err = uuid.Parse(event.ID())
	require.NoError(t, err)

	var args dummyArgs
	err = events.Deserialize(event, &args)
	require.NoError(t, err)
	require.Equal(t, fixtureArgs(), args)
}
