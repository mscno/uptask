package events

import (
	"context"
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/samber/oops"
	"time"
)

type Kinder interface {
	Kind() string
}

func Serialize(ctx context.Context, args Kinder) (cloudevents.Event, error) {
	return SerializeWithExt(ctx, args)
}

const Source = "fxfeed"

const ApplicationJson = "application/json"

func SerializeWithExt(ctx context.Context, args Kinder, extKvPairs ...string) (cloudevents.Event, error) {
	if len(extKvPairs)%2 != 0 {
		return cloudevents.Event{}, oops.In("taskserver").
			Tags("SerializeWithExt", "extKvPairs must be even").
			With("extKvPairs", extKvPairs).
			Errorf("extKvPairs must be even")
	}

	exts := make(map[string]string)
	for i := 0; i < len(extKvPairs); i += 2 {
		exts[extKvPairs[i]] = extKvPairs[i+1]
	}

	byteslice, err := json.Marshal(args)

	e := event.New(event.CloudEventsVersionV1)
	e.SetID(uuid.NewString())
	e.SetType(args.Kind())
	e.SetSource(Source)
	e.SetTime(time.Now().UTC())
	for k, v := range exts {
		e.SetExtension(k, v)
	}

	err = e.SetData(cloudevents.ApplicationCloudEventsJSON, byteslice)
	if err != nil {
		return cloudevents.Event{}, oops.In("taskserver").
			Tags("SerializeWithExt", "failed to set data").
			With("event", e).
			Wrap(err)
	}

	err = e.Validate()
	if err != nil {
		return cloudevents.Event{}, oops.In("taskserver").
			Tags("SerializeWithExt", "failed to validate event").
			With("event", e).
			Wrap(err)
	}

	return e, nil
}
