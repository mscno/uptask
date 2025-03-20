package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/samber/oops"
	"time"
)

type Kinder interface {
	Kind() string
}

type Payloader interface {
	Payload() any
}

func Serialize(ctx context.Context, args Kinder) (cloudevents.Event, error) {
	return SerializeWithExt(ctx, args)
}

const Source = "uptask"

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

	e := event.New(event.CloudEventsVersionV1)
	e.SetID(uuid.NewString())
	e.SetType(args.Kind())
	e.SetSource(Source)
	e.SetTime(time.Now().UTC())
	for k, v := range exts {
		e.SetExtension(k, v)
	}

	var buffer bytes.Buffer
	if p, ok := args.(Payloader); ok {
		err := json.NewEncoder(&buffer).Encode(p.Payload())
		if err != nil {
			return cloudevents.Event{}, fmt.Errorf("failed to encode payload: %w", err)
		}
	} else {
		err := json.NewEncoder(&buffer).Encode(args)
		if err != nil {
			return cloudevents.Event{}, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	err := e.SetData(cloudevents.ApplicationCloudEventsJSON, buffer.Bytes())
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
