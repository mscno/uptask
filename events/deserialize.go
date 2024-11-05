package events

import (
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func Deserialize(ce cloudevents.Event, target interface{}) error {
	return json.Unmarshal(ce.Data(), &target)
}
