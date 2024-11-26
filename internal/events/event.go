package events

type Event struct {
	ID   string
	Kind string
	Args map[string]interface{}
}
