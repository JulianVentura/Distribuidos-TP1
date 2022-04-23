package models

type Event struct {
	id    string
	value float64
}

func NewEvent(id string, value float64) (Event, error) {
	return Event{id: id, value: value}, nil
}
