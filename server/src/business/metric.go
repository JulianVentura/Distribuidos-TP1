package business

import "fmt"

type Metric struct {
	Id        string
	Value     float64
	Timestamp uint64
}

func NewMetric(id string, value float64) (Metric, error) {
	if value < 0.0 {
		return Metric{}, fmt.Errorf("Value can't be negative")
	}

	return Metric{Id: id, Value: value, Timestamp: 0}, nil
}
