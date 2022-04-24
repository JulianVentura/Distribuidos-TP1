package models

import "fmt"

//TODO: Ver si vale la pena cambiar "Metric" por "Event" cuando se trata de la llegada de un evento
type Metric struct {
	Id    string
	Value float64
}

func NewMetric(id string, value float64) (Metric, error) {
	//TODO: Chequear si es correcto descartar negativos
	if value < 0.0 {
		return Metric{}, fmt.Errorf("Value can't be negative")
	}

	return Metric{Id: id, Value: value}, nil
}
