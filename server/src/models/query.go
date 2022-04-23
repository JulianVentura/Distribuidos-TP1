package models

//TODO: Completar
type Query struct {
	id string
}

func NewQuery(id string, value float64) (Query, error) {
	return Query{id: id}, nil
}
