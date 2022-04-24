package models

import (
	"fmt"
	"time"
)

type Query struct { //Implements Encodable
	Metric_id             string
	From                  time.Time
	To                    time.Time
	Aggregation           string
	AggregationWindowSecs float64
}

func NewQuery(id string, from string, to string, agg string, agg_window_secs float64) (Query, error) {
	err := validate_aggregation(agg, agg_window_secs)
	if err != nil {
		return Query{}, err
	}

	t_from, t_to, err := validate_and_parse_time(from, to)
	if err != nil {
		return Query{}, err
	}
	return Query{
		Metric_id:             id,
		From:                  t_from,
		To:                    t_to,
		Aggregation:           agg,
		AggregationWindowSecs: agg_window_secs,
	}, nil
}

func validate_aggregation(aggregation string, agg_window_secs float64) error {
	if agg_window_secs < 0.0 {
		return fmt.Errorf("Aggregation window can't be negative")
	}
	// Golang's set
	valid_aggs := map[string]bool{
		"MAX":   true,
		"MIN":   true,
		"COUNT": true,
		"AVG":   true,
	}

	_, ok := valid_aggs[aggregation]
	if !ok {
		return fmt.Errorf("Aggregation command %v not valid", aggregation)
	}

	return nil
}

func validate_and_parse_time(f string, t string) (time.Time, time.Time, error) {
	//Our precission will be to seconds order
	err_time := time.Time{}
	layout := "2006-01-02T15:04:05" // Weird Golang's time parse layout
	from, ef := time.Parse(layout, f)
	to, et := time.Parse(layout, t)

	if ef != nil || et != nil {
		return err_time, err_time, fmt.Errorf("Couldn't parse time objects")
	}
	//TODO: Ver si hace falta limitar tambien que sean iguales
	if to.Unix() < from.Unix() {
		return err_time, err_time, fmt.Errorf("Provided time period is negative")
	}

	return err_time, err_time, nil
}
