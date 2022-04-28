package business

import (
	"fmt"
	"time"
)

type Query struct { //Implements Encodable
	Id                    string
	Metric_id             string
	From                  time.Time
	To                    time.Time
	Aggregation           string
	AggregationWindowSecs float64
}

func NewQuery(
	id string,
	metric_id string,
	from time.Time,
	to time.Time,
	agg string,
	agg_window_secs float64) (Query, error) {

	err := validate_aggregation(agg, agg_window_secs)
	if err != nil {
		return Query{}, err
	}

	err = validate_time(&from, &to)
	if err != nil {
		return Query{}, err
	}
	return Query{
		Id:                    id,
		Metric_id:             metric_id,
		From:                  from,
		To:                    to,
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

func validate_time(from *time.Time, to *time.Time) error {
	if to.UnixMilli() < from.UnixMilli() {
		return fmt.Errorf("Provided time period is negative")
	}

	return nil
}
