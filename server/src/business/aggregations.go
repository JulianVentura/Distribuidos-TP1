package business

import (
	"fmt"
	"math"
)

func Aggregate(query *Query, metrics []*Metric) ([]float64, error) {
	//If AggWindSec == 0 then we just want to return all the metric values as an array
	//It doesn't have any sense to aggregate only one value
	if query.AggregationWindowSecs == 0.0 {
		result := make([]float64, len(metrics))
		for i := 0; i < len(metrics); i++ {
			result[i] = metrics[i].Value
		}

		return result, nil
	}

	//Calculate the number of bins
	bins := split_in_bins(query.From.UnixMilli(), query.To.UnixMilli(), query.AggregationWindowSecs, metrics)
	result, err := aggregate_bins(bins, query.Aggregation)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func split_in_bins(from int64, to int64, agg_wind_secs float64, metrics []*Metric) [][]float64 {

	agg_wind_ms := uint64(agg_wind_secs * 1000)
	time_interval := float64(to - from)
	n := uint64(math.Ceil(time_interval / float64(agg_wind_ms)))
	bins := make([][]float64, n)
	//Number of elements per bin approximation
	k := uint64(len(metrics)) / n
	top := uint64(from) + agg_wind_ms

	metric_idx := 0
	for bin_idx := 0; bin_idx < len(bins); bin_idx++ {
		current := make([]float64, 0, k)
		for metric_idx < len(metrics) && metrics[metric_idx].Timestamp < top {
			current = append(current, metrics[metric_idx].Value)
			metric_idx++
		}
		bins[bin_idx] = current
		top += agg_wind_ms
	}

	return bins
}

func aggregate_bins(bins [][]float64, aggregation string) ([]float64, error) {

	agg_fn, err := get_agg_function(aggregation)
	if err != nil {
		return nil, err
	}
	result := make([]float64, 0, len(bins))
	for _, bin := range bins {
		result = append(result, agg_fn(bin))
	}

	return result, nil
}

func get_agg_function(aggregation string) (func([]float64) float64, error) {

	switch aggregation {
	case "MAX":
		return max, nil
	case "MIN":
		return min, nil
	case "COUNT":
		return count, nil
	case "AVG":
		return avg, nil
	}

	return nil, fmt.Errorf("Aggregation operation not recognized")
}

func max(elems []float64) float64 {
	if len(elems) < 1 {
		return 0
	}
	max := math.Inf(-1)
	for _, elem := range elems {
		max = math.Max(max, elem)
	}

	return max
}

func min(elems []float64) float64 {
	if len(elems) < 1 {
		return 0
	}
	min := math.Inf(1)
	for _, elem := range elems {
		min = math.Min(min, elem)
	}

	return min
}

func count(elems []float64) float64 {
	return float64(len(elems))
}

func avg(elems []float64) float64 {
	if len(elems) < 1 {
		return 0
	}
	sum := 0.0
	for _, elem := range elems {
		sum += elem
	}

	return sum / float64(len(elems))
}
