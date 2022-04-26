package database

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/models"
	"fmt"
	"io"
	"strconv"
	"strings"
)

//TODO: Ver si vale la pena cambiar esto a binario más adelante
func encode_metric(metric *models.Metric, timestamp int64) string {
	return fmt.Sprintf("%v,%v", timestamp, metric.Value)
}

func decode_metric(code string, metric_id string) (*models.Metric, error) {
	args := strings.Split(code, ",")
	//It shouldn't fail, we encoded it
	value, err := strconv.ParseFloat(args[1], 10)
	if err != nil {
		return nil, err
	}
	return &models.Metric{
		Id:    metric_id,
		Value: value,
	}, nil
}

func decode_timestamp(code string) (int64, error) {
	args := strings.Split(code, ",")
	//It shouldn't fail since we encoded it
	timestamp, err := strconv.ParseInt(args[0], 10, 64)

	return timestamp, err
}

func write_whole_string(writer io.Writer, message string) error {
	to_write := len(message)
	wrote := 0

	for wrote < to_write {
		bytes, err := io.WriteString(writer, message[wrote:])

		if err != nil {
			return Err.Ctx("Error on read syscall of io.Writer on Merger", err)
		}

		wrote += bytes
	}

	return nil
}
