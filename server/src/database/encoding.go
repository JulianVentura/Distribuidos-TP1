package database

import (
	Err "distribuidos/tp1/common/errors"
	bs "distribuidos/tp1/server/src/business"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

//TODO: Todas las operaciones sobre los archivos deberían ser más eficientes

func encode_metric(metric *bs.Metric) string {
	return fmt.Sprintf("%v,%v", metric.Timestamp, metric.Value)
}

func decode_metric(code string, metric_id string) (*bs.Metric, error) {
	args := strings.Split(code, ",")
	//It shouldn't fail, we encoded it
	timestamp, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, err
	}
	value, err := strconv.ParseFloat(args[1], 10)
	if err != nil {
		return nil, err
	}
	return &bs.Metric{
		Id:        metric_id,
		Value:     value,
		Timestamp: uint64(timestamp),
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

func read_into_lines(file string) ([]string, error) {
	bytes, err := ioutil.ReadFile(file) //File descriptor is automatically closed
	if err != nil {
		return nil, fmt.Errorf("Couldn't open file %v. %v\n", file, err)
	}

	string_result := string(bytes)

	splits := strings.Split(string_result, "\n")

	l := len(splits)

	if l > 0 {
		splits = splits[:(l - 1)] //Filter out \n
	}

	return splits, nil
}
