package protocol

import "fmt"

type Encodable interface {
	encode() []byte
	fromEncoding([]byte) error
	// implementsEncodable() can be added as a method to force explicit interface implementation. It doesn't do anything
}

const (
	MetricOP uint8 = 0
	QueryOP        = 1
)

type AggregationCommand uint8

const (
	AVG   AggregationCommand = 0
	MIN                      = 1
	MAX                      = 2
	COUNT                    = 3
)

type Metric struct { //Implements Encodable
	id    string
	value float64
}

type Query struct { //Implements Encodable
	metric_id             string
	from                  string //Datetime
	to                    string //Datetime
	aggregation           AggregationCommand
	aggregationWindowSecs float64
}

func NewMetric(id string, value float64) *Metric {
	return &Metric{
		id:    id,
		value: value,
	}
}

func (self *Query) encode() []byte {
	message_id := Encode8(QueryOP)
	metric_id := EncodeString(self.metric_id)
	from := EncodeString(self.from)
	to := EncodeString(self.to)
	aggregation := Encode8(uint8(self.aggregation))
	agg_window := Encode64(uint64(self.aggregationWindowSecs))

	return append_slices([][]byte{message_id, metric_id, from, to, aggregation, agg_window})
}

//TODO: Que pasa si la data llega corrupta? Panic?
func (self *Query) fromEncoding(code []byte) error {
	start := uint32(1) //MetricID encoding

	metric_id, str_len := DecodeString(code[start:])
	start += str_len

	from, str_len := DecodeString(code[start:])
	start += str_len

	to, str_len := DecodeString(code[start:])
	start += str_len

	self.aggregation = AggregationCommand(Decode8(code[start:]))
	start += 1

	self.aggregationWindowSecs = float64(Decode64(code[start:]))
	self.metric_id = metric_id
	self.from = from
	self.to = to

	return nil
}

//TODO: Improve performance
func (self *Metric) encode() []byte {
	message_id := Encode8(MetricOP)
	metric_id := EncodeString(self.id)
	value := Encode64(uint64(self.value))

	return append_slices([][]byte{message_id, metric_id, value})
}

func (self *Metric) fromEncoding(code []byte) error {
	start := uint32(1) //MetricID encoding
	id, str_len := DecodeString(code[start:])
	start += str_len
	self.id = id
	self.value = float64(Decode64(code[start:]))

	return nil
}

func Encode(message Encodable) []byte {
	return message.encode()
}

func Decode(code []byte) (Encodable, error) {
	var err error
	var msg Encodable
	message_id := uint8(Decode8(code))
	switch message_id {
	case MetricOP:
		msg = &Metric{}
	case QueryOP:
		msg = &Query{}
	default:
		return nil, fmt.Errorf("Error al decodificar el mensaje\n")
	}

	err = msg.fromEncoding(code) //Including message_id
	if err != nil {
		return nil, err
	}

	return msg, nil
}
