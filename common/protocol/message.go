package protocol

import "fmt"

type Encodable interface {
	encode() []byte
	fromEncoding([]byte) error
	// implementsEncodable() can be added as a method to force explicit interface implementation. It doesn't do anything
}

const (
	MetricOP        uint8 = 0
	QueryOP               = 1
	QueryResponseOP       = 2
	ErrorOP               = 3
	FinishOP              = 4
	OkOP                  = 5
)

type AggregationCommand uint8

const (
	AVG   AggregationCommand = 0
	MIN                      = 1
	MAX                      = 2
	COUNT                    = 3
)

type Metric struct { //Implements Encodable
	Id    string
	Value float64
}

type Query struct { //Implements Encodable
	Metric_id             string
	From                  string //Datetime
	To                    string //Datetime
	Aggregation           string
	AggregationWindowSecs float64
}

type Error struct { //Implements Encodable
	Message string
}

type Finish struct { //Implements Encodable
	Message string
}

type QueryResponse struct { //Implements Encodable
	Data []float64
}

type Ok struct { //Implements Encodable
}

func encodeAggregation(agg string) []byte {
	var code AggregationCommand
	switch agg {
	case "AVG":
		code = AVG
	case "MIN":
		code = MIN
	case "MAX":
		code = MAX
	case "COUNT":
		code = COUNT
	default:
		//We should return an error instead
		//This is done on purpose to force server to check corruption on aggregation command
		//The optimal solution would check for errors on both client and server sides
		code = AggregationCommand(255)
	}

	return encode8(uint8(code))
}

func decodeAggregation(bytes []byte) (string, uint32, error) {
	var agg string
	_code, n := decode8(bytes)
	code := AggregationCommand(_code)

	switch code {
	case AVG:
		agg = "AVG"
	case MIN:
		agg = "MIN"
	case MAX:
		agg = "MAX"
	case COUNT:
		agg = "COUNT"
	default:
		agg = "UNKNOWN" //We leave the error to the upper layer
	}

	return agg, n, nil
}
func (self *Query) encode() []byte {
	message_id := encode8(QueryOP)
	metric_id := encode_string(self.Metric_id)
	from := encode_string(self.From)
	to := encode_string(self.To)
	aggregation := encodeAggregation(self.Aggregation)
	agg_window := encodeF64(self.AggregationWindowSecs)

	return append_slices([][]byte{message_id, metric_id, from, to, aggregation, agg_window})
}

func (self *Query) fromEncoding(code []byte) error {
	_, start := decode8(code)

	metric_id, n := decode_string(code[start:])
	start += n

	from, n := decode_string(code[start:])
	start += n

	to, n := decode_string(code[start:])
	start += n

	aggregation, n, err := decodeAggregation(code[start:])
	if err != nil {
		return err
	}
	start += n

	agg_window, n := decodeF64(code[start:])
	start += n

	self.Metric_id = metric_id
	self.From = from
	self.To = to
	self.Aggregation = aggregation
	self.AggregationWindowSecs = agg_window

	return nil
}

func (self *Metric) encode() []byte {
	message_id := encode8(MetricOP)
	metric_id := encode_string(self.Id)
	value := encodeF64(self.Value)

	return append_slices([][]byte{message_id, metric_id, value})
}

func (self *Metric) fromEncoding(code []byte) error {

	_, start := decode8(code)

	id, n := decode_string(code[start:])
	start += n

	value, n := decodeF64(code[start:])
	start += n

	self.Id = id
	self.Value = value

	return nil
}

func (self *Error) encode() []byte {
	message_id := encode8(ErrorOP)
	message := encode_string(self.Message)
	return append(message_id, message...)
}

func (self *Error) fromEncoding(code []byte) error {

	_, start := decode8(code)

	message, _ := decode_string(code[start:])

	self.Message = message

	return nil
}

func (self *Finish) encode() []byte {
	message_id := encode8(FinishOP)
	message := encode_string(self.Message)
	return append(message_id, message...)
}

func (self *Finish) fromEncoding(code []byte) error {
	_, start := decode8(code)

	message, _ := decode_string(code[start:])

	self.Message = message

	return nil
}

func (self *Ok) encode() []byte {
	return encode8(OkOP)
}

func (self *Ok) fromEncoding(code []byte) error {
	return nil
}

func (self *QueryResponse) encode() []byte {

	message_id := encode8(QueryResponseOP)
	data := encode_F64slice(self.Data)

	return append(message_id, data...)
}

func (self *QueryResponse) fromEncoding(code []byte) error {
	_, start := decode8(code)
	slice, _ := decode_F64slice(code[start:])

	self.Data = slice

	return nil
}

func Encode(message Encodable) []byte {
	return message.encode()
}

func Decode(code []byte) (Encodable, error) {
	var err error
	var msg Encodable

	id, _ := decode8(code)
	message_id := uint8(id)

	switch message_id {

	case MetricOP:
		msg = &Metric{}
	case QueryOP:
		msg = &Query{}
	case ErrorOP:
		msg = &Error{}
	case FinishOP:
		msg = &Finish{}
	case OkOP:
		msg = &Ok{}
	case QueryResponseOP:
		msg = &QueryResponse{}
	default:
		return nil, fmt.Errorf("Unknown received message\n")
	}

	err = msg.fromEncoding(code) //Including message_id
	if err != nil {
		return nil, err
	}

	return msg, nil
}
