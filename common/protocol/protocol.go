package protocol

import (
	"distribuidos/tp1/common/socket"
	"encoding/binary"
	"math"
	"time"
)

func encodeF64(number float64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, math.Float64bits(number))

	return buffer
}

func encode64(number uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, number)

	return buffer
}

func encode32(number uint32) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, number)

	return buffer
}

func encode16(number uint16) []byte {
	buffer := make([]byte, 2)
	binary.BigEndian.PutUint16(buffer, number)

	return buffer
}

func encode8(number uint8) []byte {
	//With one byte the endianess interpretation are all the same
	buffer := make([]byte, 1)
	buffer[0] = byte(number)
	return buffer
}

func encodeString(str string) []byte {
	encoded_size := encode32(uint32(len(str)))
	//UTF-8 Encoding
	encoded_str := []byte(str)
	return append(encoded_size, encoded_str...)
}

func decodeF64(encoded []byte) (float64, uint32) {
	decoded := binary.BigEndian.Uint64(encoded)

	return math.Float64frombits(decoded), 8
}

func decode64(encoded []byte) (uint64, uint32) {
	return binary.BigEndian.Uint64(encoded), 8
}

func decode32(encoded []byte) (uint32, uint32) {
	return binary.BigEndian.Uint32(encoded), 4
}

func decode16(encoded []byte) (uint16, uint32) {
	return binary.BigEndian.Uint16(encoded), 2
}

func decode8(encoded []byte) (uint8, uint32) {
	return uint8(encoded[0]), 1
}

func decodeString(encoded []byte) (string, uint32) {

	str_len, n := decode32(encoded)
	str := string(encoded[n : str_len+n])

	return str, str_len + n
}

func Send(socket *socket.TCPConnection, message Encodable) error {
	encoded_msg := message.encode()
	size := uint32(len(encoded_msg))
	msg_len := encode32(size)
	to_send := append(msg_len, encoded_msg...)

	return socket.Send(to_send)
}

func Receive_with_timeout(socket *socket.TCPConnection, t time.Duration) (Encodable, error) {
	buffer := make([]byte, 4)
	//TODO: Ver si vale la pena ajustar el tiempo demorado entre el primer llamado y el segundo, porque en el peor de los casos se
	// espera 2xt
	err := socket.Receive_with_timeout(buffer, t)
	if err != nil {
		return nil, err
	}
	msg_len, _ := decode32(buffer)
	buffer = make([]byte, msg_len)
	err = socket.Receive_with_timeout(buffer, t)
	if err != nil {
		return nil, err
	}
	decoded, err := Decode(buffer)

	return decoded, err
}

func Receive(socket *socket.TCPConnection) (Encodable, error) {
	buffer := make([]byte, 4)
	err := socket.Receive(buffer)
	if err != nil {
		return nil, err
	}
	msg_len, _ := decode32(buffer)
	buffer = make([]byte, msg_len)
	err = socket.Receive(buffer)
	if err != nil {
		return nil, err
	}
	decoded, err := Decode(buffer)

	return decoded, err
}

func append_slices(slices [][]byte) []byte {
	size := 0
	for _, slice := range slices {
		size += len(slice)
	}

	result := make([]byte, size)

	start := 0
	for _, slice := range slices {
		start += copy(result[start:], slice)
	}

	return result
}
