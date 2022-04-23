package protocol

import (
	"distribuidos/tp1/common/socket"
	"encoding/binary"
)

func Encode64(number uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, number)

	return buffer
}

func Encode32(number uint32) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, number)

	return buffer
}

func Encode16(number uint16) []byte {
	buffer := make([]byte, 2)
	binary.BigEndian.PutUint16(buffer, number)

	return buffer
}

func Encode8(number uint8) []byte {
	buffer := make([]byte, 1)
	buffer[0] = byte(number)
	return buffer
}

func EncodeString(str string) []byte {
	encoded_size := Encode32(uint32(len(str)))
	//UTF-8 Encoding
	encoded_str := []byte(str)

	return append(encoded_size, encoded_str...)
}

func Decode64(encoded []byte) uint64 {
	return binary.BigEndian.Uint64(encoded)
}

func Decode32(encoded []byte) uint32 {
	return binary.BigEndian.Uint32(encoded)
}

func Decode16(encoded []byte) uint16 {
	return binary.BigEndian.Uint16(encoded)
}

func Decode8(encoded []byte) uint8 {
	return uint8(encoded[0])
}

func DecodeString(encoded []byte) (string, uint32) {

	str_len := Decode32(encoded[:4])
	str := string(encoded[4:str_len])

	return str, str_len
}

func Send(socket *socket.TCPConnection, message Encodable) error {
	encoded_msg := message.encode()
	size := uint32(len(encoded_msg))
	msg_len := Encode32(size)
	to_send := append(msg_len, encoded_msg...)

	return socket.Send(to_send)
}

func Receive(socket *socket.TCPConnection) (*Encodable, error) {
	buffer := make([]byte, 4)
	err := socket.Receive(buffer)
	if err != nil {
		return nil, err
	}
	msg_len := Decode32(buffer)
	buffer = make([]byte, msg_len)
	err = socket.Receive(buffer)
	if err != nil {
		return nil, err
	}
	decoded, err := Decode(buffer)

	return &decoded, err
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
