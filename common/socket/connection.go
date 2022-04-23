package socket

import (
	Err "distribuidos/tp1/common/errors"
	"net"
)

type TCPConnection struct {
	skt net.Conn
}

func newTCPConnection(skt net.Conn) TCPConnection {
	return TCPConnection{skt: skt}
}

func (self *TCPConnection) Close() error {
	err := self.skt.Close()

	if err != nil {
		return Err.Ctx("Error on close syscall of socket.Close", err)
	}

	return nil
}

func (self *TCPConnection) Send(message []byte) error {
	toSend := len(message)
	send := 0
	for send < toSend {
		bytes, err := self.skt.Write(message[send:])

		if err != nil {
			return Err.Ctx("Error on write syscall of socket.Send", err)
		}
		send += bytes
	}

	return nil
}

func (self *TCPConnection) Receive(message []byte) error {
	toReceive := len(message)
	received := 0
	for received < toReceive {
		bytes, err := self.skt.Read(message[received:])

		if err != nil {
			return Err.Ctx("Error on read syscall of socket.Receive", err)
		}

		received += bytes
	}

	return nil
}
