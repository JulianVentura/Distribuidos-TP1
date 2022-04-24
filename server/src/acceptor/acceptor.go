package acceptor

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/messages"
	"fmt"
)

type AcceptorConfig struct {
	Host string
	Port string
}

type Acceptor struct {
	skt          socket.ServerSocket
	dispatcher   chan messages.DispatcherMessage
	finish       bool
	has_finished chan bool
}

func Start(config AcceptorConfig, dispatcher chan messages.DispatcherMessage) (*Acceptor, error) {
	skt, err := socket.NewServer(fmt.Sprintf("%v:%v", config.Host, config.Port))
	if err != nil {
		return nil, Err.Ctx("Couldn't create server", err)
	}
	f_channel := make(chan bool, 2)

	acc := &Acceptor{
		skt:          skt,
		dispatcher:   dispatcher,
		finish:       false,
		has_finished: f_channel,
	}

	go acc.run()

	return acc, nil
}

func (self *Acceptor) Finish() {
	self.finish = true
	_ = self.skt.Close()
	<-self.has_finished // We wait until the worker has finished
}

func (self *Acceptor) run() {
	for {
		conn, err := self.skt.Accept()
		if err != nil {
			if self.finish == true {
				fmt.Println("Acceptor Worker has been closed")
				break
			}
			panic("Accept has failed") // TODO: Ver como informar de un error fatal al servidor, para su finalizacion
		}

		self.dispatcher <- messages.NewConnection{Skt: &conn}
	}
	_ = self.skt.Close()
	self.has_finished <- true
}
