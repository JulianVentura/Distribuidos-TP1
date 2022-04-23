package dispatcher

import (
	"distribuidos/tp1/server/src/messages"
)

type Dispatcher struct {
	queue        chan messages.DispatcherMessage
	has_finished chan bool
	connections  []chan messages.ConnectionWorkerMessage
}

type DispatcherConfig struct {
	Queue_size uint
}

func Start(
	config DispatcherConfig,
	queue chan messages.DispatcherMessage,
	connections []chan messages.ConnectionWorkerMessage,
) (*Dispatcher, error) {

	dispatcher := &Dispatcher{
		queue:        queue,
		has_finished: make(chan bool, 1),
		connections:  connections,
	}

	go dispatcher.run()

	return dispatcher, nil
}

func (self *Dispatcher) run() {
	//Dispatcher loop
}

func (self *Dispatcher) Finish() {

}
