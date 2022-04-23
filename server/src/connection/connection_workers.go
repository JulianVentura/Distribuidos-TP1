package connection

import (
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/messages"
	"fmt"
)

//TODO: Change
type ConnectionWorker struct {
	id                 uint
	queue              chan messages.ConnectionWorkerMessage
	dispatcher         chan messages.DispatcherMessage
	has_finished       chan bool
	current_connection *socket.TCPConnection
}

type ConnectionWorkerConfig struct {
	Connection_timeout_ms uint
	Queue_size            uint
	Id                    uint
}

//TODO: Ver como indicarle la cola del dispatcher para que pueda notificar fin de conexion
func StartConnectionWorker(
	config ConnectionWorkerConfig,
	queue chan messages.ConnectionWorkerMessage,
	dispatcher chan messages.DispatcherMessage,
) (*ConnectionWorker, error) {

	worker := &ConnectionWorker{
		id:                 config.Id,
		queue:              queue,
		dispatcher:         dispatcher,
		has_finished:       make(chan bool, 1),
		current_connection: nil,
	}

	go worker.run()

	return worker, nil
}

func (self *ConnectionWorker) Finish() {
	close(self.queue)
	<-self.has_finished
}

func (self *ConnectionWorker) run() {
	//Connection worker loop
	for event := range self.queue {
		fmt.Printf("Worker %v recibe evento %v\n", self.id, event)
		// switch event.(type) {
		// 	case
		// }
	}

	if self.current_connection != nil {
		self.current_connection.Close()
	}
	self.has_finished <- true
}
