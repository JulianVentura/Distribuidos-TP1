package dispatcher

import (
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/server/src/messages"

	log "github.com/sirupsen/logrus"
)

type Dispatcher struct {
	queue              chan messages.DispatcherMessage
	has_finished       chan bool
	quit               chan bool
	connections        []chan messages.ConnectionWorkerMessage
	alarm_worker_queue chan messages.AlarmManagerMessage
	free_connections   chan uint
}

type DispatcherConfig struct {
}

func Start(
	config DispatcherConfig,
	queue chan messages.DispatcherMessage,
	connections []chan messages.ConnectionWorkerMessage,
	alarm_worker_queue chan messages.AlarmManagerMessage,
) (*Dispatcher, error) {

	free_conn := make(chan uint, len(connections))
	for i := uint(0); i < uint(len(connections)); i++ {
		free_conn <- i
	}
	dispatcher := &Dispatcher{
		queue:              queue,
		has_finished:       make(chan bool, 2),
		quit:               make(chan bool, 2),
		connections:        connections,
		alarm_worker_queue: alarm_worker_queue,
		free_connections:   free_conn,
	}

	go dispatcher.run()

	log.Info("Dispatcher started")
	return dispatcher, nil
}

func (self *Dispatcher) Finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *Dispatcher) run() {
Loop:
	for {
		select {
		case <-self.quit:
			break Loop
		case message := <-self.queue:
			self.dispatch(message)
		}
	}

	log.Info("Finishing Dispatcher")
Finish:
	for {
		select {
		case message := <-self.queue:
			self.dispatch(message)
		default:
			break Finish
		}
	}

	self.has_finished <- true
	log.Info("Dispatcher has finished")
}

func (self *Dispatcher) dispatch(message messages.DispatcherMessage) {
	switch m := message.(type) {
	case *messages.NewConnection:
		self.handle_new_connection(m)
	case *messages.QueryResponse:
		self.handle_query_response(m)
	case *messages.ConnectionFinished:
		self.handle_connection_finished(m)
	default:
		log.Error("An unknown message was assigned to dispatcher")
	}
}

func (self *Dispatcher) handle_new_connection(conn *messages.NewConnection) {
	log.Info("New connection has arrived to the server")

	select {
	case conn_id := <-self.free_connections:
		self.connections[conn_id] <- conn
	default:
		log.Info("There isn't any connection workers free, discarding...")
		protocol.Send(conn.Skt, &protocol.Finish{Message: "Servicio no disponible"})
		conn.Skt.Close()
	}
}

func (self *Dispatcher) handle_query_response(query *messages.QueryResponse) {
	conn_id := query.Conn_worker_id
	n_cons := uint(len(self.connections))
	if conn_id > n_cons {
		log.Errorf("Received a QueryResponse with conn_id %v, which is invalid", conn_id)
	} else if conn_id == n_cons { //AlarmManager id
		self.alarm_worker_queue <- query
	} else {
		self.connections[conn_id] <- query
	}
}

func (self *Dispatcher) handle_connection_finished(conn *messages.ConnectionFinished) {
	conn_id := conn.Conn_worker_id
	if conn_id >= uint(len(self.connections)) {
		log.Errorf("Received a ConnectionFinished with conn_id %v, which is invalid", conn_id)
	}
	self.free_connections <- conn_id
}
