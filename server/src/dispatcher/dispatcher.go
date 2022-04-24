package dispatcher

import (
	"distribuidos/tp1/server/src/messages"
	"fmt"
)

type Dispatcher struct {
	queue            chan messages.DispatcherMessage
	has_finished     chan bool
	quit             chan bool
	connections      []chan messages.ConnectionWorkerMessage
	free_connections chan uint
}

type DispatcherConfig struct {
}

func Start(
	config DispatcherConfig,
	queue chan messages.DispatcherMessage,
	connections []chan messages.ConnectionWorkerMessage,
) (*Dispatcher, error) {

	free_conn := make(chan uint, len(connections))
	for i := uint(0); i < uint(len(connections)); i++ {
		free_conn <- i
	}
	dispatcher := &Dispatcher{
		queue:            queue,
		has_finished:     make(chan bool, 2),
		quit:             make(chan bool, 2),
		connections:      connections,
		free_connections: free_conn,
	}

	go dispatcher.run()

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

Finish:
	for {
		select {
		case message := <-self.queue:
			self.dispatch(message)
		default:
			break Finish
		}
	}

	close(self.queue)
	self.has_finished <- true
}

func (self *Dispatcher) dispatch(message messages.DispatcherMessage) {
	switch m := message.(type) {
	case messages.NewConnection:
		self.handle_new_connection(&m)
	case messages.QueryResponse:
		self.handle_query_response(&m)
	case messages.ConnectionFinished:
		self.handle_connection_finished(&m)
	default:
		fmt.Println("ERROR: An unknown message was assigned to dispatcher")
	}
}

func (self *Dispatcher) handle_new_connection(conn *messages.NewConnection) {
	//TODO: Estaria bueno poder obtener ip y puerto de la conexion entrante
	fmt.Println("New connection has arrived to the server")
	fmt.Printf("%v\n", conn.Skt)
	//TODO: Acá habria que hacer un chequeo de rate limiting
	conn_id := <-self.free_connections
	self.connections[conn_id] <- conn
}

func (self *Dispatcher) handle_query_response(query *messages.QueryResponse) {
	conn_id := query.Conn_worker_id
	if conn_id >= uint(len(self.connections)) {
		fmt.Printf("ERROR: Received a QueryResponse with conn_id %v, which is invalid\n", conn_id)
	}
	self.connections[conn_id] <- query
}

func (self *Dispatcher) handle_connection_finished(conn *messages.ConnectionFinished) {
	conn_id := conn.Conn_worker_id
	if conn_id >= uint(len(self.connections)) {
		fmt.Printf("ERROR: Received a ConnectionFinished with conn_id %v, which is invalid\n", conn_id)
	}
	self.free_connections <- conn_id
}
