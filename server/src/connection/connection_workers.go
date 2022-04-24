package connection

import (
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"time"
)

//TODO: Change
type ConnectionWorker struct {
	id           uint
	queue        chan messages.ConnectionWorkerMessage
	dispatcher   chan messages.DispatcherMessage
	quit         chan bool
	has_finished chan bool
	conn_timeout time.Duration
}

type ConnectionWorkerConfig struct {
	Connection_timeout time.Duration
	Id                 uint
}

//TODO: Ver como indicarle la cola del dispatcher para que pueda notificar fin de conexion
func StartConnectionWorker(
	config ConnectionWorkerConfig,
	queue chan messages.ConnectionWorkerMessage,
	dispatcher chan messages.DispatcherMessage,
) (*ConnectionWorker, error) {

	worker := &ConnectionWorker{
		id:           config.Id,
		queue:        queue,
		dispatcher:   dispatcher,
		quit:         make(chan bool, 2),
		has_finished: make(chan bool, 2),
		conn_timeout: config.Connection_timeout,
	}

	go worker.run()

	return worker, nil
}

func (self *ConnectionWorker) Finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *ConnectionWorker) run() {
Loop:
	for {
		select {
		case <-self.quit:
			break Loop
		case message := <-self.queue:
			fmt.Println("New message received in worker")
			switch m := message.(type) {
			case *messages.NewConnection:
				self.handle_client_connection(m.Skt)
				fmt.Println("Client connection finished")
			case *messages.QueryResponse:
				fmt.Println("ERROR: A query response was assigned to a worker without an active connection")
			default:
				fmt.Printf("ERROR: An unknown message was assigned to a worker %v\n", m)
			}
		}
	}

	close(self.queue)
	self.has_finished <- true
}

func (self *ConnectionWorker) handle_client_connection(client *socket.TCPConnection) {

Loop:
	for {
		select {
		case <-self.quit:
			self.quit <- true // De esta forma propagamos la señal
			break Loop
		default:
			message, err := protocol.Receive_with_timeout(client, self.conn_timeout)
			if err != nil {
				//Wichever the error is, we want to close the connection
				send_finish(client)
				break Loop
			}
			switch m := message.(type) {
			case *protocol.Metric:
				self.handle_new_metric(m, client)
			case *protocol.Query:
				self.handle_query(m, client)
			case *protocol.Finish:
				break Loop
			}
		}
	}

	_ = client.Close()
	//Acá probablemente tendríamos que enviar el mensaje de conexion finalizada al Dispatcher
}

func (self *ConnectionWorker) handle_new_metric(metric *protocol.Metric, client *socket.TCPConnection) {
	fmt.Printf(" - Metric: (%v, %v)\n", metric.Id, metric.Value)
	//Instanciar métrica a nivel business
	//Encolar en la cola de writers de bdd
	//Enviar mensaje OK al cliente
	send_ok(client)
	//retornar
}

func (self *ConnectionWorker) handle_query(query *protocol.Query, client *socket.TCPConnection) {
	fmt.Printf(" - Query: (%v, %v, %v, %v, %v)\n", query.Metric_id, query.From, query.To, query.Aggregation, query.AggregationWindowSecs)
	//Instanciar query a nivel business
	//Encolar en la cola de readers de bdd
	//Esperar por la respuesta desde self.queue
	//Enviar mensaje QueryResponse al cliente
	send_ok(client) //Change
	//retornar
}

func send_error(client *socket.TCPConnection, message string) error {
	return protocol.Send(client, &protocol.Error{Message: message})
}

func send_finish(client *socket.TCPConnection) {
	_ = protocol.Send(client, &protocol.Finish{})
}

func send_ok(client *socket.TCPConnection) {
	_ = protocol.Send(client, &protocol.Ok{})
}
