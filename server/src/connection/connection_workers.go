package connection

import (
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/messages"
	"distribuidos/tp1/server/src/models"
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
	self.dispatcher <- messages.ConnectionFinished{
		Conn_worker_id: self.id,
	}
}

func (self *ConnectionWorker) handle_new_metric(m *protocol.Metric, client *socket.TCPConnection) {
	//Instanciar métrica a nivel business
	metric, err := models.NewMetric(m.Id, m.Value)
	if err != nil {
		send_error(client, "Bad formating")
		return
	}
	//Encolar en la cola de writers de bdd
	fmt.Printf(" - Metric: (%v, %v)\n", metric.Id, metric.Value)
	//Enviar mensaje OK al cliente
	send_ok(client)
}

func (self *ConnectionWorker) handle_query(q *protocol.Query, client *socket.TCPConnection) {
	//Instanciar query a nivel business
	query, err := models.NewQuery(q.Metric_id, q.From, q.To, q.Aggregation, q.AggregationWindowSecs)
	if err != nil {
		send_error(client, "Bad formating")
		return
	}
	//Encolar en la cola de readers de bdd
	fmt.Printf(" - Query: (%v, %v, %v, %v, %v)\n", query.Metric_id, query.From, query.To, query.Aggregation, query.AggregationWindowSecs)
	//Esperar por la respuesta desde self.queue
	//Enviar mensaje QueryResponse al cliente
	send_ok(client) //Change
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
