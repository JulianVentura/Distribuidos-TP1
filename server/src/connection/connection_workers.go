package connection

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	bs "distribuidos/tp1/server/src/business"
	"distribuidos/tp1/server/src/messages"
	"time"

	log "github.com/sirupsen/logrus"
)

type ConnectionWorker struct {
	id           uint
	queue        chan messages.ConnectionWorkerMessage
	dispatcher   chan messages.DispatcherMessage
	event_queue  chan messages.WriteDatabaseMessage
	query_queue  chan messages.ReadDatabaseMessage
	quit         chan bool
	has_finished chan bool
	conn_timeout time.Duration
}

type ConnectionWorkerConfig struct {
	Connection_timeout time.Duration
	Id                 uint
}

func StartConnectionWorker(
	config ConnectionWorkerConfig,
	queue chan messages.ConnectionWorkerMessage,
	dispatcher chan messages.DispatcherMessage,
	event_queue chan messages.WriteDatabaseMessage,
	query_queue chan messages.ReadDatabaseMessage,
) (*ConnectionWorker, error) {

	worker := &ConnectionWorker{
		id:           config.Id,
		queue:        queue,
		dispatcher:   dispatcher,
		event_queue:  event_queue,
		query_queue:  query_queue,
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
			switch m := message.(type) {
			case *messages.NewConnection:
				self.handle_client_connection(m.Skt)
			case *messages.QueryResponse:
				log.Errorf("A query response was assigned to a worker without an active connection")
			default:
				log.Errorf("An unknown message was assigned to a worker %v", m)
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
			self.quit <- true // We do this in order to propagate the signal
			break Loop
		default:
			message, err := protocol.Receive_with_timeout(client, self.conn_timeout)
			if err != nil {
				//Wichever the error is, we want to close the connection
				log.Debugf("Closing client connection because of error %v", err)
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
	self.dispatcher <- &messages.ConnectionFinished{
		Conn_worker_id: self.id,
	}

	log.Info("Client connection finished")
}

func (self *ConnectionWorker) handle_new_metric(m *protocol.Metric, client *socket.TCPConnection) {
	//Validate metric data with business rules
	metric, err := bs.NewMetric(m.Id, m.Value)
	if err != nil {
		send_error(client, "Bad formating")
		return
	}
	//Send the new metric to the Database
	self.event_queue <- &messages.NewMetric{
		Conn_worker_id: self.id,
		Metric:         metric,
	}
	//Send confirmation message to client
	send_ok(client)
}

func (self *ConnectionWorker) handle_query(q *protocol.Query, client *socket.TCPConnection) {
	//Data parsing
	from, err1 := parse_time(q.From)
	to, err2 := parse_time(q.To)
	if err1 != nil || err2 != nil {
		send_error(client, "Bad formating")
		return
	}
	//Validate query data with business rules
	query, err := bs.NewQuery("", q.Metric_id, from, to, q.Aggregation, q.AggregationWindowSecs)
	if err != nil {
		send_error(client, "Bad formating")
		return
	}
	//Send the query to the Database
	self.query_queue <- &messages.NewQuery{
		Conn_worker_id: self.id,
		Query:          query,
	}

	//Wait for the Database response
	var response *messages.QueryResponse

Loop:
	for {
		message := <-self.queue
		switch m := message.(type) {
		case *messages.QueryResponse:
			response = m
			break Loop
		default:
			log.Errorf("An unexpected message was received by ConnectionWorker, waiting for a QueryResponse %v", m)
		}
	}

	//Check if there was an error
	if response.Is_error {
		send_error(client, response.Error_message)
		return
	}

	//Aggregate the database result
	result, err := bs.Aggregate(&response.Query, response.Response)
	if err != nil {
		log.Errorf("ConnectionWorker: Error aggregating query response: %v", err)
		return
	}

	//Send the aggregated result to the client
	send_query_response(client, result)
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

func send_query_response(client *socket.TCPConnection, data []float64) error {
	return protocol.Send(client, &protocol.QueryResponse{Data: data})
}

func parse_time(t string) (time.Time, error) {
	//Our precission will be to milliseconds order
	layout := "2006-01-02T15:04:05.000Z" // Weird Golang's time parse layout
	parsed, err := time.Parse(layout, t)

	if err != nil {
		return time.Time{}, Err.Ctx("Couldn't parse time object", err)
	}
	return parsed, nil
}
