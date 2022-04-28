package database

import (
	bs "distribuidos/tp1/server/src/business"
	"distribuidos/tp1/server/src/messages"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type reader struct {
	config           readerConfig
	query_queue      chan messages.ReadDatabaseMessage
	dispatcher_queue chan messages.DispatcherMessage
	file_manager     *FileManager
	quit             chan bool
	has_finished     chan bool
}

type readerConfig struct {
	id         uint
	files_path string
}

func start_reader(
	config readerConfig,
	file_manager *FileManager,
	query_queue chan messages.ReadDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage) (*reader, error) {

	self := &reader{
		config:           config,
		query_queue:      query_queue,
		dispatcher_queue: dispatcher_queue,
		file_manager:     file_manager,
		quit:             make(chan bool, 2),
		has_finished:     make(chan bool, 2),
	}

	go self.run()

	log.Infof("Database reader %v started\n", self.config.id)
	return self, nil
}

func (self *reader) finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *reader) run() {

Loop:
	for {
		select {
		case <-self.quit:
			self.quit <- true // Check if this is necesary: We propagate the signal
			break Loop
		case message := <-self.query_queue:
			switch m := message.(type) {
			case *messages.NewQuery:
				self.handle_new_query(m)
			default:
				log.Error("Message received on DatabaseReader is not recognized")
			}
		}
	}

	//If quit signal has been received, we must first empty the query queue
	self.handle_remaining_events()

	log.Infof("Database reader %v finished\n", self.config.id)
	self.has_finished <- true
}

func (self *reader) handle_new_query(query *messages.NewQuery) {
	metric_id := query.Query.Metric_id

	from := uint64(query.Query.From.UnixMilli())
	to := uint64(query.Query.To.UnixMilli())

	metrics, err := self.find_metrics_on_interval(metric_id, from, to)

	if err != nil {
		self.send_query_error(fmt.Sprintf("%v", err), query)
		return
	}

	self.dispatcher_queue <- &messages.QueryResponse{
		Conn_worker_id: query.Conn_worker_id,
		Query:          query.Query,
		Response:       metrics,
		Is_error:       false,
	}

	return
}

func (self *reader) send_query_error(msg string, query *messages.NewQuery) {

	self.dispatcher_queue <- &messages.QueryResponse{
		Conn_worker_id: query.Conn_worker_id,
		Query:          query.Query,
		Is_error:       true,
		Error_message:  msg,
	}
}

func (self *reader) handle_remaining_events() {
Loop:
	for {
		select {
		case message := <-self.query_queue:
			switch m := message.(type) {
			case *messages.NewQuery:
				self.handle_new_query(m)
			default:
				log.Error("Message received on DatabaseReader is not recognized")
			}
		default:
			break Loop
		}
	}
}

func (self *reader) find_metrics_on_interval(metric_id string, from uint64, to uint64) ([]*bs.Metric, error) {
	file_name := fmt.Sprintf("%v/%v", self.config.files_path, metric_id)
	result := make([]*bs.Metric, 0, 20) //Start buffer size

	lock, err := self.file_manager.get_lock_for(metric_id)
	if err != nil {
		return nil, fmt.Errorf("Requested metric %v do not exist", metric_id)
	}
	lock.RLock()
	//We hole file is being written in memmory, that's bad.
	//A better solution would be to use binary search over the file, using FSEEK

	lines, err := read_into_lines(file_name)
	lock.RUnlock()

	if err != nil {
		return nil, err
	}

	//Lineal search over memmory
	for _, line := range lines {
		metric, err := decode_metric(line, metric_id)
		if err != nil {
			return nil, fmt.Errorf("Error decoding metric %v on Query. %v", metric_id, err)
		}
		if metric.Timestamp >= from {
			result = append(result, metric)
		}
		if metric.Timestamp > to {
			break
		}
	}

	return result, nil
}
