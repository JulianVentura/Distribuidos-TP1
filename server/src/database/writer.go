package database

import (
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type writer struct {
	epoch              uint64
	epoch_mods         map[string]bool
	epoch_end          <-chan time.Time
	config             *writerConfig
	event_queue        chan messages.WriteDatabaseMessage
	merger_admin_queue chan messages.MergerAdminMessage
	quit               chan bool
	has_finished       chan bool
}

type writerConfig struct {
	id             uint
	epoch_duration time.Duration
	files_path     string
}

func start_writer(
	config writerConfig,
	event_queue chan messages.WriteDatabaseMessage,
	merger_admin_queue chan messages.MergerAdminMessage) (*writer, error) {

	wr := &writer{
		epoch:              0,
		epoch_end:          time.After(config.epoch_duration),
		config:             &config,
		event_queue:        event_queue,
		epoch_mods:         make(map[string]bool),
		merger_admin_queue: merger_admin_queue,
		quit:               make(chan bool, 2),
		has_finished:       make(chan bool, 2),
	}

	go wr.run()

	log.Infof("Database writer %v started\n", wr.config.id)
	return wr, nil
}

func (self *writer) finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *writer) run() {

Loop:
	for {
		select {
		case <-self.quit:
			self.quit <- true // De esta forma propagamos la señal
			break Loop
		case <-self.epoch_end:
			self.change_epoch()
		case message := <-self.event_queue:
			switch m := message.(type) {
			case *messages.NewMetric:
				self.handle_new_metric(m)
			default:
				log.Error("Message received on DatabaseWriter is not recognized")
			}
		}
	}

	//If quit signal has been received, we must first empty the events queue
	self.handle_remaining_events()

	log.Infof("Database writer %v finished\n", self.config.id)
	self.has_finished <- true
}

func (self *writer) handle_new_metric(metric *messages.NewMetric) {
	file_name := fmt.Sprintf("%v/%v_%v_%v", self.config.files_path, self.config.id, self.epoch, metric.Metric.Id)

	_, exists := self.epoch_mods[metric.Metric.Id]
	if !exists {
		self.epoch_mods[metric.Metric.Id] = true
	}

	timestamp := time.Now().UnixMilli()
	encoded := encode_metric(&metric.Metric, timestamp) + "\n" //We add final \n to string

	fd, err := os.OpenFile(file_name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		log.Errorf("Database writer couldn't open %v file: %v", file_name, err)
		return
	}

	defer fd.Close()

	err = write_whole_string(fd, encoded)

	if err != nil {
		log.Errorf("Database writer couldn't write on %v file: %v", file_name, err)
		return
	}
}

func (self *writer) change_epoch() {
	//Notify epoch change to Merge Admin
	log.Debugf("Worker %v changes epoch", self.config.id)
	modified := make([]string, 0, len(self.epoch_mods))
	for metric := range self.epoch_mods {
		modified = append(modified, metric)
	}
	self.merger_admin_queue <- &messages.EpochEnd{
		Writer_id:        self.config.id,
		Epoch:            self.epoch,
		Modified_metrics: modified,
	}
	//Epoch change
	self.epoch++
	//Epoch modification slice reset
	self.epoch_mods = make(map[string]bool)
	//Epoch timer reset
	self.epoch_end = time.After(self.config.epoch_duration)
}

func (self *writer) handle_remaining_events() {
Loop:
	for {
		select {
		case <-self.epoch_end:
			self.change_epoch()
		case message := <-self.event_queue:
			switch m := message.(type) {
			case *messages.NewMetric:
				self.handle_new_metric(m)
			default:
				log.Error("Message received on DatabaseWriter is not recognized")
			}
		default:
			break Loop
		}
	}

	self.change_epoch()
}
