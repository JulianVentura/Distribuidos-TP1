package database

import (
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type mergerAdminConfig struct {
	n_writers  uint
	files_path string
}

type mergeEntry struct {
	number_merges_to_do uint
	files_to_merge      []string
}

type mergerAdmin struct {
	mergeos            map[string]*mergeEntry
	epoch              uint64
	arrivals           uint
	modified_metrics   map[string]bool
	n_modified_metrics uint
	new_metrics        map[string]bool
	file_manager       *FileManager
	config             mergerAdminConfig
	queue              chan messages.MergerAdminMessage
	mergers_queue      chan messages.MergersMessage
	quit               chan bool
	has_finished       chan bool
}

func startMergerAdmin(
	config mergerAdminConfig,
	file_manager *FileManager,
	queue chan messages.MergerAdminMessage,
	mergers_queue chan messages.MergersMessage) (*mergerAdmin, error) {

	m := &mergerAdmin{
		mergeos:            make(map[string]*mergeEntry),
		epoch:              0,
		arrivals:           0,
		modified_metrics:   make(map[string]bool),
		n_modified_metrics: 0,
		new_metrics:        make(map[string]bool),
		file_manager:       file_manager,
		config:             config,
		queue:              queue,
		mergers_queue:      mergers_queue,
		quit:               make(chan bool, 2),
		has_finished:       make(chan bool, 2),
	}

	go m.run()

	log.Info("Merger Admin started")
	return m, nil
}

func (self *mergerAdmin) finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *mergerAdmin) run() {

Loop:
	for {
		select {
		case <-self.quit:
			break Loop
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.EpochEnd:
				self.handle_current_epoch(m)
			default:
				log.Error("Expected an EpochEnd message on MergerAdmin but received another one")
			}
		}
	}

	self.check_new_epoch_without_deadlock()

	log.Info("Merger Admin finished")
	self.has_finished <- true
}

func (self *mergerAdmin) check_new_epoch_without_deadlock() {
Loop:
	for {
		select {
		//Since writers have already finished (precondition), there will no be new write events
		//Then, if there is a new write event, it must be stored on the queue
		//Since we are sure that the last epoch has finished, we can only receive an EpochEnd message
		default:
			break Loop
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.EpochEnd:
				self.handle_current_epoch(m)
			default:
				log.Error("Expected an EpochEnd message on MergerAdmin but received another one")
			}
		}
	}
}

func (self *mergerAdmin) handle_current_epoch(ep_end *messages.EpochEnd) {

	start_epoch := self.epoch // We mark the start epoch so we know when should we finish

	self.handle_epoch_end(ep_end) // We make the first epoch handling

Loop:
	for {
		if start_epoch != self.epoch { // If the epoch has changed, then we finished
			break Loop
		}
		select {
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.EpochEnd:
				self.handle_epoch_end(m)
			case *messages.MergeFinished:
				self.handle_merge_finished(m)
			case *messages.AppendFinished:
				self.handle_append_finished(m)
			default:
				log.Error("Message received on MergerAdmin is not recognized")
			}
		}
	}
}

func (self *mergerAdmin) handle_merge_finished(m *messages.MergeFinished) error {
	metric_id := m.Metric_id

	entry, exists := self.mergeos[metric_id]
	if !exists {
		//This should never happen
		return fmt.Errorf("ERROR: Received a MergeFinished message but metric_id was not found")
	}

	entry.number_merges_to_do--
	entry.files_to_merge = append(entry.files_to_merge, m.File)

	self.send_merge_or_append_if_possible(entry, metric_id)

	self.mergeos[metric_id] = entry

	return nil
}

func (self *mergerAdmin) handle_append_finished(m *messages.AppendFinished) error {
	metric_id := m.Metric_id

	entry, exists := self.mergeos[metric_id]
	if !exists {
		//This should never happen
		return fmt.Errorf("ERROR: Received a MergeFinished message but metric_id was not found")
	}

	entry.number_merges_to_do = 0
	entry.files_to_merge = entry.files_to_merge[:0] //Cleaning the slice but keeping the underlaying memmory

	self.mergeos[metric_id] = entry

	self.n_modified_metrics--

	if self.n_modified_metrics == 0 { //This was the last metric of the epoch
		self.reset_epoch()
	}

	return nil
}

func (self *mergerAdmin) handle_epoch_end(m *messages.EpochEnd) {
	if m.Epoch > self.epoch {
		//If this happens too much, then maybe the configurations are wrong or the merges and merge admin can't keep up
		//Alternatively, we could choose to buffer this message in an auxiliar structure and handle
		//it later. That would be better but this is easier
		log.Warn("MergerAdmin: An EpochEnd message from a future epoch has been received, synchronization warning")
		self.queue <- m
	}
	self.arrivals++

	for _, metric_id := range m.Modified_metrics {
		self.handle_metric_modification(metric_id, m)
	}

	if self.arrivals == self.config.n_writers {
		for metric_id := range self.modified_metrics {
			entry := self.mergeos[metric_id]
			self.try_send_append(entry, metric_id)
		}
	}

	if self.arrivals == self.config.n_writers && self.n_modified_metrics == 0 {
		self.reset_epoch()
	}
}

func (self *mergerAdmin) handle_metric_modification(metric_id string, m *messages.EpochEnd) {

	_, exists := self.modified_metrics[metric_id]
	if !exists {
		self.modified_metrics[metric_id] = true
		self.n_modified_metrics++
	}

	//We mark it as modified
	self.modified_metrics[metric_id] = true

	entry, exists := self.mergeos[metric_id]
	if !exists {
		entry = &mergeEntry{
			number_merges_to_do: 0,
			files_to_merge:      make([]string, 0, self.config.n_writers),
		}
		self.mergeos[metric_id] = entry
		self.new_metrics[metric_id] = true
	}

	file_name := fmt.Sprintf("%v/%v_%v_%v", self.config.files_path, m.Writer_id, self.epoch, metric_id)

	entry.number_merges_to_do++
	entry.files_to_merge = append(entry.files_to_merge, file_name)

	self.try_send_merge(entry, metric_id)

	self.mergeos[metric_id] = entry
}

func (self *mergerAdmin) send_merge_or_append_if_possible(entry *mergeEntry, metric_id string) {

	//We want to send just one of them

	if self.try_send_merge(entry, metric_id) {
		return
	} else {
		_ = self.try_send_append(entry, metric_id)
	}
}

func (self *mergerAdmin) try_send_merge(entry *mergeEntry, metric_id string) bool {
	//If any of this conditions is true, then we can't send a merge
	not_possible := entry.number_merges_to_do < 2                  //We already merged all the files, append remains
	not_possible = not_possible || (len(entry.files_to_merge) < 2) //We must have at least two elements to merge

	if not_possible {
		return false
	}
	msg := &messages.Merge{
		Metric_id: metric_id,
		File_1:    entry.files_to_merge[0],
		File_2:    entry.files_to_merge[1],
	}
	self.mergers_queue <- msg
	entry.files_to_merge = entry.files_to_merge[2:]

	return true
}

func (self *mergerAdmin) try_send_append(entry *mergeEntry, metric_id string) bool {

	//If any of this conditions is true, then we can't send an append
	not_possible := entry.number_merges_to_do != 1                          //We are waiting for a merge message
	not_possible = not_possible || (self.arrivals != self.config.n_writers) //We didn't receive all EndEpoch messages from writers
	not_possible = not_possible || (len(entry.files_to_merge) != 1)         //We must have just one element to append

	if not_possible {
		return false
	}

	db_file := fmt.Sprintf("%v/%v", self.config.files_path, metric_id)

	_, is_new := self.new_metrics[metric_id]
	var file_lock *sync.RWMutex = nil
	if !is_new {
		file_lock, _ = self.file_manager.get_lock_for(metric_id)
	}

	msg := &messages.Append{
		Metric_id:      metric_id,
		DB_file:        db_file,
		DB_file_lock:   file_lock,
		File_to_append: entry.files_to_merge[0],
	}
	self.mergers_queue <- msg
	entry.files_to_merge = entry.files_to_merge[1:]

	return true
}

func (self *mergerAdmin) reset_epoch() {
	self.epoch++
	self.arrivals = 0
	self.n_modified_metrics = 0
	self.modified_metrics = make(map[string]bool)

	//We add the new metrics to file manager

	files := make([]string, 0, len(self.new_metrics))
	for file := range self.new_metrics {
		files = append(files, file)
	}
	if len(files) > 0 {
		_ = self.file_manager.add_new_files(files) //This can't fail
	}
	self.new_metrics = make(map[string]bool)
}
