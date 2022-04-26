package database

import (
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"time"

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

//FALTA: Ver el tema de la estructura para los locks
// Se puede hacer una primera version sin ella para chequear que se esten mergeando correctamente los archivos
type mergerAdmin struct {
	mergeos            map[string]*mergeEntry
	epoch              uint64
	arrivals           uint
	modified_metrics   map[string]bool
	n_modified_metrics uint
	new_metrics        []string
	config             mergerAdminConfig
	queue              chan messages.MergerAdminMessage
	mergers_queue      chan messages.MergersMessage
	quit               chan bool
	has_finished       chan bool
}

func startMergerAdmin(
	config mergerAdminConfig,
	queue chan messages.MergerAdminMessage,
	mergers_queue chan messages.MergersMessage) (*mergerAdmin, error) {

	m := &mergerAdmin{
		mergeos:            make(map[string]*mergeEntry),
		epoch:              0,
		arrivals:           0,
		modified_metrics:   make(map[string]bool),
		n_modified_metrics: 0,
		new_metrics:        make([]string, 0, 128),
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

	// Si self.modified_metrics == 0, quiere decir que no nos encontramos en medio de un procesamiento.
	// Como se pide que previamente hayan finalizado todos los escritores, no se pueden estar generando
	// nuevas escrituras.
	// Si existe alguna escritura sin procesar, tiene que estar en la cola de entrada.
	// Por ende nos bloqueamos en la cola y, si recibimos cualquier cosa que no sea un EpochEnd, la ignoramos (En realidad seria un error eso)
	// Si recibimos un EpochEnd, nos vamos a procesarlo (Eso implicaría hacer un bucle igualito al de arriba, pero sin la salida por el canal quit)
	// Al regresar de dicho procesamiento, volvemos a bloquearnos en el canal.

	// Si en algúna de esas iteraciones, (cuando finalizamos un epoch) nos bloqueamos en la cola por más de T duración, entonces salimos. (configurable)

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
		//If the queue is empty, then we quit because of timeout
		case <-time.After(time.Second * 1): //TODO: Config file //TODO: Quizás no hace falta el timeout si ponemos un default
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

// 1- Si el número de la sesion del writer es mayor al de la sesion actual:
// 	Volver a encolar el mensaje, pues todavía no se llegó a dicha etapa. //Como alternativa se podría encolar en una cola dedicada
// 2- Aumentar uno a la variable que indica número de mensajes de workers recibidos para esta sesion ARRIVALS
// 3- Por cada métrica modificada por el writer:
// 	1- Aumentar en uno el número de métricas actualizadas MODIFIED_METRICS
// 	2- Acceder a la estructura MERGEOS y:
// 		- Si la métrica no existía, entonces se crea una nueva entrada en la estructura MERGEOS y se la añade a una
// 			estructura auxiliar donde figuren las métricas nuevas
// 		- Sumar uno a la variable "number_merges_to_do" y añadir el nombre del archivo completo (id, sesion, metric)
// 			a la lista de archivos a mergear
// 		- Si la lista de archivos a mergear tiene más de un elemento, disparar una tarea merge(metric_id, file_1, file_2) eliminando dichos archivos
// 		- Caso contrario, si ARRIVALS == N_Writers:
// 			Disparar un append(lock, db_file, file_to_append) a un merger, eliminando el archivo del vector
// 4- Si ARRVALS == N_writers && MODIFIED_METRICS == 0: sesion_actual++
// 	- Cuando el merge admin reciba un mensaje mergeFinished de un merger:
// 		- Acceder a la estructura MERGES de la metrica y disminuir en una unidad n_merges_to_do
// 		- Si la variable n_merges_to_do llegó a 1 && ARRIVALS == N_Writers
// 			Generar una nueva tarea append(lock, db_file, file_to_append)
// 	- Cuando el merge admin reciba un mensaje AppendFinished de un merger:
// 		- Reiniciar la entrada de MERGEOS para esa métrica a un valor de cero para todos sus campos
// 		- Disminuir en una unidad la variable MODIFIED_METRICS
// 		- Si dicha métrica es nueva (se encuentra en la estructura auxiliar) hay que notificar a los lectores de la BDD de su existencia
// 		- Si la variable MODIFIED_METRICS llegó a cero, aumentar en una únidad el número de sesión

func (self *mergerAdmin) handle_merge_finished(m *messages.MergeFinished) error {
	log.Debugf("MegerAdmin: Received MergeFinished message: %v", m)
	metric_id := m.Metric_id

	entry, exists := self.mergeos[metric_id]
	if !exists {
		//TODO: Error fatal, panic?
		return fmt.Errorf("ERROR: Received a MergeFinished message but metric_id was not found")
	}

	entry.number_merges_to_do--
	entry.files_to_merge = append(entry.files_to_merge, m.File)

	self.send_merge_or_append_if_possible(entry, metric_id)

	self.mergeos[metric_id] = entry

	return nil
}

func (self *mergerAdmin) handle_append_finished(m *messages.AppendFinished) error {
	log.Debugf("MegerAdmin: Received AppendFinished message: %v", m)
	metric_id := m.Metric_id

	entry, exists := self.mergeos[metric_id]
	if !exists {
		//TODO: Error fatal, panic?
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
	//TODO: Ver si cambiar esto
	log.Debugf("MegerAdmin: Received EpochEnd message: %v", m)
	if m.Epoch > self.epoch {
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
		self.new_metrics = append(self.new_metrics, metric_id)
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
	log.Debugf("MergerAdmin: Enviando merge %v", msg)
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
	msg := &messages.Append{
		Metric_id:      metric_id,
		DB_file:        db_file,
		File_to_append: entry.files_to_merge[0],
	}
	log.Debugf("MergerAdmin: Enviando append %v", msg)
	self.mergers_queue <- msg
	entry.files_to_merge = entry.files_to_merge[1:]

	return true
}

func (self *mergerAdmin) reset_epoch() {
	log.Debugf("MergeAdmin: Epoch Change, Result of Mergeos:")
	for k, v := range self.mergeos {
		fmt.Printf("%v: %v\n", k, v.files_to_merge)
	}
	self.epoch++
	self.arrivals = 0
	self.n_modified_metrics = 0
	self.modified_metrics = make(map[string]bool)
}
