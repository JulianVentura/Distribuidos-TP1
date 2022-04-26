package database

import (
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type mergerConfig struct {
	id         uint
	files_path string
}

//TODO: Ver el tema de la estructura para los locks
// Se puede hacer una primera version sin ella para chequear que se esten mergeando correctamente los archivos
type merger struct {
	admin_queue      chan messages.MergerAdminMessage
	queue            chan messages.MergersMessage
	completed_merges uint
	config           mergerConfig
	quit             chan bool
	has_finished     chan bool
}

func start_merger(
	config mergerConfig,
	admin_q chan messages.MergerAdminMessage,
	queue chan messages.MergersMessage) (*merger, error) {

	m := &merger{
		queue:            queue,
		admin_queue:      admin_q,
		config:           config,
		completed_merges: 0,
		quit:             make(chan bool, 2),
		has_finished:     make(chan bool, 2),
	}

	go m.run()

	log.Infof("Database merger %v started\n", m.config.id)
	return m, nil
}

func (self *merger) finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *merger) run() {

Loop:
	for {
		select {
		case <-self.quit:
			break Loop
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.Merge:
				err := self.handle_merge(m)
				if err != nil {
					log.Errorf("Fatal error on merge operation: %v", err)
				}
			case *messages.Append:
				err := self.handle_append(m)
				if err != nil {
					log.Errorf("Fatal error on append operation: %v", err)
				}
			default:
				log.Error("Expected an EpochEnd message on MergerAdmin but received another one")
			}
		}
	}

	//We received quit signal, but first we have to clean the queue
End:

	for {
		select {
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.Merge:
				err := self.handle_merge(m)
				if err != nil {
					log.Errorf("Fatal error on merge operation: %v", err)
				}
			case *messages.Append:
				err := self.handle_append(m)
				if err != nil {
					log.Errorf("Fatal error on append operation: %v", err)
				}
			default:
				log.Error("Expected an EpochEnd message on MergerAdmin but received another one")
			}
		default:
			break End //If the queue is empty, we finished
		}
	}

	log.Infof("Database merger %v finished\n", self.config.id)
	self.has_finished <- true
}

func (self *merger) handle_merge(m *messages.Merge) error {
	log.Debugf("Merger: Received merge message: %v", m)
	lines_1, err := read_into_lines(m.File_1)
	if err != nil {
		return err
	}
	lines_2, err := read_into_lines(m.File_2)
	if err != nil {
		return err
	}
	result, err := merge(lines_1, lines_2)
	if err != nil {
		return err
	}

	result_file_path := fmt.Sprintf("%v/%v_%v_%v_MERGED",
		self.config.files_path,
		self.config.id,
		self.completed_merges,
		m.Metric_id)
	self.completed_merges++

	//TODO: Delete de los archivos file_1 y file_2

	err = os.Remove(m.File_1)
	if err != nil {
		log.Errorf("Merger: Failed to remove %v file", m.File_1)
	}
	err = os.Remove(m.File_2)
	if err != nil {
		log.Errorf("Merger: Failed to remove %v file", m.File_2)
	}
	err = write_from_lines(result_file_path, result)
	if err != nil {
		return err
	}

	msg := &messages.MergeFinished{
		Metric_id: m.Metric_id,
		File:      result_file_path,
	}

	self.admin_queue <- msg

	log.Debugf("Merger: Send MergeFinished %v", msg)
	return nil
}

func (self *merger) handle_append(m *messages.Append) error {

	db_file_path := fmt.Sprintf("%v/%v", self.config.files_path, m.Metric_id)

	db_file, err := os.OpenFile(db_file_path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

	if err != nil {
		return fmt.Errorf("Couldn't create file. %v\n", err)
	}
	defer db_file.Close()

	file_to_append, err := os.OpenFile(m.File_to_append, os.O_RDONLY, 0666)

	if err != nil {
		return fmt.Errorf("Couldn't create file. %v\n", err)
	}

	defer file_to_append.Close()

	_, err = io.Copy(db_file, file_to_append) //Copies until EOF is reached or error occurs

	if err != nil {
		return fmt.Errorf("Error appending content into %v. %v\n", db_file, err)
	}

	//TODO: Delete del archivo previo file_to_append

	err = os.Remove(m.File_to_append)
	if err != nil {
		log.Errorf("Merger: Failed to remove %v file", m.File_to_append)
	}
	msg := &messages.AppendFinished{
		Metric_id: m.Metric_id,
	}

	self.admin_queue <- msg

	log.Debugf("Merger: Send AppendFinished %v", msg)

	return nil
}

func read_into_lines(file string) ([]string, error) {
	bytes, err := ioutil.ReadFile(file) //File descriptor is automatically closed
	if err != nil {
		return nil, fmt.Errorf("Couldn't open file %v. %v\n", file, err)
	}

	string_result := string(bytes)

	splits := strings.Split(string_result, "\n")

	l := len(splits)

	if l > 0 {
		splits = splits[:(l - 1)] //Filter out \n
	}

	return splits, nil
}

func write_from_lines(file string, lines []string) error {
	string_result := strings.Join(lines, "\n") + "\n" //Add last \n
	fd, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	if err != nil {
		return fmt.Errorf("Couldn't create file %v. %v\n", file, err)
	}
	defer fd.Close()

	err = write_whole_string(fd, string_result)

	if err != nil {
		return fmt.Errorf("Couldn't write on file %v. %v\n", file, err)
	}

	return nil
}

func merge(a []string, b []string) ([]string, error) {

	result := make([]string, len(a)+len(b))

	i := 0 //a pointer
	j := 0 //b pointer

	a_top := len(a)
	b_top := len(b)

	for i < a_top && j < b_top {
		enc_a, err := decode_timestamp(a[i])
		if err != nil {
			return nil, err
		}
		enc_b, err := decode_timestamp(b[j])
		if err != nil {
			return nil, err
		}
		if enc_a <= enc_b {
			result[i+j] = a[i]
			i++
		} else {
			result[i+j] = b[j]
			j++
		}
	}

	for i < a_top {
		result[i+j] = a[i]
		i++
	}

	for j < b_top {
		result[i+j] = b[j]
		j++
	}

	return result, nil
}
