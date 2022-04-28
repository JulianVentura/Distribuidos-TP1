package database

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/messages"
	"time"
)

type Database struct {
	writers      []*writer
	readers      []*reader
	merger_admin *mergerAdmin
	mergers      []*merger
}

type DatabaseConfig struct {
	N_writers              uint
	N_readers              uint
	N_mergers              uint
	Merge_admin_queue_size uint
	Mergers_queue_size     uint
	Epoch_duration         time.Duration
	Files_path             string
}

func Start_database(
	config DatabaseConfig,
	query_queue chan messages.ReadDatabaseMessage,
	event_queue chan messages.WriteDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage,
) (*Database, error) {

	merger_admin_queue := make(chan messages.MergerAdminMessage, config.Merge_admin_queue_size)
	mergers_queue := make(chan messages.MergersMessage, config.Mergers_queue_size)

	db := &Database{}

	file_manager, err := NewFileManager(config.Files_path)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error FileManager", err)
	}

	err = db.start_writers(&config, event_queue, merger_admin_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database writer workers", err)
	}
	err = db.start_readers(&config, file_manager, query_queue, dispatcher_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database reader workers", err)
	}
	err = db.start_mergers(&config, merger_admin_queue, mergers_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database merger workers", err)
	}
	err = db.start_merger_admin(&config, file_manager, merger_admin_queue, mergers_queue)
	if err != nil {
		db.Finish()
		return nil, Err.Ctx("Error starting database merger admin", err)
	}

	return db, nil
}

func (self *Database) Finish() {
	// Finishing readers
	if self.readers != nil {
		for _, v := range self.readers {
			v.finish()
		}
	}

	//Finishing writers
	if self.writers != nil {
		for _, v := range self.writers {
			v.finish()
		}
	}

	//Finishing merger admin
	if self.merger_admin != nil {
		self.merger_admin.finish()
	}

	// Finishing mergers
	if self.mergers != nil {
		for _, v := range self.mergers {
			v.finish()
		}
	}

}

func (self *Database) start_readers(
	config *DatabaseConfig,
	file_manager *FileManager,
	query_queue chan messages.ReadDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage,
) error {
	cfg := readerConfig{
		id:         0,
		files_path: config.Files_path,
	}
	self.readers = make([]*reader, config.N_readers)

	for i := uint(0); i < config.N_readers; i++ {
		cfg.id = i
		r, err := start_reader(cfg, file_manager, query_queue, dispatcher_queue)
		if err != nil {
			return err
		}
		self.readers[i] = r
	}
	return nil
}

func (self *Database) start_writers(
	config *DatabaseConfig,
	event_queue chan messages.WriteDatabaseMessage,
	merger_admin_queue chan messages.MergerAdminMessage) error {

	cfg := writerConfig{
		id:             0,
		epoch_duration: config.Epoch_duration,
		files_path:     config.Files_path,
	}
	self.writers = make([]*writer, config.N_writers)

	for i := uint(0); i < config.N_writers; i++ {
		cfg.id = i
		w, err := start_writer(cfg, event_queue, merger_admin_queue)
		if err != nil {
			return err
		}
		self.writers[i] = w
	}
	return nil
}

func (self *Database) start_mergers(
	config *DatabaseConfig,
	merger_admin_queue chan messages.MergerAdminMessage,
	mergers_q chan messages.MergersMessage) error {

	cfg := mergerConfig{
		id:         0,
		files_path: config.Files_path,
	}

	self.mergers = make([]*merger, config.N_mergers)

	for i := uint(0); i < config.N_mergers; i++ {
		cfg.id = i
		m, err := start_merger(cfg, merger_admin_queue, mergers_q)
		if err != nil {
			return err
		}
		self.mergers[i] = m
	}

	return nil
}

func (self *Database) start_merger_admin(
	config *DatabaseConfig,
	file_manager *FileManager,
	merger_admin_q chan messages.MergerAdminMessage,
	mergers_q chan messages.MergersMessage) error {

	cfg := mergerAdminConfig{
		n_writers:  config.N_writers,
		files_path: config.Files_path,
	}

	merger_admin, err := startMergerAdmin(cfg, file_manager, merger_admin_q, mergers_q)
	if err != nil {
		return err
	}

	self.merger_admin = merger_admin

	return nil
}

// Riesgo de deadlock si se llena la cola de los mergers y del Merger admin al mismo tiempo
//
// 	Se puede solucionar desde los mergers usando un select que envíe a la cola del admin y como caso default
// 		almacene en un slice local a ese merger.
// 	Cuando esto suceda habrá que pasar a un modo "deadlock avoidance"
// 	Método Deadlock Avoidance:
// 		- Intentar enviar al admin el primer mensaje de la cola
// 			- Si el mensaje es enviado, eliminarlo de la cola y continuar el loop
// 			- Si el mensaje no es enviado, entonces el admin sigue bloqueado:
// 				- Sacar un mensaje de la cola de los mergers y encolarlo en la cola auxiliar interna. Continuar el loop
