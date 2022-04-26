package server

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/acceptor"
	"distribuidos/tp1/server/src/connection"
	"distribuidos/tp1/server/src/database"
	"distribuidos/tp1/server/src/dispatcher"
	"distribuidos/tp1/server/src/messages"
	"time"
)

type ServerConfig struct {
	//Logging
	Log_level string
	//Connection
	Server_ip           string
	Server_port         string
	Client_conn_timeout time.Duration
	//Number of workers
	Con_worker_number uint
	DB_writers_number uint
	DB_readers_number uint
	DB_mergers_number uint
	//Queues sizes
	DB_readers_queue_size     uint
	DB_writers_queue_size     uint
	DB_merge_admin_queue_size uint
	DB_mergers_queue_size     uint
	Con_worker_queue_size     uint
	Dispatcher_queue_size     uint
	//Database
	DB_epoch_duration time.Duration
	DB_files_path     string
}

type ServerQueues struct {
	Dispatcher_queue chan messages.DispatcherMessage
	Con_worker_queue []chan messages.ConnectionWorkerMessage
	Write_db_queue   chan messages.WriteDatabaseMessage
	Read_db_queue    chan messages.ReadDatabaseMessage
	//Faltan los mergers y alarm worker
}

type Server struct {
	acceptor     *acceptor.Acceptor
	dispatcher   *dispatcher.Dispatcher
	conn_workers []*connection.ConnectionWorker
	database     *database.Database
}

// TODO: Realizar limpieza en caso de error y modularizar
func Start(config ServerConfig) (*Server, error) {

	//Iniciamos una estructura vacia

	self := &Server{
		acceptor:     nil,
		conn_workers: nil,
		dispatcher:   nil,
		database:     nil,
	}
	//Iniciamos las colas de mensajes
	queues := start_queues(&config)

	//Iniciamos la base de datos
	err := self.start_database(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Database", err)
	}
	//Iniciamos a los connection workers
	err = self.start_connection_workers(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Connection Workers", err)
	}
	//Iniciamos al servicio de alarma
	//TODO
	//Iniciamos al dispatcher
	err = self.start_dispatcher(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Dispatcher", err)
	}
	//Iniciamos al aceptador
	err = self.start_acceptor(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Acceptor", err)
	}

	return self, nil
}

func (self *Server) Finish() {
	//Cerrar al aceptador
	if self.acceptor != nil {
		self.acceptor.Finish()
	}
	//Cerrar las conexiones
	if self.conn_workers != nil {
		for _, conn := range self.conn_workers {
			conn.Finish()
		}
	}
	//Cerrar la base de datos
	if self.database != nil {
		self.database.Finish()
	}
	//Cerrar el dispatcher
	if self.dispatcher != nil {
		self.dispatcher.Finish()
	}
}

func start_queues(config *ServerConfig) ServerQueues {

	con_worker_queue := make([]chan messages.ConnectionWorkerMessage, config.Con_worker_number)
	for i := uint(0); i < config.Con_worker_number; i++ {
		con_worker_queue[i] = make(chan messages.ConnectionWorkerMessage, config.Con_worker_queue_size)
	}

	return ServerQueues{
		Dispatcher_queue: make(chan messages.DispatcherMessage, config.Dispatcher_queue_size),
		Con_worker_queue: con_worker_queue,
		Write_db_queue:   make(chan messages.WriteDatabaseMessage, config.DB_writers_queue_size),
		Read_db_queue:    make(chan messages.ReadDatabaseMessage, config.DB_readers_queue_size),
	}
}

func (self *Server) start_database(config *ServerConfig, queues *ServerQueues) error {
	db_config := database.DatabaseConfig{
		N_readers:              config.DB_readers_number,
		N_writers:              config.DB_writers_number,
		N_mergers:              config.DB_mergers_number,
		Merge_admin_queue_size: config.DB_merge_admin_queue_size,
		Mergers_queue_size:     config.DB_mergers_queue_size,
		Epoch_duration:         config.DB_epoch_duration,
		Files_path:             config.DB_files_path,
	}
	db, err := database.Start_database(db_config, queues.Read_db_queue, queues.Write_db_queue, queues.Dispatcher_queue)
	if err != nil {
		return err
	}

	self.database = db

	return nil
}

func (self *Server) start_connection_workers(config *ServerConfig, queues *ServerQueues) error {
	self.conn_workers = make([]*connection.ConnectionWorker, config.Con_worker_number)
	w_config := connection.ConnectionWorkerConfig{
		Id:                 0,
		Connection_timeout: config.Client_conn_timeout,
	}
	for i := uint(0); i < config.Con_worker_number; i++ {
		w_config.Id = i
		conn, err := connection.StartConnectionWorker(
			w_config,
			queues.Con_worker_queue[i],
			queues.Dispatcher_queue,
			queues.Write_db_queue,
			queues.Read_db_queue)
		if err != nil {
			return err
		}
		self.conn_workers[i] = conn
	}

	return nil
}

func (self *Server) start_dispatcher(config *ServerConfig, queues *ServerQueues) error {

	//TODO: Check this
	d_config := dispatcher.DispatcherConfig{}
	dispatcher, err := dispatcher.Start(d_config, queues.Dispatcher_queue, queues.Con_worker_queue)

	if err != nil {
		return err
	}

	self.dispatcher = dispatcher

	return nil
}

func (self *Server) start_acceptor(config *ServerConfig, queues *ServerQueues) error {
	acceptor_config := acceptor.AcceptorConfig{
		Host: config.Server_ip,
		Port: config.Server_port,
	}
	acceptor, err := acceptor.Start(acceptor_config, queues.Dispatcher_queue)

	if err != nil {
		return err
	}

	self.acceptor = acceptor

	return nil
}
