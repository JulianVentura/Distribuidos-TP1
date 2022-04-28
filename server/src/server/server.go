package server

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/acceptor"
	"distribuidos/tp1/server/src/alarm"
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
	Alarm_manager_queue_size  uint
	//Database
	DB_epoch_duration time.Duration
	DB_files_path     string
	//Alarm
	Alarm_period      time.Duration
	Alarm_config_file string
}

type ServerQueues struct {
	Dispatcher_queue    chan messages.DispatcherMessage
	Con_worker_queue    []chan messages.ConnectionWorkerMessage
	Write_db_queue      chan messages.WriteDatabaseMessage
	Read_db_queue       chan messages.ReadDatabaseMessage
	Alarm_manager_queue chan messages.AlarmManagerMessage
}

type Server struct {
	acceptor      *acceptor.Acceptor
	dispatcher    *dispatcher.Dispatcher
	conn_workers  []*connection.ConnectionWorker
	database      *database.Database
	alarm_manager *alarm.AlarmManager
}

func Start(config ServerConfig) (*Server, error) {

	self := &Server{
		acceptor:     nil,
		conn_workers: nil,
		dispatcher:   nil,
		database:     nil,
	}
	//Queues initialization
	queues := start_queues(&config)

	//Database initialization
	err := self.start_database(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Database", err)
	}
	//Connection Workers initialization
	err = self.start_connection_workers(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Connection Workers", err)
	}
	//Alarm Manager initialization
	err = self.start_alarm_manager(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Alarm Manager", err)
	}
	//Dispatcher initialization
	err = self.start_dispatcher(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Dispatcher", err)
	}
	//Acceptor initialization
	err = self.start_acceptor(&config, &queues)
	if err != nil {
		self.Finish()
		return nil, Err.Ctx("Error starting Acceptor", err)
	}

	return self, nil
}

func (self *Server) Finish() {
	//Close the Acceptor
	if self.acceptor != nil {
		self.acceptor.Finish()
	}
	//Close the Alarm Manager
	if self.alarm_manager != nil {
		self.alarm_manager.Finish()
	}
	//Close the Connection Workers
	if self.conn_workers != nil {
		for _, conn := range self.conn_workers {
			conn.Finish()
		}
	}
	//Close the Database
	if self.database != nil {
		self.database.Finish()
	}
	//Close the Dispatcher
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
		Dispatcher_queue:    make(chan messages.DispatcherMessage, config.Dispatcher_queue_size),
		Con_worker_queue:    con_worker_queue,
		Write_db_queue:      make(chan messages.WriteDatabaseMessage, config.DB_writers_queue_size),
		Read_db_queue:       make(chan messages.ReadDatabaseMessage, config.DB_readers_queue_size),
		Alarm_manager_queue: make(chan messages.AlarmManagerMessage, config.Alarm_manager_queue_size),
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

	d_config := dispatcher.DispatcherConfig{}
	dispatcher, err := dispatcher.Start(d_config, queues.Dispatcher_queue, queues.Con_worker_queue, queues.Alarm_manager_queue)

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

func (self *Server) start_alarm_manager(config *ServerConfig, queues *ServerQueues) error {
	cfg := alarm.AlarmManagerConfig{
		Id:               config.Con_worker_number,
		Period:           config.Alarm_period,
		Config_file_path: config.Alarm_config_file,
	}
	alarm_manager, err := alarm.StartAlarmManager(cfg, queues.Alarm_manager_queue, queues.Read_db_queue)

	if err != nil {
		return err
	}

	self.alarm_manager = alarm_manager

	return nil
}
