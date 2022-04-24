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
	DB_readers_queue_size uint
	DB_writers_queue_size uint
	DB_mergers_queue_size uint
	Con_worker_queue_size uint
	Dispatcher_queue_size uint
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

	//Iniciamos las colas de mensajes
	queues := start_queues(&config)

	//Iniciamos la base de datos
	db, err := start_database(&config, &queues)
	if err != nil {
		return nil, Err.Ctx("Error starting Database", err)
	}
	//Iniciamos a los connection workers
	conn_workers, err := start_connection_workers(&config, &queues)
	if err != nil {
		return nil, Err.Ctx("Error starting Connection Workers", err)
	}
	//Iniciamos al servicio de alarma
	//TODO
	//Iniciamos al dispatcher
	dispatcher, err := start_dispatcher(&config, &queues)
	if err != nil {
		return nil, Err.Ctx("Error starting Dispatcher", err)
	}
	//Iniciamos al aceptador
	acceptor, err := start_acceptor(&config, &queues)
	if err != nil {
		return nil, Err.Ctx("Error starting Acceptor", err)
	}

	server := &Server{
		acceptor:     acceptor,
		conn_workers: conn_workers,
		dispatcher:   dispatcher,
		database:     db,
	}
	return server, nil
}

func (self *Server) Finish() {
	//Cerrar al aceptador
	self.acceptor.Finish()
	//Cerrar las conexiones
	for _, conn := range self.conn_workers {
		conn.Finish()
	}
	//Cerrar la base de datos
	//Cerrar el dispatcher
	self.dispatcher.Finish()
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

func start_database(config *ServerConfig, queues *ServerQueues) (*database.Database, error) {
	db_config := database.DatabaseConfig{
		N_readers: config.DB_readers_number,
		N_writers: config.DB_writers_number,
		N_mergers: config.DB_mergers_number,
	}
	return database.Start(db_config, queues.Read_db_queue, queues.Write_db_queue, queues.Dispatcher_queue)
}

func start_connection_workers(config *ServerConfig, queues *ServerQueues) ([]*connection.ConnectionWorker, error) {
	connections := make([]*connection.ConnectionWorker, config.Con_worker_number)
	w_config := connection.ConnectionWorkerConfig{
		Id:                 0,
		Connection_timeout: config.Client_conn_timeout,
	}
	for i := uint(0); i < config.Con_worker_number; i++ {
		w_config.Id = i
		conn, err := connection.StartConnectionWorker(w_config, queues.Con_worker_queue[i], queues.Dispatcher_queue)
		if err != nil {
			//TODO:
			//Limpiar acá las conexiones ya creadas, posiblemente cerrando el canal
			return connections, err
		}
		connections[i] = conn
	}

	return connections, nil
}

func start_dispatcher(config *ServerConfig, queues *ServerQueues) (*dispatcher.Dispatcher, error) {

	//TODO: Check this
	d_config := dispatcher.DispatcherConfig{}
	return dispatcher.Start(d_config, queues.Dispatcher_queue, queues.Con_worker_queue)
}

func start_acceptor(config *ServerConfig, queues *ServerQueues) (*acceptor.Acceptor, error) {
	acceptor_config := acceptor.AcceptorConfig{
		Host: config.Server_ip,
		Port: config.Server_port,
	}
	return acceptor.Start(acceptor_config, queues.Dispatcher_queue)
}
