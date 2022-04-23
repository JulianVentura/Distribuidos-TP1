package database

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/messages"
)

// Change
type Database struct {
	file               int
	query_queue        chan messages.ReadDatabaseMessage
	event_queue        chan messages.WriteDatabaseMessage
	dispatcher_queue   chan messages.DispatcherMessage
	merger_admin_queue chan string
	mergers_queue      chan string
}

type DatabaseConfig struct {
	N_writers uint
	N_readers uint
	N_mergers uint
}

//TODO: Es un mock, falta definir bien la api y realizar liberacion de recursos en caso normal y en caso de error
func Start(
	config DatabaseConfig,
	query_queue chan messages.ReadDatabaseMessage,
	event_queue chan messages.WriteDatabaseMessage,
	dispatcher_queue chan messages.DispatcherMessage,
) (*Database, error) {
	err := start_readers(&config)
	if err != nil {
		return nil, Err.Ctx("Error starting database reader workers", err)
	}
	err = start_writers(&config)
	if err != nil {
		return nil, Err.Ctx("Error starting database writer workers", err)
	}
	mergers_queue, err := start_mergers(&config)
	if err != nil {
		return nil, Err.Ctx("Error starting database merger workers", err)
	}
	merger_admin_queue, err := start_merger_admin(&config)
	if err != nil {
		return nil, Err.Ctx("Error starting database merger admin", err)
	}

	return &Database{
		file:               4,
		event_queue:        event_queue,
		query_queue:        query_queue,
		dispatcher_queue:   dispatcher_queue,
		mergers_queue:      mergers_queue,
		merger_admin_queue: merger_admin_queue,
	}, nil
}

//Change
func start_readers(config *DatabaseConfig) error {
	return nil
}

//Change
func start_writers(config *DatabaseConfig) error {
	return nil
}

//Change
func start_mergers(config *DatabaseConfig) (chan string, error) {

	return make(chan string, 1), nil
}

//Change
func start_merger_admin(config *DatabaseConfig) (chan string, error) {

	return make(chan string, 1), nil
}
