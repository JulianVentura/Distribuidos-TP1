package messages

import (
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/models"
)

type DispatcherMessage interface {
	implementsDispatcherMessage()
}

type ConnectionWorkerMessage interface {
	implementsConnectionWorkerMessage()
}

type WriteDatabaseMessage interface {
	implementsWriteDatabaseMessage()
}

type ReadDatabaseMessage interface {
	implementsReadDatabaseMessage()
}

type NewConnection struct {
	Skt *socket.TCPConnection
}

type ConnectionFinished struct {
	Conn_worker_id uint
}

type QueryResponse struct {
	Conn_worker_id uint
	Response       string //Change
}

type NewMetric struct {
	Conn_worker_id uint
	Metric         models.Metric
}

type NewQuery struct {
	Conn_worker_id uint
	Query          models.Query
}

func (NewConnection) implementsDispatcherMessage()       {}
func (NewConnection) implementsConnectionWorkerMessage() {}

func (ConnectionFinished) implementsDispatcherMessage() {}

func (QueryResponse) implementsDispatcherMessage()       {}
func (QueryResponse) implementsConnectionWorkerMessage() {}

func (NewMetric) implementsWriteDatabaseMessage() {}
func (NewMetric) implementsReadDatabaseMessage()  {}
