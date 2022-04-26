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

type MergerAdminMessage interface {
	implementsMergerAdminMessage()
}

type MergersMessage interface {
	implementsMergersMessage()
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

type EpochEnd struct {
	Writer_id        uint
	Epoch            uint64
	Modified_metrics []string
}

type AppendFinished struct {
	Metric_id string
}

type MergeFinished struct {
	Metric_id string
	File      string
}

type Merge struct {
	Metric_id string
	File_1    string
	File_2    string
}

type Append struct {
	Metric_id      string
	File_to_append string
	DB_file        string
}

func (NewConnection) implementsDispatcherMessage()       {}
func (NewConnection) implementsConnectionWorkerMessage() {}

func (ConnectionFinished) implementsDispatcherMessage() {}

func (QueryResponse) implementsDispatcherMessage()       {}
func (QueryResponse) implementsConnectionWorkerMessage() {}

func (NewMetric) implementsWriteDatabaseMessage() {}
func (NewMetric) implementsReadDatabaseMessage()  {}

func (EpochEnd) implementsMergerAdminMessage()       {}
func (MergeFinished) implementsMergerAdminMessage()  {}
func (AppendFinished) implementsMergerAdminMessage() {}

func (Merge) implementsMergersMessage()  {}
func (Append) implementsMergersMessage() {}
