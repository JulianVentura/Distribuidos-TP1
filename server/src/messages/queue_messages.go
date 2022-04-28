package messages

import (
	"distribuidos/tp1/common/socket"
	bs "distribuidos/tp1/server/src/business"
	"sync"
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

type AlarmManagerMessage interface {
	implementsAlarmManagerMessage()
}

type NewConnection struct {
	Skt *socket.TCPConnection
}

type ConnectionFinished struct {
	Conn_worker_id uint
}

type QueryResponse struct {
	Conn_worker_id uint
	Query          bs.Query
	Response       []*bs.Metric
	Is_error       bool
	Error_message  string
}

type NewMetric struct {
	Conn_worker_id uint
	Metric         bs.Metric
}

type NewQuery struct {
	Conn_worker_id uint
	Query          bs.Query
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
	DB_file_lock   *sync.RWMutex
}

func (NewConnection) implementsDispatcherMessage()       {}
func (NewConnection) implementsConnectionWorkerMessage() {}

func (ConnectionFinished) implementsDispatcherMessage() {}

func (QueryResponse) implementsDispatcherMessage()       {}
func (QueryResponse) implementsConnectionWorkerMessage() {}
func (QueryResponse) implementsAlarmManagerMessage()     {}

func (NewMetric) implementsWriteDatabaseMessage() {}
func (NewMetric) implementsReadDatabaseMessage()  {}

func (EpochEnd) implementsMergerAdminMessage()       {}
func (MergeFinished) implementsMergerAdminMessage()  {}
func (AppendFinished) implementsMergerAdminMessage() {}

func (Merge) implementsMergersMessage()  {}
func (Append) implementsMergersMessage() {}

func (NewQuery) implementsReadDatabaseMessage() {}
