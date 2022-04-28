package alarm

import (
	bs "distribuidos/tp1/server/src/business"
	"distribuidos/tp1/server/src/messages"
	"fmt"
	"io/ioutil"
	"time"

	Err "distribuidos/tp1/common/errors"

	json "github.com/buger/jsonparser"
	log "github.com/sirupsen/logrus"
)

type AlarmManagerConfig struct {
	Id               uint
	Period           time.Duration
	Config_file_path string
}

type Alarm struct {
	id              string
	metric_id       string
	aggregation     string
	limit           float64
	agg_window_secs float64
}

type AlarmManager struct {
	alarms       map[string]*Alarm
	config       AlarmManagerConfig
	queue        chan messages.AlarmManagerMessage
	query_queue  chan messages.ReadDatabaseMessage
	awake        <-chan time.Time
	quit         chan bool
	has_finished chan bool
}

func StartAlarmManager(
	config AlarmManagerConfig,
	queue chan messages.AlarmManagerMessage,
	query_queue chan messages.ReadDatabaseMessage,
) (*AlarmManager, error) {

	self := &AlarmManager{}

	alarms, err := self.read_and_parse_alarms(config.Config_file_path)
	if err != nil {
		return nil, Err.Ctx("Error parsing Alarms config file", err)
	}

	self.config = config
	self.alarms = alarms
	self.queue = queue
	self.query_queue = query_queue
	self.awake = time.After(config.Period)
	self.quit = make(chan bool, 2)
	self.has_finished = make(chan bool, 2)

	log.Info("Alarm Manager started")
	self.print_alarms()

	go self.run()

	return self, nil
}

func (self *AlarmManager) Finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *AlarmManager) run() {

Loop:
	for {
		select {
		case <-self.quit:
			break Loop
		case <-self.awake:
			self.start_new_metric_check()
		case message := <-self.queue:
			switch m := message.(type) {
			case *messages.QueryResponse:
				self.handle_query_response(m)
			default:
				log.Errorf("An unknown message was assigned to AlarmManager %v", m)
			}
		}
	}

	self.has_finished <- true
	log.Info("Alarm Manager has finished")
}

func (self *AlarmManager) start_new_metric_check() {
	log.Debugf("AlarmManager: Starting new check")
	self.awake = time.After(self.config.Period)

	for _, alarm := range self.alarms {
		self.send_query(alarm)
	}
}

func (self *AlarmManager) send_query(alarm *Alarm) {
	query, _ := self.query_from_alarm(alarm) //It won't fail, we checked it

	msg := &messages.NewQuery{
		Conn_worker_id: self.config.Id,
		Query:          query,
	}

	self.query_queue <- msg
}

func (self *AlarmManager) handle_query_response(m *messages.QueryResponse) {
	if m.Is_error {
		log.Error("AlarmManager: Wrong configuration, %v", m.Error_message)
		return
	}
	alarm, exists := self.alarms[m.Query.Id]
	if !exists {
		log.Error("AlarmManager: Received a response for an unknown alarm %v", m.Query.Id)
		return
	}
	aggregation, err := bs.Aggregate(&m.Query, m.Response)
	if err != nil {
		log.Error("Aggregation failed on alarm %v. %v", alarm.id, err)
		return
	}
	for _, val := range aggregation {
		if val > alarm.limit {
			log.Warnf("%v: Limit of %v for aggregation %v on metric %v surpassed with a value of %v",
				alarm.id, alarm.limit, alarm.aggregation, alarm.metric_id, val)
		}
	}
}

func (self *AlarmManager) read_and_parse_alarms(path string) (map[string]*Alarm, error) {
	//Parse json and create alarms
	json_data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open file %v. %v\n", path, err)
	}

	alarms := make(map[string]*Alarm, 0)

	_, err = json.ArrayEach(json_data, func(value []byte, dataType json.ValueType, offset int, err error) {
		id, err := json.GetString(value, "id")
		if err != nil {
			log.Warnf("Couldn't parse alarm of uknown id %v", err)
			return
		}
		metric_id, err := json.GetString(value, "metric_id")
		if err != nil {
			log.Warnf("Couldn't parse alarm %v. %v", id, err)
			return
		}
		aggregation, err := json.GetString(value, "aggregation")
		if err != nil {
			log.Warnf("Couldn't parse alarm %v. %v", id, err)
			return
		}
		aggregation_window_secs, err := json.GetFloat(value, "aggregation_window_secs")
		if err != nil {
			log.Warnf("Couldn't parse alarm %v. %v", id, err)
			return
		}
		limit, err := json.GetFloat(value, "limit")
		if err != nil {
			log.Warnf("Couldn't parse alarm %v. %v", id, err)
			return
		}

		al := Alarm{
			id:              id,
			metric_id:       metric_id,
			aggregation:     aggregation,
			agg_window_secs: aggregation_window_secs,
			limit:           limit,
		}

		_, err = self.query_from_alarm(&al)
		if err != nil {
			log.Warnf("Alarm %v has formating errors. %v", id, err)
		}

		alarms[al.id] = &al
	})

	if err != nil {
		return nil, err
	}

	return alarms, nil
}

func (self *AlarmManager) print_alarms() {

	for _, alarm := range self.alarms {
		log.Infof("%v\n", alarm.id)
		log.Infof(" - MetricID: %v\n", alarm.metric_id)
		log.Infof(" - Aggregation: %v\n", alarm.aggregation)
		log.Infof(" - AggregationWindowSecs: %v\n", alarm.agg_window_secs)
		log.Infof(" - Limit: %v\n\n", alarm.limit)
	}
}

func (self *AlarmManager) query_from_alarm(alarm *Alarm) (bs.Query, error) {
	to := time.Now()
	from := to.Add(self.config.Period * -1)
	query, err := bs.NewQuery(
		alarm.id,
		alarm.metric_id,
		from,
		to,
		alarm.aggregation,
		alarm.agg_window_secs)

	return query, err
}
