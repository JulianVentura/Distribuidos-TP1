package client

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	"fmt"
	"io/ioutil"
	"time"

	json "github.com/buger/jsonparser"
)

type ClientConfig struct {
	Id                uint
	Server_address    string
	Loop_period       time.Duration
	Queries_file_path string
}

type Client struct {
	id              uint
	server          *socket.TCPConnection
	loop_period     time.Duration
	queries_to_send []*protocol.Query
	quit            chan bool
	has_finished    chan bool
}

func Start(config ClientConfig) (*Client, error) {
	queries, err := read_and_parse_queries(config.Queries_file_path)

	if err != nil {
		return nil, Err.Ctx("Error parsing queries file", err)
	}

	server, err := socket.NewClient(config.Server_address)

	if err != nil {
		msg := fmt.Sprintf("Connection with server on address %v failed", config.Server_address)
		return nil, Err.Ctx(msg, err)
	}

	fmt.Println("Connection with server established")

	client := &Client{
		id:              config.Id,
		server:          server,
		loop_period:     config.Loop_period,
		queries_to_send: queries,
		quit:            make(chan bool, 2),
		has_finished:    make(chan bool, 2),
	}

	go client.run()

	return client, nil
}

func (self *Client) Finish() {
	self.quit <- true
	<-self.has_finished
}

func (self *Client) run() {
	defer func() {
		self.server.Close()
		self.has_finished <- true
	}()

Loop:
	for id, query := range self.queries_to_send {
		select {
		case <-self.quit:
			break Loop
		default:
			fmt.Printf("Sending query %v to server\n", id)
			print_query(query)
			err := protocol.Send(self.server, query)
			if err != nil {
				fmt.Println(Err.Ctx("Error sending query to server. ", err))
				return
			}
			response, err := protocol.Receive(self.server)
			if err != nil {
				fmt.Println(Err.Ctx("Error receiving response from server", err))
				return
			}
			fmt.Println("Server response: ")
			should_finish := parse_server_response(response)
			if should_finish {
				return
			}
		}
		time.Sleep(self.loop_period)
	}
	protocol.Send(self.server, &protocol.Finish{})
}

func print_query(query *protocol.Query) {
	fmt.Printf(" - MetricID: %v\n", query.Metric_id)
	fmt.Printf(" - From: %v\n", query.From)
	fmt.Printf(" - To: %v\n", query.To)
	fmt.Printf(" - Aggregation: %v\n", query.Aggregation)
	fmt.Printf(" - AggregationWindowSecs: %v\n", query.AggregationWindowSecs)
	fmt.Println()
}

func parse_server_response(response protocol.Encodable) bool {

	switch t := response.(type) {
	case *protocol.Ok:
		fmt.Println("Ok")
	case *protocol.Error:
		fmt.Printf("ERROR: %v\n", t.Message)
	case *protocol.QueryResponse:
		fmt.Printf("%v\n", t.Data)
	case *protocol.Finish:
		if len(t.Message) == 0 {
			fmt.Println("Finish")
		} else {
			fmt.Printf("Finish: %v\n", t.Message)
		}
		return true
	}

	return false
}

func read_and_parse_queries(path string) ([]*protocol.Query, error) {
	//Parse json and create alarms
	json_data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open file %v. %v\n", path, err)
	}

	queries := make([]*protocol.Query, 0)

	_, err = json.ArrayEach(json_data, func(value []byte, dataType json.ValueType, offset int, err error) {
		//Ignore errors
		metric_id, _ := json.GetString(value, "metric_id")
		aggregation, _ := json.GetString(value, "aggregation")
		aggregation_window_secs, _ := json.GetFloat(value, "aggregation_window_secs")
		from, _ := json.GetString(value, "from")
		to, _ := json.GetString(value, "to")

		query := &protocol.Query{
			Metric_id:             metric_id,
			From:                  from,
			To:                    to,
			Aggregation:           aggregation,
			AggregationWindowSecs: aggregation_window_secs,
		}

		queries = append(queries, query)
	})

	if err != nil {
		return nil, err
	}

	return queries, nil
}
