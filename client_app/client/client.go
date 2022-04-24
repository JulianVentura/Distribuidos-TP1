package client

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	"fmt"
	"math/rand"
	"time"
)

type ClientConfig struct {
	Id              uint
	Server_address  string
	Loop_period     time.Duration
	Number_of_loops uint
}

type Client struct {
	id              uint
	server          *socket.TCPConnection
	loop_period     time.Duration
	number_of_loops uint
	quit            chan bool
	has_finished    chan bool
}

func Start(config ClientConfig) (*Client, error) {
	server, err := socket.NewClient(config.Server_address)
	if err != nil {
		msg := fmt.Sprintf("Connection with server on address %v failed", config.Server_address)
		return nil, Err.Ctx(msg, err)
	}

	fmt.Println("Connection with server established")
	client := &Client{
		id:              config.Id,
		server:          server,
		quit:            make(chan bool, 2),
		has_finished:    make(chan bool, 2),
		loop_period:     config.Loop_period,
		number_of_loops: config.Number_of_loops,
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
	for i := uint(1); i < self.number_of_loops; i++ {
		select {
		case <-self.quit:
			break Loop
		default:
			message := randomMessage(self.id)
			fmt.Printf("Sending to server: %v\n", message)
			err := protocol.Send(self.server, message)
			if err != nil {
				fmt.Println(Err.Ctx("Error sending a metric to server. ", err))
				return
			}
			response, err := protocol.Receive(self.server)
			if err != nil {
				fmt.Println(Err.Ctx("Error receiving response from server", err))
				return
			}
			should_finish := parse_server_response(response)
			if should_finish {
				return
			}
		}
		time.Sleep(self.loop_period)
	}
	protocol.Send(self.server, &protocol.Finish{})
}

func parse_server_response(response protocol.Encodable) bool {

	switch t := response.(type) {
	case *protocol.Ok:
		fmt.Println("Ok")
	case *protocol.Error:
		fmt.Printf("ERROR: %v\n", t.Message)
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

func randomMessage(id uint) protocol.Encodable {
	switch x := rand.Intn(100); {
	case x < 20:
		return &protocol.Query{
			Metric_id:             fmt.Sprintf("CLI_METRIC_%v", id),
			From:                  "2020-01-01T14:23:44",
			To:                    "2020-01-01T14:25:44",
			Aggregation:           "MAX",
			AggregationWindowSecs: 2.3,
		}
	default:
		return &protocol.Metric{
			Id:    fmt.Sprintf("CLI_METRIC_%v", id),
			Value: float64(id),
		}

	}

}
