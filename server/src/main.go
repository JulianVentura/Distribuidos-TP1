package main

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/common/protocol"
	"distribuidos/tp1/common/socket"
	"distribuidos/tp1/server/src/server"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/viper"
)

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (server.ServerConfig, error) {

	c_config := server.ServerConfig{}

	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "ip")
	v.BindEnv("server", "port")
	v.BindEnv("workers", "database", "writers")
	v.BindEnv("workers", "database", "readers")
	v.BindEnv("workers", "database", "mergers")
	v.BindEnv("workers", "connection")
	v.BindEnv("queue", "database", "writers")
	v.BindEnv("queue", "database", "readers")
	v.BindEnv("queue", "database", "mergers")
	v.BindEnv("queue", "connection")
	v.BindEnv("queue", "dispatcher")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Println("Configuration could not be read from config file. Using env variables instead")
	}

	if _, err := time.ParseDuration(v.GetString("client.timeout")); err != nil {
		return c_config, Err.Ctx("Could not parse client_timeout as time.Duration.", err)
	}

	c_config.Server_ip = v.GetString("address.ip")
	c_config.Server_port = v.GetString("address.port")
	c_config.Client_conn_timeout = v.GetDuration("client.timeout")
	c_config.DB_writers_number = v.GetUint("workers.database.writers")
	c_config.DB_readers_number = v.GetUint("workers.database.readers")
	c_config.DB_mergers_number = v.GetUint("workers.database.mergers")
	c_config.Con_worker_number = v.GetUint("workers.connection")
	c_config.DB_writers_queue_size = v.GetUint("queues.database.writers")
	c_config.DB_readers_queue_size = v.GetUint("queues.database.readers")
	c_config.DB_mergers_queue_size = v.GetUint("queues.database.mergers")
	c_config.Con_worker_queue_size = v.GetUint("queues.connection")
	c_config.Dispatcher_queue_size = v.GetUint("queues.dispatcher")

	return c_config, nil

}

func PrintConfig(config *server.ServerConfig) {
	fmt.Println("Server configuration")
	fmt.Printf("Server IP: %s\n", config.Server_ip)
	fmt.Printf("Server PORT: %s\n", config.Server_port)
	fmt.Printf("Client connection timeout: %s\n", config.Client_conn_timeout)
	fmt.Println("Number of workers:")
	fmt.Printf(" - Database writers: %v\n", config.DB_writers_number)
	fmt.Printf(" - Database readers: %v\n", config.DB_readers_number)
	fmt.Printf(" - Database mergers: %v\n", config.DB_mergers_number)
	fmt.Printf(" - Connections: %v\n", config.Con_worker_number)
	fmt.Println("Queue size:")
	fmt.Printf(" - Database writers: %v\n", config.DB_writers_queue_size)
	fmt.Printf(" - Database readers: %v\n", config.DB_readers_queue_size)
	fmt.Printf(" - Database mergers: %v\n", config.DB_mergers_queue_size)
	fmt.Printf(" - Connections: %v\n", config.Con_worker_queue_size)
	fmt.Printf(" - Dispatcher: %v\n", config.Dispatcher_queue_size)
}

func handleClientConnection(c *server.ServerConfig) error {

	server, err := socket.NewServer(fmt.Sprintf("%v:%v", c.Server_ip, c.Server_port))
	if err != nil {
		return err
	}
	fmt.Println("Server Started")
	client, err := server.Accept()
	fmt.Println("New Client has arrived")
	if err != nil {
		return err
	}
	_ = server.Close()

Loop:
	for {
		fmt.Println("Receiving a message from client")
		message, err := protocol.Receive(&client)
		if err != nil {
			fmt.Printf("Receive message failed: %v", err)
			break Loop
		}
		switch m := message.(type) {
		case *protocol.Metric:
			fmt.Printf(" - Metric: (%v, %v)\n", m.Id, m.Value)
		case *protocol.Query:
			fmt.Printf(" - Query: (%v, %v, %v, %v, %v)\n", m.Metric_id, m.From, m.To, m.Aggregation, m.AggregationWindowSecs)
		case *protocol.Finish:
			fmt.Println("Client finished, closing...")
			break Loop
		}
	}

	_ = client.Close()

	return nil
}

func main() {
	fmt.Println("Starting Server...")
	config, err := InitConfig()
	if err != nil {
		fmt.Printf("Error found on configuration. %v\n", err)
		return
	}
	PrintConfig(&config)

	server, err := server.Start(config)
	if err != nil {
		fmt.Printf("Error starting server. %v\n", err)
		return
	}
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM)
	<-exit

	fmt.Println("Shuting down server...")
	server.Finish()
}
