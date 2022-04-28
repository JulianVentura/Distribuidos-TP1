package main

import (
	Err "distribuidos/tp1/common/errors"
	"distribuidos/tp1/server/src/server"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
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
	v.BindEnv("log", "level")
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

	if _, err := time.ParseDuration(v.GetString("database.epoch-duration")); err != nil {
		return c_config, Err.Ctx("Could not parse database_epoch_duration as time.Duration.", err)
	}

	if _, err := time.ParseDuration(v.GetString("alarm.period")); err != nil {
		return c_config, Err.Ctx("Could not parse alarm_period as time.Duration.", err)
	}

	c_config.Server_ip = v.GetString("address.ip")
	c_config.Server_port = v.GetString("address.port")
	c_config.Log_level = v.GetString("log.level")
	c_config.Client_conn_timeout = v.GetDuration("client.timeout")
	c_config.DB_writers_number = v.GetUint("workers.database.writers")
	c_config.DB_readers_number = v.GetUint("workers.database.readers")
	c_config.DB_mergers_number = v.GetUint("workers.database.mergers")
	c_config.Con_worker_number = v.GetUint("workers.connection")
	c_config.DB_writers_queue_size = v.GetUint("queues.database.writers")
	c_config.DB_readers_queue_size = v.GetUint("queues.database.readers")
	c_config.DB_merge_admin_queue_size = v.GetUint("queues.database.merger-admin")
	c_config.DB_mergers_queue_size = v.GetUint("queues.database.mergers")
	c_config.Con_worker_queue_size = v.GetUint("queues.connection")
	c_config.Dispatcher_queue_size = v.GetUint("queues.dispatcher")
	c_config.Alarm_manager_queue_size = v.GetUint("queues.alarm-manager")
	c_config.DB_epoch_duration = v.GetDuration("database.epoch-duration")
	c_config.DB_files_path = v.GetString("database.files-path")
	c_config.Alarm_period = v.GetDuration("alarm.period")
	c_config.Alarm_config_file = v.GetString("alarm.config-path")

	return c_config, nil

}

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	log.SetLevel(level)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:      true,
		DisableTimestamp: true,
	})
	return nil
}

func PrintConfig(config *server.ServerConfig) {
	log.Infof("Server configuration")
	log.Infof("Server IP: %s", config.Server_ip)
	log.Infof("Server PORT: %s", config.Server_port)
	log.Infof("Server Log Level: %s", config.Log_level)
	log.Infof("Client connection timeout: %s", config.Client_conn_timeout)
	log.Infof("Number of workers:")
	log.Infof(" - Database writers: %v", config.DB_writers_number)
	log.Infof(" - Database readers: %v", config.DB_readers_number)
	log.Infof(" - Database mergers: %v", config.DB_mergers_number)
	log.Infof(" - Connections: %v", config.Con_worker_number)
	log.Infof("Queue size:")
	log.Infof(" - Database writers: %v", config.DB_writers_queue_size)
	log.Infof(" - Database readers: %v", config.DB_readers_queue_size)
	log.Infof(" - Database readers: %v", config.DB_readers_queue_size)
	log.Infof(" - Database merger admin: %v", config.DB_merge_admin_queue_size)
	log.Infof(" - Database mergers: %v", config.DB_mergers_queue_size)
	log.Infof(" - Connections: %v", config.Con_worker_queue_size)
	log.Infof(" - Dispatcher: %v", config.Dispatcher_queue_size)
	log.Infof(" - AlarmManager: %v", config.Alarm_manager_queue_size)
	log.Infof("Database:")
	log.Infof(" - Epoch duration: %v", config.DB_epoch_duration)
	log.Infof(" - Files path: %v", config.DB_files_path)
	log.Infof("Alarm:")
	log.Infof(" - Period: %v", config.Alarm_period)
	log.Infof(" - Config file path: %v", config.Alarm_config_file)
}

func main() {
	fmt.Println("Starting Server...")
	config, err := InitConfig()
	if err != nil {
		fmt.Printf("Error found on configuration. %v\n", err)
		return
	}

	if err := InitLogger(config.Log_level); err != nil {
		log.Fatalf("%s", err)
	}

	PrintConfig(&config)

	server, err := server.Start(config)
	if err != nil {
		log.Fatalf("Error starting server. %v", err)
		return
	}
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM)
	<-exit

	log.Infof("Shuting down server...")
	server.Finish()
}
