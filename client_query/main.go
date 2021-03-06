package main

import (
	"distribuidos/tp1/client_query/client"
	Err "distribuidos/tp1/common/errors"
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
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("loop", "period")
	v.BindEnv("loop", "lapse")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Println("Configuration could not be read from config file. Using env variables instead")
	}

	if _, err := time.ParseDuration(v.GetString("loop.period")); err != nil {
		return nil, Err.Ctx("Could not parse CLI_LOOP_PERIOD env var as time.Duration.", err)
	}

	return v, nil
}

func PrintConfig(c_config *client.ClientConfig) {
	fmt.Printf("Client %v configuration: \n", c_config.Id)
	fmt.Printf(" - ID: %v\n", c_config.Id)
	fmt.Printf(" - Server Address: %v\n", c_config.Server_address)
	fmt.Printf(" - Loop Period: %v\n", c_config.Loop_period)
	fmt.Printf(" - Queries File Path: %v\n", c_config.Queries_file_path)
	fmt.Print("----\n\n")
}

func main() {
	fmt.Println("Starting Client...")
	config, err := InitConfig()
	if err != nil {
		fmt.Printf("Error found on configuration. %v\n", err)
		return
	}

	c_config := client.ClientConfig{
		Id:                config.GetUint("id"),
		Server_address:    config.GetString("server.address"),
		Loop_period:       config.GetDuration("loop.period"),
		Queries_file_path: config.GetString("queries.file-path"),
	}

	PrintConfig(&c_config)

	cli, err := client.Start(c_config)

	if err != nil {
		fmt.Printf("Error starting client. %v\n", err)
		return
	}
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM)
	<-exit

	cli.Finish()
}
