package Aggregator

import (
	"Aggregator/GlobalTypes"
	"os"
	"encoding/json"
	"fmt"
)

const DefaultConfigurationFIle = "Aggregator.json"

func LoadConfigurations() (*GlobalTypes.AggregatorConfiguration, error) {

	wd,err := os.Getwd()
	configFile, err := os.Open(fmt.Sprintf("%s/%s", wd,DefaultConfigurationFIle))
	if err != nil {
		return nil,err
	}

	defer configFile.Close()

	decoder := json.NewDecoder(configFile)

	var config GlobalTypes.AggregatorConfiguration
	err = decoder.Decode(&config)
	return &config,err
}
