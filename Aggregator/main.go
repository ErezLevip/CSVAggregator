package main

import (
	"Aggregator/Aggregator"
	"log"
	"runtime"
	"os"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU()) //use max processing cores

	config, err := Aggregator.LoadConfigurations()
	if err != nil {
		log.Panic(err.Error())
	}

	log.Println("Configurations loaded successfully ")

	// create the output folder if needed
	if config.SaveFilesToDisk {
		if _, err := os.Stat(config.LocalFolderLocation); os.IsNotExist(err) {
			e := os.Mkdir(config.LocalFolderLocation, 0755)
			if e != nil {
				log.Panic(e.Error())
			}
		}

	}

	a := Aggregator.GzipAggregator{}.Make(*config)
	err = a.Start()
	if err != nil {
		log.Println(err.Error())
	}
}
