package Aggregator

import (
	"io/ioutil"
	"strings"
	"Aggregator/GlobalTypes"
	"os"
	"log"
	"sync"
	"Aggregator/DbHandler"
	"fmt"
)

const SupportedExtensions = ".gz"
const WorkingDirectoryPlaceholder = "#wd"

type Aggregator interface {
	Start() error
}

type GzipAggregator struct {
	config GlobalTypes.AggregatorConfiguration
	db     DbHandler.DbHandler
}

func (a GzipAggregator) Make(config GlobalTypes.AggregatorConfiguration) Aggregator {
	return &GzipAggregator{
		config: config,
		db:     DbHandler.MySqlDbHandler{}.Make(config.DbConfiguration),
	}
}

func (a *GzipAggregator) Start() error {

	if a.config.TruncateEveryRun{
		a.db.TruncateAll()
		log.Println("TruncateAll ended successfully")
	}

	//map all files in selected folder
	var files = make(map[string]int64)
	err := a.mapFolder(a.config.FilesLocation, SupportedExtensions, &files)

	log.Println(fmt.Sprintf("%d were mapped",len(files)))

	// insert all files to 1 channel
	filesChan := make(chan FileType, len(files))
	go func() {
		for f, s := range files {
			filesChan <- FileType{
				Size:     s,
				FileName: f,
			}
		}
		close(filesChan)
	}()

	// create a slice of readonly channels for all the results
	results := make([] <-chan DbHandler.AggregatedResult, 0)

	log.Println(fmt.Sprintf("Starting %d workers",a.config.MaxWorkers))
	// consume the files channel from all workers (fan out)
	for i := 0; i < a.config.MaxWorkers; i++ {
		// create new worker
		worker := &AggregatorWorker{
			config:       a.config,
			db:           a.db,
			workerNumber: i,
		}

		c, err := worker.Start(filesChan)

		if err != nil {
			log.Println(err.Error())
		}
		// collect all the result channels in a slice
		results = append(results, c)
	}

	// merge all the channels (fan in)
	mr := a.mergeResults(results...)

	log.Println("Inserting results to database")

	// insert all the merged results to the database
	done, err := a.db.InsertDocuments(mr)

	<-done
	log.Println("Done.")

	return err
}

func (a *GzipAggregator) mergeResults(rc ...  <-chan DbHandler.AggregatedResult) <-chan DbHandler.AggregatedResult {

	var wg sync.WaitGroup // create a wait group
	out := make(chan DbHandler.AggregatedResult)

	// create a func that will merge each channel to the out channel
	output := func(wr <-chan DbHandler.AggregatedResult) {
		for r := range wr {
			out <- r
		}
		wg.Done() // finish the wait group of the current channel
	}

	wg.Add(len(rc)) // add a wait group for each channel
	for _, wr := range rc { // execute the the func
		go output(wr)
	}

	go func() {
		wg.Wait() // wait for al channels to finish
		close(out) // close the channel
	}()

	return out
}

func (a *GzipAggregator) mapFolder(path string, extensions string, result *map[string]int64) error {
	if strings.Contains(path, WorkingDirectoryPlaceholder) {
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		path = strings.Replace(path, WorkingDirectoryPlaceholder, wd, 1)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			err = a.mapFolder(fmt.Sprintf("%s/%s", path, f.Name()), extensions, result)
		} else {
			if _, exists := (*result)[f.Name()]; !exists && strings.Contains(f.Name(), extensions) {
				(*result)[fmt.Sprintf("%s/%s", path, f.Name())] = f.Size() - a.config.GzipAvgHeaderSize
			}
		}
	}
	return err
}
