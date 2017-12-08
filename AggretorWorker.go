package Aggregator

import (
	"Aggregator/GlobalTypes"
	"github.com/nu7hatch/gouuid"
	"sync"
	"Aggregator/DbHandler"
	"time"
	"bytes"
	"path/filepath"
	"os"
	"compress/gzip"
	"bufio"
	"io"
	"fmt"
	"crypto/md5"
)

type AggregatorWorker struct {
	config       GlobalTypes.AggregatorConfiguration
	id           *uuid.UUID
	db           DbHandler.DbHandler
	workerNumber int
}

//
// this De duplication can also be implemented using redis/database (if the case was unique in all results ever)
// i didn't hash the lines since they are very short, if the case was different i would have used MD5
// the mutex might block the sometimes on write, but since its very efficient O(1) the mutex wont work that much
//
var ProcessedLines map[string]bool // static map will contain all the lines as the key so we will make sure there are no duplications
var mutex = &sync.Mutex{}          // lock mutex for the map

func CheckProcessedLinesExists(line string) bool {
	mutex.Lock()
	result := true
	if _, exists := ProcessedLines[line]; !exists {
		result = false
	}
	mutex.Unlock()
	return result
}

func AppendProcessedLines(line string) {
	mutex.Lock()
	if ProcessedLines == nil {
		ProcessedLines = make(map[string]bool)
	}

	ProcessedLines[line] = true
	mutex.Unlock()
}

func (w *AggregatorWorker) Start(files <-chan FileType) (out chan DbHandler.AggregatedResult, err error) {
	w.id, _ = uuid.NewV4() // worker id is generated

	w.db.InsertWorkers(DbHandler.Worker{
		Id:        w.id,
		StartDate: time.Now(),
	})

	// create the worker's out channel
	out = make(chan DbHandler.AggregatedResult)

	go func() {
		defer close(out)
		// run until there are no files left in the channel
		workerCSVCount := 0
		for {
			var r DbHandler.AggregatedResult

			r, err = w.aggregateCSVFiles(files, workerCSVCount)
			out <- r
			workerCSVCount ++
			if len(files) == 0 {
				break
			}
		}

	}()
	return out, err
}

// will return ~1k gzip files
func (w *AggregatorWorker) aggregateCSVFiles(files <-chan FileType, fileNumber int) (result DbHandler.AggregatedResult, err error) {
	var currentSize int64 = 0 //header size will be calculated only once.
	var newCSVFile bytes.Buffer
	var filesNames bytes.Buffer

	filesNames.WriteString("|")

	//run on all files in the channel
	for file := range files {
		// if the current csv file size and the new file are greater than the allowed file size break
		if currentSize+file.Size >= w.config.MaxAggregatedFileSize {
			break
		}

		filesNames.WriteString(filepath.Base(file.FileName) + "|") // write only the file name to the file names string

		f, err := os.Open(file.FileName) //open the file
		gzipr, err := gzip.NewReader(f)  //create gzip reader

		if err != nil {
			return result, err
		}

		rr := bufio.NewReader(gzipr) //create bufio reader using the gzip reader since it implements io.reader
		for {
			line, err := rr.ReadString('\n') // read new line
			if err == io.EOF {
				break
			}
			// make sure the line doesn't exists unless its the first line which is the header

			h := md5.New()
			io.WriteString(h,line)
			lineHash := string(h.Sum(nil))

			if !CheckProcessedLinesExists(lineHash) || newCSVFile.Len() == 0 {
				newCSVFile.WriteString(line)
				currentSize += int64(len(line))
				AppendProcessedLines(lineHash)
			}
		}
		f.Close()     //close the open file
		gzipr.Close() //close the gzip reader
	}

	if w.config.SaveFilesToDisk { // write to disk if enabled in the configuration
		f, _ := os.Create(fmt.Sprintf("%s/worker-%v-%v.csv.gz", w.config.LocalFolderLocation, w.workerNumber, fileNumber))
		gzw := gzip.NewWriter(f)
		gzw.Write(newCSVFile.Bytes())
		gzw.Flush()
		gzw.Close()
		f.Close()
	}
	// write the buffer to gzip file
	var newGzipBuff bytes.Buffer
	gzw := gzip.NewWriter(&newGzipBuff)
	gzw.Write(newCSVFile.Bytes())
	gzw.Flush()
	gzw.Close()

	//generate document id and create result object
	id, err := uuid.NewV4()
	result = DbHandler.AggregatedResult{
		Id:              id,
		Data:            &newGzipBuff,
		AggregatedFiles: filesNames.String(),
		WorkerId:        w.id,
		ProcessedTime:   time.Now(),
	}
	return
}
