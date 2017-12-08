package DbHandler

import (
	_ "github.com/go-sql-driver/mysql"
	"Aggregator/GlobalTypes"
	"io"
	"bytes"
	"github.com/nu7hatch/gouuid"
	"time"
	"encoding/base64"
	"reflect"
	"log"
	"strings"
	"database/sql"
	"fmt"
)

type DbHandler interface {
	InsertDocuments(results <-chan AggregatedResult) (chan bool, error)
	InsertWorkers(worker Worker)
	TruncateAll()
}

type MySqlDbHandler struct {
	Config GlobalTypes.DbConfiguration
}

func (m MySqlDbHandler) Make(config GlobalTypes.DbConfiguration) DbHandler {
	return &MySqlDbHandler{
		Config: config,
	}
}

type AggregatedResult struct {
	Id              *uuid.UUID
	Data            io.Reader
	WorkerId        *uuid.UUID
	AggregatedFiles string
	ProcessedTime   time.Time
}

type Worker struct {
	Id        *uuid.UUID
	StartDate time.Time
}

type ParamKeyValue struct {
	Type  reflect.Type
	Value interface{}
}

const DateFormatters = "'%m/%d/%Y'"

func queryParametersCreator(params map[string]ParamKeyValue) string {
	var b bytes.Buffer //query params buffer
	for k, v := range params {
		var value string
		if (v.Type == reflect.TypeOf(time.Time{})) { //insert time aas parsed date
			t := v.Value.(time.Time)
			value += fmt.Sprintf("STR_TO_DATE('%d/%d/%d',%s),", t.Month(), t.Day(), t.Year(), DateFormatters)
		} else {
			value = "'" + v.Value.(string) + "',"
		}

		b.WriteString(k + "=" + value + " ") // append each param to its value
	}
	return strings.Trim(strings.Trim(b.String(), " "), ",")
}

func (m *MySqlDbHandler) TruncateAll() {

	db, err := sql.Open(m.Config.Driver, m.Config.ConnectionString) // open connection to mysql database
	defer db.Close()
	_, err = db.Exec("TRUNCATE `dev`.`documents`;")
	_, err = db.Exec("TRUNCATE `dev`.`workers`;")

	if err != nil{
		log.Panic(err.Error())
	}
}


// can be turned into a bulk insert if needed using a buffered channel.
func (m *MySqlDbHandler) InsertDocuments(results <-chan AggregatedResult) (done chan bool, err error) {

	done = make(chan bool)
	db, err := sql.Open(m.Config.Driver, m.Config.ConnectionString) // open connection to mysql database

	go func() {
		successfulInsert := 0
		failures := 0
		defer db.Close()

		for result := range results {
			var queryBuffer bytes.Buffer
			queryBuffer.WriteString("INSERT INTO documents SET ")

			// collect all the parameters in a map
			params := make(map[string]ParamKeyValue)
			params["Id"] = ParamKeyValue{Value: result.Id.String(), Type: reflect.TypeOf(result.Id.String())}
			params["WorkerId"] = ParamKeyValue{Value: result.WorkerId.String(), Type: reflect.TypeOf(result.WorkerId.String())}
			params["Processed"] = ParamKeyValue{Value: result.ProcessedTime, Type: reflect.TypeOf(result.ProcessedTime)}
			params["AggregateFiles"] = ParamKeyValue{Value: result.AggregatedFiles, Type: reflect.TypeOf(result.AggregatedFiles)}
			params["Data"] = ParamKeyValue{Value: getBase64String(result.Data), Type: reflect.TypeOf(result.Id.String())}

			// create sql key value string
			queryParams := queryParametersCreator(params)
			queryBuffer.WriteString(strings.Trim(queryParams, " ") + ",")
			query := strings.Trim(queryBuffer.String(), ",")
			_, err = db.Exec(query) // execute
			if err != nil {
				log.Println(err.Error())
				failures++
			} else {
				successfulInsert++
			}
		}

		log.Println(fmt.Sprintf("%d successful results and %d failures", successfulInsert, failures))
		done <- true
		close(done)
	}()
	return done, err
}

func (m *MySqlDbHandler) InsertWorkers(worker Worker) {

	go func() {
		db, err := sql.Open(m.Config.Driver, m.Config.ConnectionString) // open connection to mysql database
		if err != nil {
			log.Println(err.Error())
		}
		defer db.Close()

		params := make(map[string]ParamKeyValue)
		params["Id"] = ParamKeyValue{Value: worker.Id.String(), Type: reflect.TypeOf(worker.Id.String())}
		params["StartDate"] = ParamKeyValue{Value: worker.StartDate, Type: reflect.TypeOf(worker.StartDate)}
		queryParams := queryParametersCreator(params)

		query := "INSERT INTO workers SET " + queryParams
		_, err = db.Exec(query) //execute
		if err != nil {
			log.Println(err.Error())
		}

	}()
}

func getBase64String(r io.Reader) string {
	buff := make([]byte, 128) //128 bytes buffer
	result := make([]byte, 0) //the total slice of bytes
	for {
		_, err := r.Read(buff)
		if err == io.EOF {
			break
		}
		result = append(result, buff...) // combine the new buffer slice and the total
	}
	return base64.StdEncoding.EncodeToString(buff) //encode to base 64 string
}
