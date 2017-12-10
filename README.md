# CSV Files Aggregator
The CSV Files Aggregator accept a large amount of small CSV Files packed in Gzips and aggregates them into a larger file packed in gzip for future processing.

The Service is configured through Aggregator.js
```
{
  "files_location": "#wd/ProcessingData",
  "max_workers": 10,
  "max_aggregated_file_size": 1024,
  "save_files_to_disk":false,
  "local_folder_location":"output",
  "gzip_avg_header_size":40,
  "truncate_every_run":true,
  "db_configuration": {
    "driver": "mysql",
    "connection_string":""
  }
}
```
The default source folder is "ProcessingData" and #wd represents the working directory.

The current supported database driver is MySQL.
To change the adapter to a different adapter all that needs to be done is to switch the driver in the configuration and fix the query syntax.

Insert your connection string in the configuration file and you are ready to go.

To start using the service with docker:
```
Run "docker build - docker build -t aggregator ."
3. Run the docker image - "docker run aggregator"
```
