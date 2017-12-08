package GlobalTypes

type AggregatorConfiguration struct {
	FilesLocation string `json:"files_location"`
	MaxWorkers int `json:"max_workers"`
	GzipAvgHeaderSize int64 `json:"gzip_avg_header_size"`
	SaveFilesToDisk bool `json:"save_files_to_disk"`
	LocalFolderLocation string `json:"local_folder_location"`
	MaxAggregatedFileSize int64 `json:"max_aggregated_file_size"`
	TruncateEveryRun bool `json:"truncate_every_run"`
	DbConfiguration DbConfiguration `json:"db_configuration"`
}

type DbConfiguration struct {
	ConnectionString string `json:"connection_string"`
	Driver string `json:"driver"`
}


