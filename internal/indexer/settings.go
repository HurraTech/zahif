package indexer

type IndexSettings struct {
	Target          string   `json:target`
	Parallelism     int      `json:parallelism`
	IndexIdentifier string   `json: index_identifier`
	ExcludePatterns []string `json: exclude_patterns`
}
