package backend

import (
	"fmt"

	"github.com/expectedsh/go-sonic/sonic"
)

type Sonic struct {
	ingester    sonic.Ingestable
	search      sonic.Searchable
	parallelism int
}

func NewSonicBackend(host string, port int, password string, parallelism int) (*Sonic, error) {
	ingester, err := sonic.NewIngester(host, port, password)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to sonic server: %v", err)
	}

	search, err := sonic.NewSearch(host, port, password)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to sonic server: %v", err)
	}

	return &Sonic{ingester: ingester, search: search, parallelism: parallelism}, nil

}

func (s *Sonic) IndexFiles(indexName string, docs []Document) error {
	var records []sonic.IngestBulkRecord
	for _, d := range docs {
		records = append(records, sonic.IngestBulkRecord{d.ID, d.Content})
	}
	err := s.ingester.BulkPush(indexName, "generic", s.parallelism, records)
	if err != nil {
		return fmt.Errorf("Sonic error while indexing documents: %v", err)
	}
	return nil
}

func (s *Sonic) DeleteIndex(indexName string) error {
	if err := s.ingester.FlushCollection(indexName); err != nil {
		return fmt.Errorf("Sonic error while deleting index %s: %v", indexName, err)
	}
	return nil
}

func (s *Sonic) SearchIndex(indexName string, query string, from int, limit int) ([]string, error) {
	results, err := s.search.Query(indexName, "generic", query, limit, from)
	if err != nil {
		return nil, fmt.Errorf("Sonic while fulfilling search request: %v", err)
	}
	if len(results) == 1 && results[0] == "" {
		results = []string{} // removing sonic empty result element
	}
	return results, nil
}
