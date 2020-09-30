package backend

import (
	"fmt"
	"strings"

	"github.com/expectedsh/go-sonic/sonic"
	log "github.com/sirupsen/logrus"
)

type Sonic struct {
	ingester    sonic.Ingestable
	search      sonic.Searchable
	parallelism int
	host        string
	port        int
	password    string
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

	return &Sonic{ingester: ingester, search: search, parallelism: parallelism, host: host, port: port, password: password}, nil
}

func (s *Sonic) reconnect() error {
	log.Warning("Re-connecting to Sonic backend")

	ingester, err := sonic.NewIngester(s.host, s.port, s.password)
	if err != nil {
		return fmt.Errorf("Failed to connect to sonic server: %v", err)
	}

	search, err := sonic.NewSearch(s.host, s.port, s.password)
	if err != nil {
		return fmt.Errorf("Failed to connect to sonic server: %v", err)
	}
	s.ingester = ingester
	s.search = search
	return nil
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

func (s *Sonic) IndexFile(indexName string, d Document) error {
	err := s.ingester.FlushObject(indexName, "generic", d.ID)
	if err != nil {
		return fmt.Errorf("Sonic error while indexing document: %v", err)
	}

	err = s.ingester.Push(indexName, "generic", d.ID, d.Content)
	if err != nil {
		return fmt.Errorf("Sonic error while indexing document: %v", err)
	}
	return nil
}

func (s *Sonic) DeleteIndex(indexName string) error {
	err := s.ingester.FlushCollection(indexName)
	if isSonicClosed(err) {
		err = s.reconnect()
	}

	if err != nil {
		return fmt.Errorf("Sonic error while deleting index %s: %v", indexName, err)
	}
	return nil
}

func (s *Sonic) SearchIndex(indexName string, query string, from int, limit int) ([]string, error) {
	results, err := s.search.Query(indexName, "generic", query, limit, from)
	if isSonicClosed(err) {
		s.reconnect()
		results, err = s.search.Query(indexName, "generic", query, limit, from)
	}

	if err != nil {
		return nil, fmt.Errorf("Sonic while fulfilling search request: %v", err)
	}
	if len(results) == 1 && results[0] == "" {
		results = []string{} // removing sonic empty result element
	}
	return results, nil
}

func isSonicClosed(err error) bool {
	return err != nil && (err == sonic.ErrClosed || strings.Contains(err.Error(), "EOF"))
}
