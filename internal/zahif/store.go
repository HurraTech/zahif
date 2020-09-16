package zahif

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/beeker1121/goque"
	"google.golang.org/appengine/log"
	"hurracloud.io/zahif/internal/indexer"
	"hurracloud.io/zahif/internal/search/backend"
)

var batchIndexers map[string]*indexer.BatchIndexer

type store struct {
	batchIndexers map[string]*indexer.BatchIndexer

	MetadataDir      string
	SearchBackend    backend.SearchBackend
	InterruptChannel <-chan string
	RetriesQueue     *goque.Queue
}

type indexSettings struct {
	Target          string
	Parallelism     int
	IndexIdentifier string
	ExcludePatterns []string
}

func (s *store) NewIndexer(settings *indexSettings) (*indexer.BatchIndexer, error) {

	if s.batchIndexers == nil {
		s.batchIndexers = make(map[string]*indexer.BatchIndexer)
	}

	s.batchIndexers[settings.IndexIdentifier] = &indexer.BatchIndexer{
		Target:           settings.Target,
		MetadataDir:      s.MetadataDir,
		IndexIdentifier:  settings.IndexIdentifier,
		Parallelism:      settings.Parallelism,
		Backend:          s.SearchBackend,
		ExcludePatterns:  settings.ExcludePatterns,
		RetriesQueue:     s.RetriesQueue,
		InterruptChannel: s.InterruptChannel,
	}

	return s.saveToDisk(settings.IndexIdentifier)
}

func (s *store) DeleteIndexer(indexName string) {
	os.Remove(path.Join(s.MetadataDir, indexName))
}

func (s *store) GetIndexer(indexName string) (*indexer.BatchIndexer, error) {
	_, err := os.Stat(path.Join(s.MetadataDir, indexName))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Index does not exist: %s", indexName)
	}

	if val, ok := s.batchIndexers[indexName]; ok {
		return val, nil
	} else {
		indexer, err := s.readFromDisk(indexName)
		if err != nil {
			return nil, err
		}
		indexer.Backend = s.SearchBackend
		indexer.InterruptChannel = s.InterruptChannel
		indexer.RetriesQueue = s.RetriesQueue
		return indexer, nil
	}

}

func (s *store) saveToDisk(indexName string) (*indexer.BatchIndexer, error) {
	file, _ := json.MarshalIndent(s.batchIndexers[indexName], "", " ")
	err := ioutil.WriteFile(path.Join(s.MetadataDir, indexName), file, 0644)
	log.Debugf("Writing %s to disk", string(file))
	if err != nil {
		return nil, fmt.Errorf("Error while storing index metadata: %s: %v", indexName, err)
	}
	return s.batchIndexers[indexName], nil
}

func (s *store) readFromDisk(indexName string) (*indexer.BatchIndexer, error) {
	if s.batchIndexers == nil {
		s.batchIndexers = make(map[string]*indexer.BatchIndexer)
	}

	file, _ := ioutil.ReadFile(path.Join(s.MetadataDir, indexName))
	indexer := &indexer.BatchIndexer{}
	err := json.Unmarshal([]byte(file), indexer)

	if err != nil {
		return nil, fmt.Errorf("Error while reading index metadata: %s: %v", indexName, err)
	}

	s.batchIndexers[indexName] = indexer

	return indexer, nil
}
