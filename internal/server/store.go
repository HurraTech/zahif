package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/beeker1121/goque"
	log "github.com/sirupsen/logrus"

	"hurracloud.io/zahif/internal/backend"
	"hurracloud.io/zahif/internal/indexer"
)

var batchIndexers map[string]*indexer.BatchIndexer

type store struct {
	batchIndexers map[string]*indexer.BatchIndexer

	MetadataDir      string
	SearchBackend    backend.SearchBackend
	InterruptChannel <-chan string
	RetriesQueue     *goque.Queue
}

func (s *store) NewIndexer(settings *indexer.IndexSettings) (*indexer.BatchIndexer, error) {

	if s.batchIndexers == nil {
		s.batchIndexers = make(map[string]*indexer.BatchIndexer)
	}

	s.batchIndexers[settings.IndexIdentifier] = &indexer.BatchIndexer{
		IndexSettings: &indexer.IndexSettings{
			Target:          settings.Target,
			IndexIdentifier: settings.IndexIdentifier,
			Parallelism:     settings.Parallelism,
			ExcludePatterns: settings.ExcludePatterns,
		},
		MetadataDir:      s.MetadataDir,
		Backend:          s.SearchBackend,
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
		indexer.MetadataDir = s.MetadataDir
		return indexer, nil
	}

}

func (s *store) saveToDisk(indexName string) (*indexer.BatchIndexer, error) {
	log.Debugf("Serializing %v", s.batchIndexers[indexName].IndexSettings)
	file, err := json.MarshalIndent(s.batchIndexers[indexName].IndexSettings, "", " ")
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize indexer: %s: %v", indexName, err)
	}
	err = ioutil.WriteFile(path.Join(s.MetadataDir, indexName), file, 0644)
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
	indexSettings := &indexer.IndexSettings{}
	err := json.Unmarshal([]byte(file), indexSettings)

	if err != nil {
		return nil, fmt.Errorf("Error while reading index metadata: %s: %v", indexName, err)
	}

	s.batchIndexers[indexName] = &indexer.BatchIndexer{
		IndexSettings: indexSettings,
	}

	return s.batchIndexers[indexName], nil
}
