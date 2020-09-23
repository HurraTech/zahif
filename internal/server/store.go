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

type store struct {
	batchIndexers map[string]*indexer.BatchIndexer
	fileIndexers  map[string]*indexer.FileIndexer

	MetadataDir      string
	SearchBackend    backend.SearchBackend
	InterruptChannel <-chan string
	RetriesQueue     *goque.Queue
}

func (s *store) NewBatchIndexer(settings *indexer.IndexSettings) (*indexer.BatchIndexer, error) {

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

	err := s.saveSettingsToDisk(settings)
	if err != nil {
		return nil, fmt.Errorf("Failed to save index settings on disk: %f", err)
	}

	return s.batchIndexers[settings.IndexIdentifier], nil
}

func (s *store) DeleteIndexer(indexName string) {
	os.Remove(path.Join(s.MetadataDir, indexName))
}

func (s *store) GetBatchIndexer(indexName string) (*indexer.BatchIndexer, error) {
	_, err := os.Stat(path.Join(s.MetadataDir, indexName))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Index does not exist: %s", indexName)
	}

	if val, ok := s.batchIndexers[indexName]; ok {
		return val, nil
	} else {
		settings, err := s.readSettingsFromDisk(indexName)
		if err != nil {
			return nil, err
		}
		return s.NewBatchIndexer(settings)
	}
}

func (s *store) GetFileIndexer(indexName string) (*indexer.FileIndexer, error) {
	if s.fileIndexers == nil {
		s.fileIndexers = make(map[string]*indexer.FileIndexer)
	}

	if val, ok := s.fileIndexers[indexName]; ok {
		return val, nil
	} else {
		settings, err := s.readSettingsFromDisk(indexName)
		if err != nil {
			return nil, err
		}

		s.fileIndexers[indexName] = &indexer.FileIndexer{
			Backend:       s.SearchBackend,
			IndexSettings: settings,
		}
		return s.fileIndexers[indexName], nil
	}
}

func (s *store) saveSettingsToDisk(settings *indexer.IndexSettings) error {
	log.Debugf("Serializing %v", settings)
	file, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		return fmt.Errorf("Failed to serialize indexer: %s: %v", settings.IndexIdentifier, err)
	}
	err = ioutil.WriteFile(path.Join(s.MetadataDir, settings.IndexIdentifier), file, 0644)
	log.Debugf("Writing %s to disk", string(file))
	if err != nil {
		return fmt.Errorf("Error while storing index metadata: %s: %v", settings.IndexIdentifier, err)
	}
	return nil
}

func (s *store) readSettingsFromDisk(indexName string) (*indexer.IndexSettings, error) {
	file, _ := ioutil.ReadFile(path.Join(s.MetadataDir, indexName))
	indexSettings := &indexer.IndexSettings{}
	err := json.Unmarshal([]byte(file), indexSettings)

	if err != nil {
		return nil, fmt.Errorf("Error while reading index metadata: %s: %v", indexName, err)
	}
	return indexSettings, nil
}
