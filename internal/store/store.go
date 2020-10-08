package store

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

var (
	IndexDoesNotExistError = fmt.Errorf("Index Does Not Exist")
)

type Store struct {
	batchIndexers map[string]*indexer.BatchIndexer
	fileIndexers  map[string]*indexer.FileIndexer

	MetadataDir       string
	SearchBackend     backend.SearchBackend
	InterruptChannel  <-chan string
	RetriesQueue      *goque.Queue
	FileSizeThreshold int
}

func (s *Store) NewBatchIndexer(settings *indexer.IndexSettings) (*indexer.BatchIndexer, error) {

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
		MetadataDir:       s.MetadataDir,
		Backend:           s.SearchBackend,
		RetriesQueue:      s.RetriesQueue,
		InterruptChannel:  s.InterruptChannel,
		FileSizeThreshold: s.FileSizeThreshold,
	}

	err := s.saveSettingsToDisk(settings)
	if err != nil {
		return nil, fmt.Errorf("Failed to save index settings on disk: %f", err)
	}

	// If a previous index with same was deleted, a ".deleted" file might exist, let's remove it
	// deleted files are useful to ignore stale index requests, see DeleteIndexer for details
	os.Remove(path.Join(s.MetadataDir, fmt.Sprintf("%s.deleted", settings.IndexIdentifier)))

	return s.batchIndexers[settings.IndexIdentifier], nil
}

func (s *Store) DeleteIndexer(indexName string) {
	os.RemoveAll(path.Join(s.MetadataDir, indexName))

	// Let's persist that this index has been deleted
	// so that any queued index requests do not panic when they fail to find the index
	f, err := os.Create(path.Join(s.MetadataDir, fmt.Sprintf("%s.deleted", indexName)))
	if err != nil {
		log.Errorf("Could not create deleted state file: %s", err)
	}
	defer f.Close()
}

func (s *Store) GetBatchIndexer(indexName string) (*indexer.BatchIndexer, error) {
	_, err := os.Stat(path.Join(s.MetadataDir, indexName, "settings.json"))
	if os.IsNotExist(err) {
		return nil, IndexDoesNotExistError
	}

	if val, ok := s.batchIndexers[indexName]; ok {
		return val, nil
	} else {
		settings, err := s.ReadSettingsFromDisk(indexName)
		if err != nil {
			return nil, err
		}
		return s.NewBatchIndexer(settings)
	}
}

func (s *Store) GetFileIndexer(indexName string) (*indexer.FileIndexer, error) {
	if s.fileIndexers == nil {
		s.fileIndexers = make(map[string]*indexer.FileIndexer)
	}

	if val, ok := s.fileIndexers[indexName]; ok {
		return val, nil
	} else {
		settings, err := s.ReadSettingsFromDisk(indexName)
		if err != nil {
			return nil, err
		}

		s.fileIndexers[indexName] = &indexer.FileIndexer{
			Backend:           s.SearchBackend,
			IndexSettings:     settings,
			FileSizeThreshold: s.FileSizeThreshold,
		}
		return s.fileIndexers[indexName], nil
	}
}

func (s *Store) IsStaleIndex(indexName string) bool {
	_, err := os.Stat(path.Join(s.MetadataDir, fmt.Sprintf("%s.deleted", indexName)))
	if os.IsNotExist(err) {
		return false
	} else if err != nil {
		log.Warningf("Could not determine if index %s is stale: %s. Assume not to avoid losing data: %s", indexName, err)
	}
	return true
}

func (s *Store) saveSettingsToDisk(settings *indexer.IndexSettings) error {
	log.Debugf("Serializing %v", settings)
	file, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		return fmt.Errorf("Failed to serialize indexer: %s: %v", settings.IndexIdentifier, err)
	}
	err = os.MkdirAll(path.Join(s.MetadataDir, settings.IndexIdentifier), 0755)
	if err != nil {
		return fmt.Errorf("Error creating index metadata directory: %s: %v", settings.IndexIdentifier, err)
	}

	err = ioutil.WriteFile(path.Join(s.MetadataDir, settings.IndexIdentifier, "settings.json"), file, 0644)
	log.Debugf("Writing %s to disk", string(file))
	if err != nil {
		return fmt.Errorf("Error while storing index metadata: %s: %v", settings.IndexIdentifier, err)
	}
	return nil
}

func (s *Store) ReadSettingsFromDisk(indexName string) (*indexer.IndexSettings, error) {
	file, _ := ioutil.ReadFile(path.Join(s.MetadataDir, indexName, "settings.json"))
	indexSettings := &indexer.IndexSettings{}
	err := json.Unmarshal([]byte(file), indexSettings)

	if err != nil {
		return nil, fmt.Errorf("Error while reading index metadata: %s: %v", indexName, err)
	}
	return indexSettings, nil
}
