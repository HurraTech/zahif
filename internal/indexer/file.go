package indexer

import (
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"hurracloud.io/zahif/internal/backend"
	"hurracloud.io/zahif/internal/indexer/utils"
)

type FileIndexer struct {
	Backend           backend.SearchBackend
	IndexSettings     *IndexSettings
	FileSizeThreshold int
}

func (i *FileIndexer) IndexFile(filePath string) error {
	log.Tracef("Indexing %s in %s", filePath, i.IndexSettings.IndexIdentifier)

	if !utils.IsIndexable(filePath, i.FileSizeThreshold) {
		log.Tracef("File not indexable, skipping: %s", filePath)
		return nil
	}

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("Failed to read: %s: %s", filePath, err)
	}

	record := backend.Document{ID: filePath, Content: string(content)}
	return i.Backend.IndexFile(i.IndexSettings.IndexIdentifier, record)
}
