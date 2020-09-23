package indexer

import (
	log "github.com/sirupsen/logrus"
	"hurracloud.io/zahif/internal/backend"
)

type FileIndexRequest struct {
	FilePath        string
	IndexIdentifier string
}

type FileIndexer struct {
	backend.SearchBackend
}

func (w *FileIndexer) IndexFile(req FileIndexRequest) {
	log.Tracef("Indexing %s in %s", req.FilePath, req.IndexIdentifier)
}
