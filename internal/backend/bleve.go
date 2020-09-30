package backend

import (
	"fmt"
	"os"
	"path"

	"github.com/blevesearch/bleve"
	log "github.com/sirupsen/logrus"
)

type Bleve struct {
	metadataDir string
	parallelism int
	host        string
	port        int
	password    string
	openIndices map[string]bleve.Index
}

func NewBleveBackend(metadataDir string) (*Bleve, error) {
	indexMap := make(map[string]bleve.Index)
	return &Bleve{
		metadataDir: metadataDir,
		openIndices: indexMap,
	}, nil
}

func (b *Bleve) IndexFiles(indexName string, docs []Document) error {
	index, err := b.openIndex(indexName)
	if err != nil {
		return err
	}

	batch := index.NewBatch()
	for _, d := range docs {
		batch.Index(d.ID, d.Content)
	}

	err = index.Batch(batch)
	if err != nil {
		return fmt.Errorf("Bleve error while batch indexing: %s: %v", indexName, err)
	}
	return nil
}

func (b *Bleve) IndexFile(indexName string, d Document) error {
	index, err := b.openIndex(indexName)
	if err != nil {
		return err
	}
	return index.Index(d.ID, d.Content)
}

func (b *Bleve) DeleteIndex(indexName string) error {
	err := os.Remove(b.indexPath(indexName))
	if err != nil {
		return err
	}
	return nil
}

func (b *Bleve) SearchIndex(indexName string, query string, from int, limit int) ([]string, error) {
	index, err := b.openIndex(indexName)
	if err != nil {
		return nil, err
	}

	bq := bleve.NewMatchQuery(query)
	search := bleve.NewSearchRequestOptions(bq, limit, from, false)
	searchResults, err := index.Search(search)

	if err != nil {
		log.Errorf("Error while searching bleve index: %s: %s", indexName, err)
		return nil, err
	}

	var results []string
	for _, h := range searchResults.Hits {
		results = append(results, h.ID)
	}
	return results, nil
}

func (b *Bleve) openIndex(indexName string) (bleve.Index, error) {
	if index, ok := b.openIndices[indexName]; ok {
		return index, nil
	}

	if _, err := os.Stat(b.indexPath(indexName)); os.IsNotExist(err) {
		index, err := bleve.New(b.indexPath(indexName), bleve.NewIndexMapping())
		if err != nil {
			return nil, fmt.Errorf("Bleve error while creating new index: %s: %v", indexName, err)
		}
		return index, nil
	}

	index, err := bleve.Open(b.indexPath(indexName))
	if err != nil {
		return nil, fmt.Errorf("Bleve error while opening index: %s: %v", indexName, err)
	}
	b.openIndices[indexName] = index
	return b.openIndices[indexName], nil
}

func (b *Bleve) indexPath(indexName string) string {
	return path.Join(b.metadataDir, fmt.Sprintf("%s.bleve", indexName))
}
