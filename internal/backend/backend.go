package backend

type SearchBackend interface {
	IndexFiles(indexName string, docs []Document) error
	DeleteIndex(indexName string) error
	SearchIndex(inexName string, query string, from int, limit int) ([]string, error)
}

type IndexDoc struct {
	path    string
	content string
}

type Document struct {
	ID      string
	Content string
}
