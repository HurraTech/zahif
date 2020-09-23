package watcher

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/beeker1121/goque"
	"github.com/radovskyb/watcher"
	log "github.com/sirupsen/logrus"
	"hurracloud.io/zahif/internal/indexer"
)

type Watcher struct {
	MetadataDir string
	IndexQueue  *goque.Queue
	rootDirMap  map[string]string
	w           *watcher.Watcher
}

func (w *Watcher) Watch() error {
	_w := watcher.New()
	w.w = _w
	w.rootDirMap = map[string]string{}

	if plans, err := filepath.Glob(path.Join(w.MetadataDir, "*.plan")); err == nil {
		for _, planFile := range plans {
			indexName := strings.Split(planFile, ".")[0]
			paused, err := w.isPaused(indexName)
			if err != nil {
				log.Warnf("Could not determine if watching %s is paused, will assume not to avoid missing new data: %s", indexName, err)
				paused = false
			}

			if paused {
				log.Warningf("index %s watcher is paused", indexName)
				continue
			}

			log.Info("Setting up watcher for index: ", indexName)

			rootDir, err := w.findIndexRootDir(indexName)

			if err != nil {
				log.Errorf("Error determine watch directory for index %s: %s", indexName, err)
				continue
			}

			if err := w.w.AddRecursive(rootDir); err != nil {
				log.Errorf("Could not setup watcher for %s: %s", rootDir, err)
				continue
			}
			log.Debugf("Will watch changes in directory %s for indexing in %s", rootDir, indexName)
		}
	}

	go func() {
		for {
			select {
			case event := <-w.w.Event:
				log.Tracef("Watcher event: %s", event) // Print the event's info.
				// Which index does this changed file belong to?
				indexName, err := w.findIndexNameOfFile(event.Path)
				if err != nil {
					log.Errorf("Error handling changed file: %s", err)
					continue
				}
				log.Tracef("Enqueue index request for changed file %s in index %s", event.Name, indexName)
				w.IndexQueue.EnqueueObject(&indexer.FileIndexRequest{IndexIdentifier: indexName})
			case err := <-w.w.Error:
				log.Errorf("Watcher Error: %s", err)
			case <-w.w.Closed:
				return
			}
		}
	}()

	if err := w.w.Start(time.Millisecond * 100); err != nil {
		return fmt.Errorf("Error starting watcher: %s", err)
	}

	return nil
}

func (w *Watcher) StopWatching(indexName string) error {
	pausedFile := path.Join(w.MetadataDir, fmt.Sprintf("%s.paused", indexName))
	f, err := os.Create(pausedFile)
	if err != nil {
		return fmt.Errorf("Could not create pause file: %s", err)
	}
	defer f.Close()

	dir, err := w.findIndexRootDir(indexName)
	if err != nil {
		return err
	}

	log.Infof("Pausing watcher for index %s", indexName)

	return w.w.RemoveRecursive(dir)
}

func (w *Watcher) ResumeWatching(indexName string) error {
	pausedFile := path.Join(w.MetadataDir, fmt.Sprintf("%s.paused", indexName))
	err := os.Remove(pausedFile)
	if err != nil {
		return fmt.Errorf("Could not remove pause file: %s", err)
	}

	dir, err := w.findIndexRootDir(indexName)
	if err != nil {
		return err
	}

	log.Infof("Resuming watcher for index %s", indexName)

	return w.w.AddRecursive(dir)
}

func (w *Watcher) findIndexRootDir(indexName string) (string, error) {
	f, err := os.Open(path.Join(w.MetadataDir, fmt.Sprintf("%s.plan", indexName)))
	if err != nil {
		return "", fmt.Errorf("Error reading metadata of index: %s", indexName)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var rootDir string
	if scanner.Scan() {
		rootDir = scanner.Text()
		w.rootDirMap[rootDir] = indexName
		return rootDir, nil
	} else {
		return "", fmt.Errorf("Index '%s' metadata file unexpectedly blank", indexName)
	}

}

func (w *Watcher) findIndexNameOfFile(filePath string) (string, error) {
	for dir, index := range w.rootDirMap {
		if strings.HasPrefix(filePath, dir) {
			log.Tracef("File %s belongs to index %s", filePath, index)
			return index, nil
		}
	}
	return "", fmt.Errorf("Could not find index of file %s", filePath)
}

func (w *Watcher) isPaused(indexName string) (bool, error) {
	pausedFile := path.Join(w.MetadataDir, fmt.Sprintf("%s.paused", indexName))
	_, err := os.Stat(pausedFile)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil

}
