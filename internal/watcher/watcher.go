package watcher

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/beeker1121/goque"
	rwatcher "github.com/radovskyb/watcher"
	log "github.com/sirupsen/logrus"

	"hurracloud.io/zahif/internal/store"
)

type Watcher struct {
	MetadataDir string
	IndexQueue  *goque.Queue
	rootDirMap  map[string]string
	store       *store.Store
	w           *rwatcher.Watcher
}

type FileIndexRequest struct {
	FilePath        string
	IndexIdentifier string
}

func New(metadataDir string, indexQueue *goque.Queue) *Watcher {
	return &Watcher{
		MetadataDir: metadataDir,
		IndexQueue:  indexQueue,
		store: &store.Store{
			MetadataDir: metadataDir,
		},
		rootDirMap: make(map[string]string),
	}
}

func (w *Watcher) Watch() error {
	_w := rwatcher.New()
	w.w = _w
	w.rootDirMap = map[string]string{}

	if plans, err := filepath.Glob(path.Join(w.MetadataDir, "**", "settings.json")); err == nil {
		for _, settingsFile := range plans {
			indexName := path.Dir(settingsFile)

			paused, err := w.isPaused(indexName)
			if err != nil {
				log.Warnf("Could not determine if watching %s is paused, will assume not to avoid missing new data: %s", indexName, err)
				paused = false
			}

			if paused {
				log.Warningf("index %s watcher is paused", indexName)
				continue
			}

			if err := w.addWatcher(indexName); err != nil {
				log.Debugf("Could not setup watcher for index: %s: %v", indexName, err)
			}
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
				log.Debugf("Enqueue index request for changed file %s in index %s", event.Name(), indexName)
				req := FileIndexRequest{IndexIdentifier: indexName, FilePath: event.Path}
				w.IndexQueue.EnqueueObject(&req)
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
	pausedFile := path.Join(w.MetadataDir, indexName, "paused")
	f, err := os.Create(pausedFile)
	if err != nil {
		return fmt.Errorf("Could not create pause file: %s", err)
	}
	defer f.Close()

	return w.removeWatcher(indexName)
}

func (w *Watcher) StartOrResumeWatching(indexName string) error {
	// Remove .paused file if exsts (relevant for ResumeWatching jobs)
	pausedFile := path.Join(w.MetadataDir, indexName, "paused")
	if _, err := os.Stat(pausedFile); err == nil {
		err = os.Remove(pausedFile)
		if err != nil {
			return fmt.Errorf("Could not remove pause file: %s", err)
		}
	}

	return w.addWatcher(indexName)
}

func (w *Watcher) addWatcher(indexName string) error {
	indexSettings, err := w.store.ReadSettingsFromDisk(indexName)
	if err != nil {
		return fmt.Errorf("Error while reading index metadata. Will not setup watcher: %s: %v", indexName, err)
	}

	w.rootDirMap[indexSettings.Target] = indexName

	log.Infof("Setting up watcher for index: %s at location: %s", indexName, indexSettings.Target)
	return w.w.AddRecursive(indexSettings.Target)
}

func (w *Watcher) removeWatcher(indexName string) error {
	indexSettings, err := w.store.ReadSettingsFromDisk(indexName)
	if err != nil {
		return fmt.Errorf("Error while reading index metadata. Will not setup watcher: %s: %v", indexName, err)
	}

	log.Info("Removing watcher of index: ", indexName)
	return w.w.RemoveRecursive(indexSettings.Target)
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
	pausedFile := path.Join(w.MetadataDir, indexName, "paused")
	_, err := os.Stat(pausedFile)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil

}
