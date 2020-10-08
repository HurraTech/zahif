package utils

import (
	"os"

	"github.com/gabriel-vasile/mimetype"
	log "github.com/sirupsen/logrus"
)

func IsIndexable(path string, maxFileSizeMB int) bool {
	log.Tracef("Checking if %v is indexable", path)

	stat, err := os.Stat(path)
	if err != nil {
		log.Errorf("Could not stat %s. Will assume non-indexable: %s", path, err)
		return false
	}

	if mode := stat.Mode(); mode.IsDir() {
		return false // path is a directory
	}

	if int(stat.Size()/(1024*1024)) >= maxFileSizeMB {
		log.Infof("File %s (size=%d bytes) is larger than threshold %dMB, will not index", path, stat.Size(), maxFileSizeMB)
		return false
	}

	detectedMIME, err := mimetype.DetectFile(path)
	if err != nil {
		log.Warningf("Could not detect MIME for %s. Assuming file is indexable", path)
		return true // could not detect MIME; let's index anyway
	}

	for mime := detectedMIME; mime != nil; mime = mime.Parent() {
		if mime.Is("text/plain") {
			return true // flie is text
		}
	}

	return false // file is binray

}
