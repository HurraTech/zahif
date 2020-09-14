package utils

import (
	"github.com/gabriel-vasile/mimetype"
	log "github.com/sirupsen/logrus"
	"os"
)

func IsIndexable(path string) bool {
	log.Tracef("Checking if %v is indexable", path)
	detectedMIME, err := mimetype.DetectFile(path)

	stat, err := os.Stat(path)
	if stat != nil {
		if mode := stat.Mode(); mode.IsDir() {
			return false // path is a directory
		}
	} else {
		return false
	}

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
