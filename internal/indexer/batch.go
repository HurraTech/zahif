package indexer

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"syscall"

	"github.com/beeker1121/goque"
	log "github.com/sirupsen/logrus"

	"hurracloud.io/zahif/internal/backend"
	"hurracloud.io/zahif/internal/indexer/utils"
)

type BatchIndexer struct {
	IndexSettings           *IndexSettings
	MetadataDir             string
	Backend                 backend.SearchBackend
	RetriesQueue            *goque.Queue
	InterruptChannel        <-chan string
	planCurrentPositionByte int64
	planCurrentPositionLine int
	indexPlanFile           string
	indexProgressFile       string
	progressFileMutex       sync.Mutex
}

func (z *BatchIndexer) Index() error {
	z.indexPlanFile = fmt.Sprintf("%s/%s.plan", z.MetadataDir, z.IndexSettings.IndexIdentifier)
	z.indexProgressFile = fmt.Sprintf("%s/%s.progress", z.MetadataDir, z.IndexSettings.IndexIdentifier)

	_, err := os.Stat(z.indexPlanFile)
	totalDocs := -1
	if os.IsNotExist(err) {
		totalDocs, err = z.buildIndexPlan()
		if err != nil {
			return fmt.Errorf("Failed to build index plan file: %s", err)
		}
	}

	if err = z.executeIndexPlan(totalDocs); err != nil {
		return fmt.Errorf("Failed while indexing %s: %v", z.IndexSettings.IndexIdentifier, err)
	}

	return nil
}

func (z *BatchIndexer) CheckProgress() (int, int, float64, error) {
	z.indexProgressFile = fmt.Sprintf("%s/%s.progress", z.MetadataDir, z.IndexSettings.IndexIdentifier)

	if _, err := os.Stat(z.indexProgressFile); os.IsNotExist(err) {
		log.Tracef("Could not find progress file for index %s. Assuming progress is 0", z.IndexSettings.IndexIdentifier)
		return 0, 0, 0, nil
	}

	f, err := os.Open(z.indexProgressFile)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Could not open index progress file: %s: %s", z.indexProgressFile, err)
	}
	defer f.Close()

	_, cur, total, err := readProgressFileValues(f)

	if err != nil {
		return 0, 0, 0, fmt.Errorf("Error reading progres file values: %s", err)
	}

	return total, cur, math.Ceil((float64(cur) / float64(total)) * 100), nil
}

func (z *BatchIndexer) DeleteIndex() error {
	z.indexPlanFile = fmt.Sprintf("%s/%s.plan", z.MetadataDir, z.IndexSettings.IndexIdentifier)
	z.indexProgressFile = fmt.Sprintf("%s/%s.progress", z.MetadataDir, z.IndexSettings.IndexIdentifier)
	os.Remove(z.indexPlanFile)
	os.Remove(z.indexProgressFile)

	if err := z.Backend.DeleteIndex(z.IndexSettings.IndexIdentifier); err != nil {
		return fmt.Errorf("Failed to flush collection on sonic: %v", err)
	}

	return nil
}

func (z *BatchIndexer) buildIndexPlan() (int, error) {
	log.Infof("Build index plan of index %s", z.IndexSettings.IndexIdentifier)

	f, err := os.OpenFile(z.indexPlanFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Compile exclude patterns
	var compiledPatterns []*regexp.Regexp
	for _, pattern := range z.IndexSettings.ExcludePatterns {
		re, err := regexp.Compile(pattern)
		if err == nil {
			compiledPatterns = append(compiledPatterns, re)
			log.Debugf("Added ExcludePattern: %s", pattern)
		} else {
			log.Warningf("Invalid regexp was provided in ExcludedPatterns, escaping pattern: %s", pattern)
			re, err = regexp.Compile(regexp.QuoteMeta(pattern))
			if err == nil {
				compiledPatterns = append(compiledPatterns, re)
				log.Debugf("Added Escaped ExcludePattern: %s", pattern)
			}
		}
	}

	totalDocs := 0
	err = filepath.Walk(z.IndexSettings.Target, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Warningf("Could not walk target path: %s", err)
			return nil
		}

		for _, re := range compiledPatterns {
			if re.MatchString(path) == true {
				return nil
			}
		}

		if _, err = f.WriteString(fmt.Sprintf("%s\n", path)); err != nil {
			return fmt.Errorf("Could not write to index plan file: %s", err)
		}
		totalDocs++
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("Could not write to index plan file: %s", err)
	}

	return totalDocs, nil
}

func (z *BatchIndexer) executeIndexPlan(totalDocs int) error {
	log.Infof("Start/Resume indexing of index %s", z.IndexSettings.IndexIdentifier)

	pf, perr := os.Open(z.indexProgressFile)
	f, err := os.Open(z.indexPlanFile)
	if err != nil {
		return fmt.Errorf("Could not read index plan file: %s: %s", z.indexPlanFile, err)
	}

	if totalDocs == -1 {
		log.Debugf("Total Docs unknown, will read from progress file")
	}

	// Determine if we're resuming or starting from scratch
	start := int64(0)
	startLine := 0

	if perr == nil {
		start, startLine, totalDocs, err = readProgressFileValues(pf)
		if err != nil {
			return fmt.Errorf("Failed to read progress file: %s", err)
		}

		if _, err := f.Seek(start, os.SEEK_SET); err != nil {
			return fmt.Errorf("Failed to seek plan file to resume position: %s", err)
		}
	}

	defer f.Close()
	pf.Close()

	// Scan plan file while maintaining our progress in bytes and line number
	scanner := bufio.NewScanner(f)
	pos := start
	posLine := startLine
	scanLines := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		pos += int64(advance)
		return
	}
	scanner.Split(scanLines)

	// Setup graceful exit handler to save progress in case of interruption
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		// Save progress
		z.saveProgress(pos, posLine, totalDocs)
		os.Exit(0)
	}()

	// Start indexing
	var bulk []string
OUTER:
	for {
		select {
		case indexID := <-z.InterruptChannel:
			if indexID == z.IndexSettings.IndexIdentifier {
				log.Warningf("Indexing of %s interrupted", z.IndexSettings.IndexIdentifier)
				z.saveProgress(pos, posLine, totalDocs)
				return nil
			} else {
				log.Warningf("Indexing of %s interrupted. But we are not indexing %s currently. Ignoring request", indexID, indexID)
			}
		default:
			for scanner.Scan() {
				path := scanner.Text()
				posLine++
				if !utils.IsIndexable(path) {
					continue
				}

				bulk = append(bulk, path)
				if len(bulk) >= z.IndexSettings.Parallelism {
					z.indexFiles(bulk)
					log.Debugf("Indexed %d out of %d documents (index=%s, progress=%f)", posLine, totalDocs, z.IndexSettings.IndexIdentifier, math.Ceil(float64(posLine)/float64(totalDocs)*100))
					bulk = nil
					z.saveProgress(pos, posLine, totalDocs)
					continue OUTER
				}
			}

			z.saveProgress(pos, posLine, totalDocs)

			if err := scanner.Err(); err != nil {
				return fmt.Errorf("Error while scanning plan file: %s", err)
			}
			return nil
		}
	}

}

func (z *BatchIndexer) indexFiles(paths []string) {
	var records []backend.Document
	for _, path := range paths {
		log.Debugf("Will index %s", path)
		content, err := ioutil.ReadFile(path)
		if err != nil {
			log.Debugf("Failed to read %s. Skipping it", path)
			continue
		}
		record := backend.Document{ID: path, Content: string(content)}
		records = append(records, record)
	}

	log.Infof("Starting Bulk Indexing of %d files (index=%s)", len(records), z.IndexSettings.IndexIdentifier)
	_ = z.Backend.IndexFiles(z.IndexSettings.IndexIdentifier, records)
}

func (z *BatchIndexer) saveProgress(posBytes int64, posLines int, totalLines int) error {
	z.progressFileMutex.Lock()
	f, err := os.OpenFile(z.indexProgressFile, os.O_RDWR|os.O_CREATE, 0644)
	if err == nil {
		err = f.Truncate(0)
		_, err = f.WriteString(fmt.Sprintf("%v\n%v\n%v", posBytes, posLines, totalLines))
	}
	err = f.Close()
	z.progressFileMutex.Unlock()
	return err
}

func readProgressFileValues(file io.Reader) (int64, int, int, error) {
	scanner := bufio.NewScanner(file)
	var pfContentPos string
	var pfContentPosLine string
	var prContentTotalDocs string
	if scanner.Scan() {
		pfContentPos = scanner.Text()
	}

	if scanner.Scan() {
		pfContentPosLine = scanner.Text()
	}

	if scanner.Scan() {
		prContentTotalDocs = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return int64(0), 0, 0, fmt.Errorf("Error reading progress file: %s", err)
	} else {
		start, err := strconv.ParseInt((pfContentPos), 10, 64)
		startLine, lerr := strconv.Atoi(pfContentPosLine)
		totalDocs, terr := strconv.Atoi(prContentTotalDocs)

		if err != nil {
			return int64(0), 0, 0, fmt.Errorf("Error parsing contents of progress file %s: %s", pfContentPos, err)
		}

		if lerr != nil {
			return int64(0), 0, 0, fmt.Errorf("Error parsing contents of progress file %s: %s", pfContentPosLine, err)
		}

		if terr != nil {
			return int64(0), 0, 0, fmt.Errorf("Error parsing contents of progress file %s: %s", prContentTotalDocs, err)
		}
		return start, startLine, totalDocs, nil
	}

}
