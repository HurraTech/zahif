package main

import (
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"hurracloud.io/zahif/internal/backend"
	"hurracloud.io/zahif/internal/server"
	"hurracloud.io/zahif/internal/watcher"
)

type Options struct {
	Parallelism       int    `short:"p" long:"parallelism" env:"PARALLELISM" description:"How many parallel indexing threads" default:"2"`
	MetadataDir       string `short:"d" long:"metadta_dir" env:"METADATA_DIR" description:"Where to store metadta about indices" default:"."`
	Backend           string `short:"b" long:"backend" env:"BACKEND" description:"Which backend to use, sonic or bleve" default:"bleve"`
	SonicHost         string `short:"h" long:"sonic_host" env:"SONIC_HOST" description:"Sonic server host" default:"127.0.0.1"`
	SonicPort         int    `short:"P" long:"sonic_port" env:"SONIC_PORT" description:"Sonic server port" default:"1491"`
	SonicPassword     string `short:"s" long:"sonic_pssword" env:"SONIC_PASSWORD" description:"Sonic server password" default:"SecretPassword"`
	Verbose           []bool `short:"v" long:"verbose" description:"Enable verbose logging"`
	Mode              string `short:"m" long:"mode" env:"MODE" description:"Zahif mode ('batch' or 'single')" default:"batch" choice:"batch" choice:"single"`
	Listen            string `short:"L" long:"listen" env:"LISTEN" description:"Address to bind server to" default:"127.0.0.1"`
	Port              int    `short:"o" long:"port" env:"PORT" description:"Port to bind server to" default:"10001"`
	FileSizeThreshold int    `short:"S" long:"filesize_threshold" env:"FILESIZE_THRESHOLD" description:"Specify a size threshold for files to index (in MB)" default:"30"`
}

var searchBackend backend.SearchBackend

var options Options

func main() {
	_, err := flags.Parse(&options)

	if err != nil {
		panic(err)
	}

	if len(options.Verbose) == 1 {
		log.SetLevel(log.DebugLevel)
	} else if len(options.Verbose) == 2 {
		log.SetLevel(log.TraceLevel)
	}

	if options.Backend == "sonic" {
		searchBackend, err = backend.NewSonicBackend(options.SonicHost, options.SonicPort, options.SonicPassword, options.Parallelism)
	} else if options.Backend == "bleve" {
		searchBackend, err = backend.NewBleveBackend(options.MetadataDir)
	} else {
		log.Fatalf("Invalid backend '%s', currently supported backends are: sonic, bleve", options.Backend)
	}

	if err != nil {
		log.Fatalf("Error connecting to search backend: %s", err)
	}

	s, err := server.NewZahifServer(searchBackend, options.Listen, options.Port, options.MetadataDir, options.Parallelism, options.FileSizeThreshold)
	if err != nil {
		log.Fatalf("Failed creating zahif server: %v", err)
	}

	watcher := watcher.New(options.MetadataDir, s.IndexQueue)

	s.Watcher = watcher // to stop watcher on StopWatching requests

	go watcher.Watch()

	s.Start()
}
