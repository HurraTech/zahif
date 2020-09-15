package main

import (
    "context"
    "fmt"
    "github.com/beeker1121/goque"
    "github.com/expectedsh/go-sonic/sonic"
    "github.com/jessevdk/go-flags"
    log "github.com/sirupsen/logrus"
    "google.golang.org/grpc"
    "net"
    "time"

    "hurracloud.io/zahif/internal/indexer"
    pb "hurracloud.io/zahif/internal/proto"
)

type Options struct {
    Parallelism   int    `short:"p" long:"parallelism" env:"PARALLELISM" description:"How many parallel indexing threads" default:"5"`
    MetadataDir   string `short:"d" long:"metadta_dir" env:"METADATA_DIR" description:"Where to store metadta about indices" default:"."`
    SonicHost     string `short:"h" long:"sonic_host" env:"SONIC_HOST" description:"Sonic server host" default:"127.0.0.1"`
    SonicPort     int    `short:"P" long:"sonic_port" env:"SONIC_PORT" description:"Sonic server port" default:"1491"`
    SonicPassword string `short:"s" long:"sonic_pssword" env:"SONIC_PASSWORD" description:"Sonic server password" default:"SecretPassword"`
    Verbose       bool   `short:"v" long:"verbose" description:"Enable verbose logging"`
    Mode          string `short:"m" long:"mode" env:"MODE" description:"Zahif mode ('batch' or 'single')" default:"batch" choice:"batch" choice:"single"`
    Listen        string `short:"L" long:"listen" env:"LISTEN" description:"Address to bind server to" default:"127.0.0.1"`
    Port          int    `short:"o" long:"port" env:"PORT" description:"Port to bind server to" default:"10001"`
}

type zahifServer struct {
    pb.UnimplementedZahifServer
    BatchQueue   *goque.Queue
    ControlQueue *goque.Queue
}

type ControlIndexOp struct {
    Type            string
    IndexIdentifier string
}

var options Options
var ingester sonic.Ingestable
var search sonic.Searchable
var retriesQueue *goque.Queue
var batchQueue *goque.Queue
var controlQueue *goque.Queue
var zahif zahifServer
var interruptChannel chan string
var currentRunningJob string

func main() {
    _, err := flags.Parse(&options)

    if err != nil {
        panic(err)
    }

    if options.Verbose {
        log.SetLevel(log.DebugLevel)
    }

    ingester, err = sonic.NewIngester(options.SonicHost, options.SonicPort, options.SonicPassword)
    if err != nil {
        log.Fatalf("Failed to connect to sonic server: %v", err)
    }

    search, err = sonic.NewSearch(options.SonicHost, options.SonicPort, options.SonicPassword)
    if err != nil {
        log.Fatalf("Failed to connect to sonic server: %v", err)
    }

    retriesQueue, err = goque.OpenQueue(fmt.Sprintf("%s/single.queue", options.MetadataDir))
    if err != nil {
        log.Fatalf("Failed to open single queue: %v", err)
    }
    defer retriesQueue.Close()

    batchQueue, err = goque.OpenQueue(fmt.Sprintf("%s/batch.queue", options.MetadataDir))
    if err != nil {
        log.Fatalf("Failed to open batch queue: %v", err)
    }
    defer batchQueue.Close()

    controlQueue, err = goque.OpenQueue(fmt.Sprintf("%s/control.queue", options.MetadataDir))
    if err != nil {
        log.Fatalf("Failed to open control queue: %v", err)
    }
    defer batchQueue.Close()

    interruptChannel = make(chan string)

    lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", options.Listen, options.Port))
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    go processBatchJobs()
    go processControlJobs()

    log.Infof("Zahif Server listening on %s:%d", options.Listen, options.Port)
    var opts []grpc.ServerOption
    grpcServer := grpc.NewServer(opts...)
    zahif = zahifServer{BatchQueue: batchQueue, ControlQueue: controlQueue}
    pb.RegisterZahifServer(grpcServer, &zahif)
    grpcServer.Serve(lis)

}

func (z *zahifServer) BatchIndex(ctx context.Context, req *pb.BatchIndexRequest) (*pb.BatchIndexResponse, error) {
    log.Debugf("Recevied BatchIndex Request: %v", req)
    _, err := z.BatchQueue.EnqueueObject(req)
    if err != nil {
        return nil, fmt.Errorf("Failed to queue batch index job: %v", err)
    }
    return &pb.BatchIndexResponse{}, nil
}

func (z *zahifServer) IndexProgress(ctx context.Context, req *pb.IndexProgressRequest) (*pb.IndexProgressResponse, error) {
    indexer := indexer.BatchIndexer{
        MetadataDir:     options.MetadataDir,
        IndexIdentifier: req.IndexIdentifier,
        Parallelism:     options.Parallelism,
    }

    total, indexed, percentage, err := indexer.CheckProgress()

    if err != nil {
        log.Errorf("Error while checking on index %s progess: %v", req.IndexIdentifier, err)
        return nil, err
    }
    log.Debugf("Progress of index %s is at %f (is_running=%v)", req.IndexIdentifier, percentage, currentRunningJob == req.IndexIdentifier)

    return &pb.IndexProgressResponse{PercentageDone: float32(percentage),
        IndexedDocuments: int32(indexed),
        TotalDocuments:   int32(total),
        IsRunning:        currentRunningJob == req.IndexIdentifier,
    }, nil
}

func (z *zahifServer) DeleteIndex(ctx context.Context, req *pb.DeleteIndexRequest) (*pb.DeleteIndexResponse, error) {
    _, err := z.ControlQueue.EnqueueObject(&ControlIndexOp{Type: "delete", IndexIdentifier: req.IndexIdentifier})
    if err != nil {
        return nil, fmt.Errorf("Failed to queue delete index job: %v", err)
    }
    return &pb.DeleteIndexResponse{}, nil
}

func (z *zahifServer) StopIndex(ctx context.Context, req *pb.StopIndexRequest) (*pb.StopIndexResponse, error) {
    _, err := z.ControlQueue.EnqueueObject(&ControlIndexOp{Type: "stop", IndexIdentifier: req.IndexIdentifier})
    if err != nil {
        return nil, fmt.Errorf("Failed to queue stop index job: %v", err)
    }
    return &pb.StopIndexResponse{}, nil
}

func (z *zahifServer) SearchIndex(ctx context.Context, req *pb.SearchIndexRequest) (*pb.SearchIndexResponse, error) {
    log.Debugf("Recevied SearchIndex Request. IndexID=%s", req.IndexIdentifier)
    results, err := search.Query("Files", req.IndexIdentifier, req.Query, int(req.Limit), int(req.Offset))
    if err != nil {
        return nil, fmt.Errorf("Error while fulfilling search request: %s", err)
    }
    if len(results) == 1 && results[0] == "" {
        results = []string{} // removing sonic empty result element
    }
    log.Debugf("Search Results (%d): %v", len(results), results)

    return &pb.SearchIndexResponse{Documents: results}, nil
}

func processControlJobs() {
    ticker := time.NewTicker(1000 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
        }

        log.Trace("Polling Control Queue")
        item, err := controlQueue.Peek()
        if err == goque.ErrEmpty {
            log.Trace("No jobs found in Control Queue")
            continue
        }
        if err != nil {
            log.Errorf("Failed to poll Control Queue: %v", err)
            continue
        }

        var req ControlIndexOp
        err = item.ToObject(&req)

        switch req.Type {
        case "delete":
            log.Infof("Processing delete job for index '%s'", req.IndexIdentifier)
            // Fulfill indexing request
            indexer := indexer.BatchIndexer{
                MetadataDir:     options.MetadataDir,
                IndexIdentifier: req.IndexIdentifier,
                Parallelism:     options.Parallelism,
            }

            interruptChannel <- req.IndexIdentifier

            // Delete index (metadata and storage)
            err = indexer.DeleteIndex()

            if err != nil {
                log.Errorf("Error while deleting index %s: %v", req.IndexIdentifier, err)
                continue
            }
            log.Infof("Index '%s' has been deleted successfully", req.IndexIdentifier)
        case "stop":
            log.Infof("Processing stop request for index  '%s'", req.IndexIdentifier)
            interruptChannel <- req.IndexIdentifier
        }

        // On completion, remove from queue
        _, err = controlQueue.Dequeue()
        if err != nil {
            log.Errorf("Failed to dequeue job from queue: %v", err)
        }

    }
}

func processBatchJobs() {

    ticker := time.NewTicker(1000 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
        }

        log.Trace("Polling Batch Queue")
        item, err := batchQueue.Peek()
        if err == goque.ErrEmpty {
            log.Trace("No jobs found in Batch Queue")
            // Let's unblock any delete jobs waiting on interruption requests
            select {
            case <-interruptChannel:
            default:
            }
            continue
        }
        if err != nil {
            log.Errorf("Failed to poll Batch Queue: %v", err)
            panic(err)
        }

        var indexRequest pb.BatchIndexRequest
        err = item.ToObject(&indexRequest)

        log.Debugf("Processing batch job for index '%s'", indexRequest.IndexIdentifier)
        // Fulfill indexing request
        indexer := indexer.BatchIndexer{
            Target:           indexRequest.Target,
            MetadataDir:      options.MetadataDir,
            IndexIdentifier:  indexRequest.IndexIdentifier,
            Parallelism:      options.Parallelism,
            Ingester:         ingester,
            ExcludePatterns:  indexRequest.ExcludePatterns,
            RetriesQueue:     retriesQueue,
            InterruptChannel: interruptChannel,
        }
        currentRunningJob = indexRequest.IndexIdentifier
        err = indexer.Index()
        if err != nil {
            log.Errorf("Failed while indexing %s: %v", indexRequest.IndexIdentifier, err)
            continue
        }
        currentRunningJob = ""

        log.Infof("Indexing '%s' has completed successfully", indexRequest.IndexIdentifier)

        // On completion, remove from queue
        _, err = batchQueue.Dequeue()
        if err != nil {
            log.Errorf("Failed to dequeue job from queue: %v", err)
        }
    }
}
