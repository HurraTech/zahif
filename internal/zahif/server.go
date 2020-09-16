package zahif

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/beeker1121/goque"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"hurracloud.io/zahif/internal/indexer"
	"hurracloud.io/zahif/internal/search/backend"
	pb "hurracloud.io/zahif/internal/zahif/proto"
)

type ZahifServer struct {
	pb.UnimplementedZahifServer

	backend           backend.SearchBackend
	retriesQueue      *goque.Queue
	batchQueue        *goque.Queue
	controlQueue      *goque.Queue
	interruptChannel  chan string
	currentRunningJob string
	store             *store
	listen            string
	port              int
	parallelism       int
}

type controlIndexOp struct {
	Type            string
	IndexIdentifier string
}

func NewZahif(searchBackend backend.SearchBackend, listen string, port int, metadataDir string, parallelism int) (*ZahifServer, error) {

	interruptChannel := make(chan string)

	retriesQueue, err := goque.OpenQueue(fmt.Sprintf("%s/single.queue", metadataDir))
	if err != nil {
		return nil, fmt.Errorf("Failed to open single queue: %v", err)
	}

	batchQueue, err := goque.OpenQueue(fmt.Sprintf("%s/batch.queue", metadataDir))

	if err != nil {
		return nil, fmt.Errorf("Failed to open batch queue: %v", err)
	}

	controlQueue, err := goque.OpenQueue(fmt.Sprintf("%s/control.queue", metadataDir))
	if err != nil {
		return nil, fmt.Errorf("Failed to open control queue: %v", err)
	}

	zahifStore := &store{
		MetadataDir:      metadataDir,
		SearchBackend:    searchBackend,
		InterruptChannel: interruptChannel,
		RetriesQueue:     retriesQueue,
	}

	return &ZahifServer{
		batchQueue:       batchQueue,
		controlQueue:     controlQueue,
		interruptChannel: interruptChannel,
		backend:          searchBackend,
		store:            zahifStore,
		listen:           listen,
		port:             port,
		parallelism:      parallelism,
	}, nil

}

func (z *ZahifServer) Start() error {

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterZahifServer(grpcServer, z)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", z.listen, z.port))
	if err != nil {
		return fmt.Errorf("Failed to listen: %v", err)
	}

	defer z.batchQueue.Close()
	defer z.controlQueue.Close()
	defer z.retriesQueue.Close()

	go z.processBatchJobs()
	go z.processControlJobs()

	log.Infof("Zahif Server listening on %s:%d", z.listen, z.port)
	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("Zahif failed to listen: %v", err)
	}

	return nil
}

func (z *ZahifServer) BatchIndex(ctx context.Context, req *pb.BatchIndexRequest) (*pb.BatchIndexResponse, error) {
	log.Debugf("Received BatchIndex Request: %v", req)
	_, err := z.batchQueue.EnqueueObject(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to queue batch index job: %v", err)
	}
	return &pb.BatchIndexResponse{}, nil
}

func (z *ZahifServer) IndexProgress(ctx context.Context, req *pb.IndexProgressRequest) (*pb.IndexProgressResponse, error) {
	indexer, err := z.store.GetIndexer(req.IndexIdentifier)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	total, indexed, percentage, err := indexer.CheckProgress()

	if err != nil {
		log.Errorf("Error while checking on index %s progess: %v", req.IndexIdentifier, err)
		return nil, err
	}
	log.Tracef("Progress of index %s is at %f (is_running=%v)", req.IndexIdentifier, percentage, z.currentRunningJob == req.IndexIdentifier)

	return &pb.IndexProgressResponse{PercentageDone: float32(percentage),
		IndexedDocuments: int32(indexed),
		TotalDocuments:   int32(total),
		IsRunning:        z.currentRunningJob == req.IndexIdentifier,
	}, nil
}

func (z *ZahifServer) DeleteIndex(ctx context.Context, req *pb.DeleteIndexRequest) (*pb.DeleteIndexResponse, error) {
	_, err := z.controlQueue.EnqueueObject(&controlIndexOp{Type: "delete", IndexIdentifier: req.IndexIdentifier})
	if err != nil {
		return nil, fmt.Errorf("Failed to queue delete index job: %v", err)
	}
	return &pb.DeleteIndexResponse{}, nil
}

func (z *ZahifServer) StopIndex(ctx context.Context, req *pb.StopIndexRequest) (*pb.StopIndexResponse, error) {
	_, err := z.controlQueue.EnqueueObject(&controlIndexOp{Type: "stop", IndexIdentifier: req.IndexIdentifier})
	if err != nil {
		return nil, fmt.Errorf("Failed to queue stop index job: %v", err)
	}
	return &pb.StopIndexResponse{}, nil
}

func (z *ZahifServer) SearchIndex(ctx context.Context, req *pb.SearchIndexRequest) (*pb.SearchIndexResponse, error) {
	log.Debugf("Received Search Request. Query=%s, IndexID=%s", req.Query, req.IndexIdentifier)
	results, err := z.backend.SearchIndex(req.IndexIdentifier, req.Query, int(req.Offset), int(req.Limit))
	log.Debugf("Search Results (%d): %v", len(results), results)
	if err != nil {
		log.Errorf("Error while searching index %s :%v", req.IndexIdentifier, err)
		return nil, err
	}

	return &pb.SearchIndexResponse{Documents: results}, nil
}

func (z *ZahifServer) processControlJobs() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		}

		log.Trace("Polling Control Queue")
		item, err := z.controlQueue.Peek()
		if err == goque.ErrEmpty {
			log.Trace("No jobs found in Control Queue")
			continue
		}
		if err != nil {
			log.Errorf("Failed to poll Control Queue: %v", err)
			continue
		}

		var req controlIndexOp
		err = item.ToObject(&req)

		switch req.Type {
		case "delete":
			log.Infof("Processing delete job for index '%s'", req.IndexIdentifier)
			// Fulfill indexing request
			indexer, err := z.store.GetIndexer(req.IndexIdentifier)
			if err != nil {
				log.Errorf("Could not rerieve indexer metadata: %s: %v", req.IndexIdentifier, err)
				continue
			}

			z.interruptChannel <- req.IndexIdentifier

			// Delete index (metadata and storage)
			err = indexer.DeleteIndex()
			if err != nil {
				log.Errorf("Error while deleting index %s: %v", req.IndexIdentifier, err)
				continue
			}
			z.store.DeleteIndexer(req.IndexIdentifier)

			log.Infof("Index '%s' has been deleted successfully", req.IndexIdentifier)
		case "stop":
			log.Infof("Processing stop request for index  '%s'", req.IndexIdentifier)
			z.interruptChannel <- req.IndexIdentifier
		}

		// On completion, remove from queue
		_, err = z.controlQueue.Dequeue()
		if err != nil {
			log.Errorf("Failed to dequeue job from queue: %v", err)
		}

	}
}

func (z *ZahifServer) processBatchJobs() {

	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		}

		log.Trace("Polling Batch Queue")
		item, err := z.batchQueue.Peek()
		if err == goque.ErrEmpty {
			log.Trace("No jobs found in Batch Queue")
			// Let's ZahifServerunblock any delete jobs waiting on interruption requests
			select {
			case <-z.interruptChannel:
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
		settings := &indexer.IndexSettings{
			Target:          indexRequest.Target,
			IndexIdentifier: indexRequest.IndexIdentifier,
			ExcludePatterns: indexRequest.ExcludePatterns,
			Parallelism:     z.parallelism,
		}
		indexer, err := z.store.NewIndexer(settings)
		if err != nil {
			log.Errorf("Error creating index metadata :%s: %v", indexRequest.IndexIdentifier, err)
			continue
		}

		z.currentRunningJob = indexRequest.IndexIdentifier
		err = indexer.Index()
		if err != nil {
			log.Errorf("Failed while indexing %s: %v", indexRequest.IndexIdentifier, err)
			continue
		}
		z.currentRunningJob = ""

		log.Infof("Indexing '%s' has completed successfully", indexRequest.IndexIdentifier)

		// On completion, remove from queue
		_, err = z.batchQueue.Dequeue()
		if err != nil {
			log.Errorf("Failed to dequeue job from queue: %v", err)
		}
	}
}
