package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	aws "github.com/AlCutter/betty/storage/aws"
	f_log "github.com/transparency-dev/formats/log"
	"golang.org/x/mod/sumdb/note"
	"k8s.io/klog/v2"
)

var (
	path                     = flag.String("path", "betty", "Path to log root diretory")
	batchSize                = flag.Int("batch_size", 1, "Size of batch before flushing")
	bundleSize               = flag.Int("bundle_size", 256, "Size of bundles written to S3")
	sequencedBundleMaxSize   = flag.Int("sequence_bundle_size", 100, "Maximum number of entries to store togerther in the sequencing table")
	batchMaxAge              = flag.Duration("batch_max_age", 100*time.Millisecond, "Max age for batch entries before flushing")
	integrateBundleBatchSize = flag.Int("integrate_bundle_batch_size", 4, "Max number of bundles to integrate at a time")
	sequenceWithLock         = flag.Bool("sequence_with_lock", false, "Whether to use a lock for sequencing")
	dedupEntries             = flag.Bool("dedup_entries", false, "Whether to deduplicate entries before sequencing")
	dedupEntriesSeq          = flag.Bool("dedup_entries_seq", false, "Whether to deduplicate entries at sequencing")

	listen = flag.String("listen", ":2024", "Address:port to listen on")

	signer   = flag.String("log_signer", "PRIVATE+KEY+Test-Betty+df84580a+Afge8kCzBXU7jb3cV2Q363oNXCufJ6u9mjOY1BGRY9E2", "Log signer")
	verifier = flag.String("log_verifier", "Test-Betty+df84580a+AQQASqPUZoIHcJAF5mBOryctwFdTV1E0GRY4kEAtTzwB", "log verifier")

	bucketName = flag.String("bucket", "bettylog", "Bucket name")
)

type latency struct {
	sync.Mutex
	total time.Duration
	n     int
	min   time.Duration
	max   time.Duration
}

func (l *latency) Add(d time.Duration) {
	l.Lock()
	defer l.Unlock()
	l.total += d
	l.n++
	if d < l.min {
		l.min = d
	}
	if d > l.max {
		l.max = d
	}
}

func (l *latency) String() string {
	l.Lock()
	defer l.Unlock()
	if l.n == 0 {
		return "--"
	}
	return fmt.Sprintf("[Mean: %v Min: %v Max %v]", l.total/time.Duration(l.n), l.min, l.max)
}

func keysFromFlag() (note.Signer, note.Verifier) {
	sKey, err := note.NewSigner(*signer)
	if err != nil {
		klog.Exitf("Invalid signing key: %v", err)
	}
	vKey, err := note.NewVerifier(*verifier)
	if err != nil {
		klog.Exitf("Invalid verifier key: %v", err)
	}
	return sKey, vKey
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	sKey, vKey := keysFromFlag()
	ct := currentTree(vKey)
	nt := newTree(*path, sKey)

	s := aws.New(ctx, *path, log.Params{EntryBundleSize: *bundleSize}, *batchMaxAge, *batchSize, ct, nt, *bucketName, *sequencedBundleMaxSize, *integrateBundleBatchSize, *sequenceWithLock, *dedupEntriesSeq)
	l := &latency{}

	if _, err := s.ReadCheckpoint(); err != nil {
		klog.Infof("ReadCheckpoint: %v", err)
		if err := s.NewTree(0, []byte("Empty")); err != nil {
			klog.Exitf("Failed to initialise log: %v", err)
		}
		klog.Infof("Initialised checkpoint.")
		if err := s.WriteSequencedIndex(0); err != nil {
			klog.Exitf("failed to initialise sequencer: %v", err)
		}
	}

	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		n := time.Now()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		idx, err := s.Sequence(ctx, b, *dedupEntries)
		if err != nil {
			klog.V(1).Infof("failed to sequence entry: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Failed to sequence entry: %v", err)))
			return
		}
		w.Write([]byte(fmt.Sprintf("%d\n", idx)))
		r.Body.Close()
		l.Add(time.Since(n))
	})

	http.HandleFunc("GET /checkpoint", func(w http.ResponseWriter, r *http.Request) {
		cp, err := s.ReadCheckpoint()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Failed to read checkpoint: %v", err)))
			return
		}
		w.Write(cp)
	})

	http.HandleFunc("GET /tile/{path...}", func(w http.ResponseWriter, r *http.Request) {
		tile, err := s.ReadFile(filepath.Join(*path, r.URL.Path))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Failed to read file: %v", err)))
			return
		}
		w.Write(tile)
	})

	http.HandleFunc("GET /seq/{path...}", func(w http.ResponseWriter, r *http.Request) {
		tile, err := s.ReadFile(filepath.Join(*path, r.URL.Path))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Failed to read file: %v", err)))
			return
		}
		w.Write(tile)
	})

	go printStats(ctx, s, ct, l)
	if err := http.ListenAndServe(*listen, http.DefaultServeMux); err != nil {
		klog.Exitf("ListenAndServe: %v", err)
	}
}

func currentTree(verifier note.Verifier) aws.CurrentTreeFunc {
	return func(cpb []byte) (uint64, []byte, error) {
		cp, _, _, err := f_log.ParseCheckpoint(cpb, verifier.Name(), verifier)
		if err != nil {
			return 0, nil, err
		}
		return cp.Size, cp.Hash, nil
	}
}

func newTree(path string, signer note.Signer) func(size uint64, hash []byte) ([]byte, error) {
	return func(size uint64, hash []byte) ([]byte, error) {
		cp := &f_log.Checkpoint{
			Origin: signer.Name(),
			Size:   size,
			Hash:   hash,
		}
		n, err := note.Sign(&note.Note{Text: string(cp.Marshal())}, signer)
		if err != nil {
			return nil, err
		}
		return n, nil
	}
}

func printStats(ctx context.Context, s *aws.Storage, cpf aws.CurrentTreeFunc, l *latency) {
	interval := time.Second
	var lastSize uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			currCP, err := s.ReadCheckpoint()
			if err != nil {
				klog.Fatalf("Couldn't load checkpoint:  %v", err)
			}
			size, _, err := cpf(currCP)
			if err != nil {
				klog.Errorf("Failed to get checkpoint: %v", err)
				continue
			}
			if lastSize > 0 {
				added := size - lastSize
				klog.Infof("CP size %d (+%d); Latency: %v", size, added, l.String())
			}
			lastSize = size
		}
	}
}
