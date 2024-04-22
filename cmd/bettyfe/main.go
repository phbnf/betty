package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/storage/posix"
	f_log "github.com/transparency-dev/formats/log"
	"golang.org/x/mod/sumdb/note"
	"k8s.io/klog/v2"
)

var (
	leavesPerSecond = flag.Int64("leaves_per_second", 10, "How many leaves to generate per second")
	leafSize        = flag.Int("leaf_size", 1024, "Leaf size in bytes")
	numWriters      = flag.Int("num_writers", 100, "Number of parallel writers")
	path            = flag.String("path", "/tmp/log", "Path to log root diretory")
	batchSize       = flag.Int("batch_size", 1, "Size of batch before flushing")
	batchMaxAge     = flag.Duration("batch_max_age", 100*time.Millisecond, "Max age for batch entries before flushing")

	listen = flag.String("listen", ":2024", "Address:port to listen on")

	signer   = flag.String("log_signer", "PRIVATE+KEY+Test-Betty+df84580a+Afge8kCzBXU7jb3cV2Q363oNXCufJ6u9mjOY1BGRY9E2", "Log signer")
	verifier = flag.String("log_verifier", "Test-Betty+df84580a+AQQASqPUZoIHcJAF5mBOryctwFdTV1E0GRY4kEAtTzwB", "log verifier")
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
	ct := currentTree(*path, vKey)
	nt := newTree(*path, sKey)

	if err := os.MkdirAll(*path, 0o755); err != nil {
		klog.Exitf("failed to make directory structure: %v", err)
	}
	if _, _, err := ct(); err != nil {
		klog.Infof("ct: %v", err)
		if err := nt(0, []byte("Empty")); err != nil {
			klog.Exitf("Failed to initialise log: %v", err)
		}
	}

	s := posix.New(*path, log.Params{EntryBundleSize: *batchSize}, *batchMaxAge, ct, nt)
	l := &latency{}

	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		n := time.Now()
		defer func() { l.Add(time.Since(n)) }()

		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		idx, err := s.Sequence(ctx, b)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Failed to sequence entry: %v", err)))
			return
		}
		w.Write([]byte(fmt.Sprintf("%d\n", idx)))
	})
	fs := http.FileServer(http.Dir(*path))
	http.Handle("GET /", fs)

	go printStats(ctx, ct, l)
	if err := http.ListenAndServe(*listen, http.DefaultServeMux); err != nil {
		klog.Exitf("ListenAndServe: %v", err)
	}
}

func currentTree(path string, verifier note.Verifier) posix.CurrentTreeFunc {
	return func() (uint64, []byte, error) {
		b, err := posix.ReadCheckpoint(path)
		if err != nil {
			return 0, nil, fmt.Errorf("ReadCheckpoint: %v", err)
		}
		cp, _, _, err := f_log.ParseCheckpoint(b, verifier.Name(), verifier)
		if err != nil {
			return 0, nil, err
		}
		return cp.Size, cp.Hash, nil
	}
}

func newTree(path string, signer note.Signer) posix.NewTreeFunc {
	return func(size uint64, hash []byte) error {
		cp := &f_log.Checkpoint{
			Origin: signer.Name(),
			Size:   size,
			Hash:   hash,
		}
		n, err := note.Sign(&note.Note{Text: string(cp.Marshal())}, signer)
		if err != nil {
			return err
		}
		return posix.WriteCheckpoint(path, n)
	}
}

func printStats(ctx context.Context, s posix.CurrentTreeFunc, l *latency) {
	interval := time.Second
	var lastSize uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			size, _, err := s()
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
