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
	return fmt.Sprintf("[Mean: %v Min: %v Max %v]", l.total/time.Duration(l.n), l.min, l.max)
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if err := os.MkdirAll(*path, 0o755); err != nil {
		panic(fmt.Errorf("failed to make directory structure: %w", err))
	}
	ctx := context.Background()

	s := posix.New(*path, log.Params{EntryBundleSize: *batchSize}, *batchMaxAge)
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

	go printStats(ctx, s, l)
	if err := http.ListenAndServe(*listen, http.DefaultServeMux); err != nil {
		klog.Exitf("ListenAndServe: %v", err)
	}
}

func printStats(ctx context.Context, s *posix.Storage, l *latency) {
	interval := time.Second
	var lastCP *f_log.Checkpoint
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cp, err := s.ReadCheckpoint()
			if err != nil {
				klog.Errorf("Failed to get checkpoint: %v", err)
				continue
			}
			if lastCP != nil {
				added := cp.Size - lastCP.Size
				klog.Infof("CP size %d (+%d); Latency: %v", cp.Size, added, l.String())
			}
			lastCP = cp

		}
	}
}
