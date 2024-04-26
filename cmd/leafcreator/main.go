package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/storage/azure"
	f_log "github.com/transparency-dev/formats/log"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var (
	leavesPerSecond = flag.Int64("leaves_per_second", 10, "How many leaves to generate per second")
	leafSize        = flag.Int("leaf_size", 1024, "Leaf size in bytes")
	numWriters      = flag.Int("num_writers", 100, "Number of parallel writers")
	path            = flag.String("path", "/tmp/log", "Path to log root diretory")
	batchSize       = flag.Int("batch_size", 1, "Size of batch before flushing")
	batchMaxAge     = flag.Duration("batch_max_age", 100*time.Millisecond, "Max age for batch entries before flushing")
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

	s := azure.New(*path, log.Params{EntryBundleSize: *batchSize}, *batchMaxAge)

	l := &latency{}

	go printStats(ctx, s, l)
	// Config lib

	eg, _ := errgroup.WithContext(ctx)

	for i := 0; i < *numWriters; i++ {
		eg.Go(func() error {
			d := time.Second / time.Duration((*leavesPerSecond/2.0)+rand.Int63n(*leavesPerSecond)/2)
			for {
				time.Sleep(d)
				e := newLeaf()
				// submit leaf
				n := time.Now()
				_, err := s.Sequence(ctx, e)
				if err != nil {
					klog.Infof("Error adding leaf: %v", err)
					continue
				}
				l.Add(time.Since(n))
			}
		})
	}
	eg.Wait()
}

func newLeaf() []byte {
	r := make([]byte, *leafSize)
	if _, err := rand.Read(r); err != nil {
		panic(err)
	}
	return r
}

func printStats(ctx context.Context, s *azure.Storage, l *latency) {
	interval := time.Second
	var lastCP *f_log.Checkpoint
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cp, err := s.ReadCheckpoint(ctx)
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
