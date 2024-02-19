package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/log/writer"
	"github.com/AlCutter/betty/storage/posix"
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

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if err := os.MkdirAll(*path, 0o755); err != nil {
		panic(fmt.Errorf("failed to make directory structure: %w", err))
	}

	s := posix.New(*path, log.Params{EntryBundleSize: *batchSize})
	// Config lib
	w := writer.NewWriter(*batchSize, *batchMaxAge, s.Sequence)

	eg, _ := errgroup.WithContext(context.Background())

	t := time.NewTicker(time.Second)
	for i := 0; i < *numWriters; i++ {
		eg.Go(func() error {
			d := time.Second / time.Duration((*leavesPerSecond/2.0)+rand.Int63n(*leavesPerSecond)/2)
			for {
				time.Sleep(d)
				e := newLeaf()
				// submit leaf
				seq, err := w.Add(e)
				if err != nil {
					klog.Infof("Error adding leaf: %v", err)
				}
				select {
				case <-t.C:
					klog.Infof("Just added to %d", seq)
				default:
				}

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
