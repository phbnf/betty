package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/storage/posix"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var (
	leavesPerSecond = flag.Int64("leaves_per_second", 10, "How many leaves to generate per second")
	leafSize        = flag.Int("leaf_size", 1024, "Leaf size in bytes")
	numWriters      = flag.Int("num_writers", 100, "Number of parallel writers")
	path            = flag.String("path", "/tmp/log", "Path to log root diretory")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if err := os.MkdirAll(*path, 0o755); err != nil {
		panic(fmt.Errorf("failed to make directory structure: %w", err))
	}

	s := posix.NewStorage(*path)
	// Config lib
	w := log.NewWriter(10, s.Sequence)
	go s.Integrate(context.Background())

	eg, _ := errgroup.WithContext(context.Background())

	for i := 0; i < *numWriters; i++ {
		eg.Go(func() error {
			d := time.Second / time.Duration(*leavesPerSecond)
			for {
				time.Sleep(d)
				e := newLeaf()
				// submit leaf
				_, err := w.Add(e)
				if err != nil {
					klog.Infof("Error adding leaf: %v", err)
					return err
				}
			}
			return nil
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
