package writer

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Batch struct {
	Entries [][]byte
}

// SequenceFunc knows how to assign contiguous sequence numbers to the entries in Batch.
// Returns the sequence number of the first entry, or an error.
// Must not return successfully until the assigned sequence numbers are durably stored.
type SequenceFunc func(context.Context, Batch) (uint64, error)

func NewPool(bufferSize int, maxAge time.Duration, s SequenceFunc) *Pool {
	return &Pool{
		current: &batch{
			Done:   make(chan struct{}),
			Born:   time.Now(),
			Hashes: make(map[[32]byte]uint64),
		},
		bufferSize: bufferSize,
		seq:        s,
		maxAge:     maxAge,
	}
}

// Pool is a helper for adding entries to a log.
type Pool struct {
	sync.Mutex
	current    *batch
	bufferSize int
	maxAge     time.Duration
	flushTimer *time.Timer

	seq SequenceFunc
}

// Add adds an entry to the tree.
// Returns the assigned sequence number, or an error.
func (p *Pool) Add(e []byte) (uint64, error) {
	p.Lock()
	b := p.current
	if len(b.Entries) == 0 {
		p.flushTimer = time.AfterFunc(p.maxAge, func() {
			p.Lock()
			defer p.Unlock()
			p.flushWithLock()
		})
	}
	n := b.Add(e)
	if n >= p.bufferSize || time.Since(p.current.Born) > p.maxAge {
		p.flushWithLock()
	}
	p.Unlock()
	<-b.Done
	return b.FirstSeq + uint64(n), b.Err
}

func (p *Pool) flushWithLock() {
	t := time.Now()
	p.flushTimer.Stop()
	klog.V(1).Infof("took %v to trigger a flush", time.Now().Sub(p.current.Born))
	p.flushTimer = nil
	b := p.current
	p.current = &batch{
		Done:   make(chan struct{}),
		Born:   time.Now(),
		Hashes: make(map[[32]byte]uint64),
	}
	go func() {
		b.FirstSeq, b.Err = p.seq(context.TODO(), Batch{Entries: b.Entries})
		close(b.Done)
	}()
	klog.V(1).Infof("Took %v to flushWithLock()", time.Since(t))
}

type batch struct {
	Born     time.Time
	Entries  [][]byte
	Hashes   map[[32]byte]uint64
	Done     chan struct{}
	FirstSeq uint64
	Err      error
}

func (b *batch) Add(e []byte) int {
	hash := sha256.Sum256(e)
	i, ok := b.Hashes[hash]
	if ok {
		// TODO: not a great int conversion
		return int(i)
	}
	b.Entries = append(b.Entries, e)
	l := len(b.Entries)
	b.Hashes[hash] = uint64(l)
	return l
}
