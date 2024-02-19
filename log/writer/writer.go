package writer

import (
	"context"
	"sync"
	"time"
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
			Done: make(chan struct{}),
			Born: time.Now(),
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
	p.flushTimer.Stop()
	p.flushTimer = nil
	b := p.current
	p.current = &batch{
		Done: make(chan struct{}),
		Born: time.Now(),
	}
	go func() {
		b.FirstSeq, b.Err = p.seq(context.TODO(), Batch{Entries: b.Entries})
		close(b.Done)
	}()
}

type batch struct {
	Born     time.Time
	Entries  [][]byte
	Done     chan struct{}
	FirstSeq uint64
	Err      error
}

func (b *batch) Add(e []byte) int {
	b.Entries = append(b.Entries, e)
	return len(b.Entries)
}
