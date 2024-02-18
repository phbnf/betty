package log

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
)

var (
	ErrSeqAlreadyAssigned = errors.New("already assigned")
)

type Batch struct {
	Entries [][]byte
}

// SequenceFunc knows how to assign contiguous sequence numbers to the entries in Batch.
// Returns the sequence number of the first entry, or an error.
// Must not return successfully until the assigned sequence numbers are durably stored.
type SequenceFunc func(context.Context, Batch) (uint64, error)

func NewWriter(bufferSize int, maxAge time.Duration, s SequenceFunc) *Writer {
	rf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	return &Writer{
		rf: rf,
		current: &pool{
			Done:   make(chan struct{}),
			cRange: rf.NewEmptyRange(0),
			Born:   time.Now(),
		},
		bufferSize: bufferSize,
		seq:        s,
		maxAge:     maxAge,
	}
}

// Writer is a helper for adding entries to a log.
type Writer struct {
	sync.Mutex
	rf         compact.RangeFactory
	current    *pool
	bufferSize int
	maxAge     time.Duration
	flushTimer *time.Timer

	seq SequenceFunc
}

// Add adds an entry to the tree.
// Returns the assigned sequence number, or an error.
func (w *Writer) Add(e []byte) (uint64, error) {
	w.Lock()
	b := w.current
	if len(b.Entries) == 0 {
		w.flushTimer = time.AfterFunc(w.maxAge, func() {
			w.Lock()
			defer w.Unlock()
			w.flushWithLock()
		})
	}
	n := b.Add(e)
	if n >= w.bufferSize || time.Since(w.current.Born) > w.maxAge {
		w.flushWithLock()
	}
	w.Unlock()
	<-b.Done
	return b.FirstSeq + uint64(n), b.Err
}

func (w *Writer) flushWithLock() {
	w.flushTimer.Stop()
	w.flushTimer = nil
	b := w.current
	w.current = &pool{
		Done:   make(chan struct{}),
		cRange: w.rf.NewEmptyRange(0),
		Born:   time.Now(),
	}
	go func() {
		b.FirstSeq, b.Err = w.seq(context.TODO(), Batch{Entries: b.Entries})
		close(b.Done)
	}()
}

type pool struct {
	Born     time.Time
	Entries  [][]byte
	Done     chan struct{}
	FirstSeq uint64
	Err      error

	cRange *compact.Range
}

func (b *pool) Add(e []byte) int {
	b.Entries = append(b.Entries, e)
	return len(b.Entries)
}
