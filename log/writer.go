package log

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
)

var (
	ErrSeqAlreadyAssigned = errors.New("already assigned")
)

// SequenceFunc knows how to assign contiguous sequence numbers to the entries in Batch.
// Returns the sequence number of the first entry, or an error.
// Must not return successfully until the assigned sequence numbers are durably stored.
type SequenceFunc func(context.Context, [][]byte) (uint64, error)

func NewWriter(bufferSize int, s SequenceFunc) *Writer {
	rf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	return &Writer{
		rf: rf,
		current: &Batch{
			Done: make(chan struct{}),
		},
		bufferSize: bufferSize,
		seq:        s,
	}
}

// Writer is a helper for adding entries to a log.
type Writer struct {
	sync.Mutex
	rf         compact.RangeFactory
	current    *Batch
	bufferSize int

	seq SequenceFunc
}

// Add adds an entry to the tree.
// Returns the assigned sequence number, or an error.
func (w *Writer) Add(e []byte) (uint64, error) {
	w.Lock()
	b := w.current
	n := b.Add(e)
	if n >= w.bufferSize {
		w.current = &Batch{Done: make(chan struct{})}
		go func() {
			fmt.Printf("Writing batch")
			b.FirstSeq, b.Err = w.seq(context.TODO(), b.Entries)
			close(b.Done)
		}()
	}
	w.Unlock()
	<-b.Done
	return b.FirstSeq + uint64(n), b.Err
}

type Batch struct {
	Entries  [][]byte
	Done     chan struct{}
	FirstSeq uint64
	Err      error
}

func (b *Batch) Add(e []byte) int {
	b.Entries = append(b.Entries, e)
	return len(b.Entries)
}
