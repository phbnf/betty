package posix

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/AlCutter/betty/log"
	"k8s.io/klog/v2"
)

const (
	dirPerm = 0o755
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations.
//
// TODO: doc the batch sequence stuff.
type Storage struct {
	sync.Mutex
	Params    log.Params
	Path      string
	nextBatch uint64
	nextSeq   uint64

	work chan uint64
}

func NewStorage(path string) *Storage {
	return &Storage{
		Path: path,
		work: make(chan uint64, 10),
	}
}

type qBatch struct {
	Entries [][]byte
}

type preSeq struct {
	N       uint64
	BatchID uint64
}

// batchSeqPath returns the directory and path for a given batch sequence number.
func (s *Storage) batchSeqPath(bs uint64) (string, string) {
	x := fmt.Sprintf("%016x", bs)
	return filepath.Join(s.Path, "batches", x[0:2], x[2:4], x[4:6], x[6:8], x[8:10], x[10:12], x[12:14]), x[14:16]
}

// seqPath returns the directory and path for a given intermediate sequence file
func (s *Storage) seqPath(n uint64) (string, string) {
	x := fmt.Sprintf("%016x", n)
	return filepath.Join(s.Path, "preseq", x[0:2], x[2:4], x[4:6], x[6:8], x[8:10], x[10:12], x[12:14]), x[14:16]
}

// Sequence commits to sequence numbers for batches of entries.
// Returns the sequence number assigned to the first entry in the batch, or an error.
//
//  1. atomically writes the batch to a BatchSequence file. This may be "large".
//
//  2. atomically writes a pre-integrate file which binds entries in a BatchSequence file to their place in the log.
//     The file contains:
//     - BatchSequenceID - the number assigned to the batch in (1)
//     - N - the number of entries in the batch.
//
//     The file is "named" as Seq. Subsequent files must be named prev.Seq+prev.N
func (s *Storage) Sequence(ctx context.Context, b [][]byte) (uint64, error) {
	batch, err := s.assignBatch(ctx, b)
	if err != nil {
		return 0, err
	}
	return s.sequenceBatch(ctx, batch, len(b))
}

func (s *Storage) assignBatch(ctx context.Context, b [][]byte) (uint64, error) {
	// 2. Write temp file
	// 3. Hard link temp -> seq file

	f, err := os.CreateTemp(s.Path, "seq")
	if err != nil {
		return 0, err
	}
	qBatch := qBatch{Entries: b}
	if err := gob.NewEncoder(f).Encode(qBatch); err != nil {
		return 0, err
	}
	tmp, err := f.Name(), f.Close()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = os.Remove(tmp)
	}()

	// Now try to sequence it, we may have to scan over some new batch enties
	// if Sequence has been called recently.
	for {
		batchSeq := s.nextBatch
		// Ensure the batch directory structure is present:
		batchDir, batchFile := s.batchSeqPath(batchSeq)
		if err := os.MkdirAll(batchDir, dirPerm); err != nil {
			return 0, fmt.Errorf("failed to make batch directory structure: %w", err)
		}

		// Hardlink the batch sequence file to the temporary file
		seqPath := filepath.Join(batchDir, batchFile)
		if err := os.Link(tmp, seqPath); errors.Is(err, os.ErrExist) {
			// That batch number is in use, try the next one
			s.nextBatch++
			continue
		} else if err != nil {
			return 0, fmt.Errorf("failed to link batch file: %w", err)
		}
		klog.Infof("Created batch %d", batchSeq)
		return batchSeq, nil
	}
}

func (s *Storage) sequenceBatch(ctx context.Context, batch uint64, batchSize int) (uint64, error) {
	preSeq := preSeq{N: uint64(batchSize), BatchID: batch}
	b := &bytes.Buffer{}
	if err := gob.NewEncoder(b).Encode(preSeq); err != nil {
		return 0, err
	}

	for {
		seq := s.nextSeq
		klog.Infof("SB: try %d", seq)
		// Ensure the sequence directory structure is present:
		seqDir, seqFile := s.seqPath(seq)
		if err := os.MkdirAll(seqDir, dirPerm); err != nil {
			return 0, fmt.Errorf("failed to make preseq directory structure: %w", err)
		}

		tmp := filepath.Join(s.Path, fmt.Sprintf("preSeq-%d", seq))
		if err := createExclusive(tmp, b.Bytes()); errors.Is(err, os.ErrExist) {
			preSeq, err := readPreSeq(tmp)
			if err != nil {
				return 0, err
			}
			s.nextSeq += preSeq.N
			continue
		} else if err != nil {
			return 0, err
		}

		// Hardlink the pre sequence file to the temporary file
		seqPath := filepath.Join(seqDir, seqFile)
		if err := os.Link(tmp, seqPath); errors.Is(err, os.ErrExist) {
			// That sequence number is in use, try the next one
			preSeq, err := readPreSeq(tmp)
			if err != nil {
				return 0, err
			}
			s.nextSeq += preSeq.N
			continue
		} else if err != nil {
			return 0, fmt.Errorf("failed to link batch file: %w", err)
		}

		klog.Infof("Created batch %d (%d entries) assigned to index %d", preSeq.BatchID, preSeq.N, seq)
		s.nextSeq += preSeq.N
		s.work <- seq

		return seq, nil
	}
}

func readPreSeq(p string) (*preSeq, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	preSeq := &preSeq{}
	return preSeq, gob.NewDecoder(f).Decode(preSeq)
}

func (s *Storage) Integrate(ctx context.Context) error {
	var batchN uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batchN = <-s.work:
		}

		klog.Infof("Would try to integrate %d", batchN)
	}
}

// createExclusive creates the named file before writing the data in d to it.
// It will error if the file already exists, or it's unable to fully write the
// data & close the file.
func createExclusive(f string, d []byte) error {
	tmpFile, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("unable to create temporary file: %w", err)
	}
	n, err := tmpFile.Write(d)
	if err != nil {
		return fmt.Errorf("unable to write leafdata to temporary file: %w", err)
	}
	if got, want := n, len(d); got != want {
		return fmt.Errorf("short write on leaf, wrote %d expected %d", got, want)
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return nil
}
