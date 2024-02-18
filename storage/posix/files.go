package posix

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/serverless-log/api"
	"github.com/transparency-dev/serverless-log/api/layout"
	"k8s.io/klog/v2"

	f_log "github.com/transparency-dev/formats/log"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations.
//
// TODO: doc the batch sequence stuff.
type Storage struct {
	sync.Mutex
	Params       log.Params
	Path         string
	nextBatch    uint64
	nextSeq      uint64
	integratedTo uint64

	work chan uint64
}

func NewStorage(path string) *Storage {
	nextSeq := uint64(0)
	cpRaw, err := ReadCheckpoint(path)
	if err == nil {
		var cp f_log.Checkpoint
		if err := json.Unmarshal(cpRaw, &cp); err != nil {
			klog.Errorf("failed to unmarshal checkpoint")
		} else {
			nextSeq = cp.Size
		}
	}
	return &Storage{
		Path:    path,
		nextSeq: nextSeq,
		work:    make(chan uint64, 10),
	}
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

// preSeqPath returns the directory and path for a given intermediate sequence file
func (s *Storage) preSeqPath(n uint64) (string, string) {
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
//     The file is "named" as the sequence number assigned to the first entry. Subsequent files must be named prev.Seq+prev.N
func (s *Storage) Sequence(ctx context.Context, b log.Batch) (uint64, error) {
	s.Lock()
	defer s.Unlock()
	batch, err := s.assignBatch(ctx, b)
	if err != nil {
		return 0, err
	}
	seq, err := s.sequenceBatch(ctx, batch, len(b.Entries))
	if err != nil {
		return 0, err
	}
	s.work <- seq + uint64(len(b.Entries)) - 1
	return seq, nil
}

func (s *Storage) assignBatch(ctx context.Context, b log.Batch) (uint64, error) {
	// 2. Write temp file
	// 3. Hard link temp -> seq file

	f, err := os.CreateTemp(s.Path, "seq")
	if err != nil {
		return 0, err
	}
	if err := gob.NewEncoder(f).Encode(b); err != nil {
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
		batchPath := filepath.Join(batchDir, batchFile)
		if err := os.Link(tmp, batchPath); errors.Is(err, os.ErrExist) {
			// That batch number is in use, try the next one
			s.nextBatch++
			continue
		} else if err != nil {
			return 0, fmt.Errorf("failed to link batch file: %w", err)
		}
		klog.V(1).Infof("Created batch %d", batchSeq)
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
		klog.V(2).Infof("SB: try %d", seq)
		// Ensure the sequence directory structure is present:
		seqDir, seqFile := s.preSeqPath(seq)
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
		defer func() {
			_ = os.Remove(tmp)
		}()

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

		klog.V(1).Infof("Created batch %d (%d entries) assigned to log sequence %d", preSeq.BatchID, preSeq.N, seq)
		s.nextSeq += preSeq.N

		return seq, nil
	}
}

func readBatch(p string) (*log.Batch, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	batch := &log.Batch{}
	return batch, gob.NewDecoder(f).Decode(batch)
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.work:
		case <-time.After(time.Second):
		}

		curCP := &f_log.Checkpoint{}
		curRaw, err := ReadCheckpoint(s.Path)
		if err != nil {
			if !errors.Is(os.ErrNotExist, err) {
				// Might not exist yet, TODO remove this edgecase
				klog.Errorf("Failed to read current checkpoint: %v", err)
			}
		} else {
			if err := json.Unmarshal(curRaw, curCP); err != nil {
				klog.Errorf("failed to unmarshal checkpoint")
			}
		}

		b, bID, err := s.ReadBatch(ctx, curCP.Size)
		if err != nil {
			klog.V(2).Infof("Failed to read preSeq batch at %d - already processed?", curCP.Size)
			continue
		}
		newCP, err := doIntegrate(ctx, curCP.Size, b, s, rfc6962.DefaultHasher)
		if err != nil {
			klog.Errorf("Failed to integrate: %v", err)
			continue
		}
		klog.Infof("NewCP: %d (%x)", newCP.Size, newCP.Hash)
		newCPRaw, err := json.Marshal(newCP)
		if err != nil {
			klog.Errorf("Failed to marshall new checkpoint: %v", err)
		}
		if err := s.WriteCheckpoint(ctx, newCPRaw); err != nil {
			klog.Errorf("Failed to store new checkpoint: %v", err)
		}
		if err := s.RemoveBatch(ctx, curCP.Size, bID); err != nil {
			klog.Errorf("Failed to remove temporary batch data: %v", err)
		}
	}
}

func (s *Storage) ReadBatch(ctx context.Context, start uint64) ([][]byte, uint64, error) {
	dir, file := s.preSeqPath(start)
	preSeq, err := readPreSeq(filepath.Join(dir, file))
	if err != nil {
		return nil, 0, err
	}
	bDir, bFile := s.batchSeqPath(preSeq.BatchID)
	b, err := readBatch(filepath.Join(bDir, bFile))
	if err != nil {
		return nil, 0, err
	}
	return b.Entries, preSeq.BatchID, nil
}

func (s *Storage) RemoveBatch(ctx context.Context, start uint64, batchID uint64) error {
	dir, file := s.preSeqPath(start)
	bDir, bFile := s.batchSeqPath(batchID)
	return errors.Join(
		os.Remove(filepath.Join(dir, file)),
		os.Remove(filepath.Join(bDir, bFile)))
}

// createExclusive creates the named file before writing the data in d to it.
// It will error if the file already exists, or it's unable to fully write the
// data & close the file.
func createExclusive(f string, d []byte) error {
	tmpFile, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePerm)
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

// GetTile returns the tile at the given tile-level and tile-index.
// If no complete tile exists at that location, it will attempt to find a
// partial tile for the given tree size at that location.
func (s *Storage) GetTile(_ context.Context, level, index, logSize uint64) (*api.Tile, error) {
	tileSize := layout.PartialTileSize(level, index, logSize)
	p := filepath.Join(layout.TilePath(s.Path, level, index, tileSize))
	t, err := os.ReadFile(p)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to read tile at %q: %w", p, err)
		}
		return nil, err
	}

	var tile api.Tile
	if err := tile.UnmarshalText(t); err != nil {
		return nil, fmt.Errorf("failed to parse tile: %w", err)
	}
	return &tile, nil
}

// StoreTile writes a tile out to disk.
// Fully populated tiles are stored at the path corresponding to the level &
// index parameters, partially populated (i.e. right-hand edge) tiles are
// stored with a .xx suffix where xx is the number of "tile leaves" in hex.
func (s *Storage) StoreTile(_ context.Context, level, index uint64, tile *api.Tile) error {
	tileSize := uint64(tile.NumLeaves)
	klog.V(2).Infof("StoreTile: level %d index %x ts: %x", level, index, tileSize)
	if tileSize == 0 || tileSize > 256 {
		return fmt.Errorf("tileSize %d must be > 0 and <= 256", tileSize)
	}
	t, err := tile.MarshalText()
	if err != nil {
		return fmt.Errorf("failed to marshal tile: %w", err)
	}

	tDir, tFile := layout.TilePath(s.Path, level, index, tileSize%256)
	tPath := filepath.Join(tDir, tFile)

	if err := os.MkdirAll(tDir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", tDir, err)
	}

	// TODO(al): use unlinked temp file
	temp := fmt.Sprintf("%s.temp", tPath)
	if err := os.WriteFile(temp, t, filePerm); err != nil {
		return fmt.Errorf("failed to write temporary tile file: %w", err)
	}
	if err := os.Rename(temp, tPath); err != nil {
		return fmt.Errorf("failed to rename temporary tile file: %w", err)
	}

	if tileSize == 256 {
		partials, err := filepath.Glob(fmt.Sprintf("%s.*", tPath))
		if err != nil {
			return fmt.Errorf("failed to list partial tiles for clean up; %w", err)
		}
		// Clean up old partial tiles by symlinking them to the new full tile.
		for _, p := range partials {
			klog.V(2).Infof("relink partial %s to %s", p, tPath)
			// We have to do a little dance here to get POSIX atomicity:
			// 1. Create a new temporary symlink to the full tile
			// 2. Rename the temporary symlink over the top of the old partial tile
			tmp := fmt.Sprintf("%s.link", tPath)
			if err := os.Symlink(tPath, tmp); err != nil {
				return fmt.Errorf("failed to create temp link to full tile: %w", err)
			}
			if err := os.Rename(tmp, p); err != nil {
				return fmt.Errorf("failed to rename temp link over partial tile: %w", err)
			}
		}
	}

	return nil
}

// WriteCheckpoint stores a raw log checkpoint on disk.
func (s *Storage) WriteCheckpoint(_ context.Context, newCPRaw []byte) error {
	oPath := filepath.Join(s.Path, layout.CheckpointPath)
	tmp := fmt.Sprintf("%s.tmp", oPath)
	if err := createExclusive(tmp, newCPRaw); err != nil {
		return fmt.Errorf("failed to create temporary checkpoint file: %w", err)
	}
	return os.Rename(tmp, oPath)
}

// ReadCheckpoint reads and returns the contents of the log checkpoint file.
func ReadCheckpoint(rootDir string) ([]byte, error) {
	s := filepath.Join(rootDir, layout.CheckpointPath)
	return os.ReadFile(s)
}
