package posix

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

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
type Storage struct {
	sync.Mutex
	Params log.Params
	Path   string

	cpFile   *os.File
	latestCP f_log.Checkpoint
}

func NewStorage(path string, params log.Params) *Storage {
	r := &Storage{
		Path:   path,
		Params: params,
	}
	r.lockCP()
	defer r.unlockCP()
	if err := r.readCheckpoint(); err != nil {
		if !errors.Is(os.ErrNotExist, err) {
			panic(err)
		}
	}
	return r
}

func (s *Storage) lockCP() error {
	if s.cpFile != nil {
		panic(errors.New("already locked"))
	}

	var err error
	s.cpFile, err = os.OpenFile(filepath.Join(s.Path, layout.CheckpointPath+".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, filePerm)
	if err != nil {
		return err
	}

	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
	}
	for syscall.FcntlFlock(s.cpFile.Fd(), syscall.F_SETLKW, &flockT) == syscall.EINTR {
	}
	return nil
}

func (s *Storage) unlockCP() error {
	if s.cpFile == nil {
		panic(errors.New("not locked"))
	}
	f := s.cpFile
	s.cpFile = nil
	return f.Close()
}

// Sequence commits to sequence numbers for batches of entries.
// Returns the sequence number assigned to the first entry in the batch, or an error.
func (s *Storage) Sequence(ctx context.Context, b log.Batch) (uint64, error) {
	s.Lock()
	if err := s.lockCP(); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockCP(); err != nil {
			panic(err)
		}
		s.Unlock()
	}()

	if err := s.readCheckpoint(); err != nil {
		return 0, err
	}

	seq, err := s.sequenceBatch(ctx, b)
	if err != nil {
		return 0, err
	}

	return seq, nil
}

func (s *Storage) GetEntryBundle(ctx context.Context, index, size uint64) ([]byte, error) {
	bd, bf := layout.SeqPath(s.Path, index)
	if size < uint64(s.Params.EntryBundleSize) {
		bf = fmt.Sprintf("%s.%d", bf, size)
	}
	return os.ReadFile(filepath.Join(bd, bf))
}

func (s *Storage) sequenceBatch(ctx context.Context, batch log.Batch) (uint64, error) {
	seq := s.latestCP.Size
	bundleIndex, entriesInBundle := seq/uint64(s.Params.EntryBundleSize), seq%uint64(s.Params.EntryBundleSize)
	bundle := &bytes.Buffer{}
	if entriesInBundle > 0 {
		part, err := s.GetEntryBundle(ctx, bundleIndex, entriesInBundle)
		if err != nil {
			return 0, err
		}
		bundle.Write(part)
	}
	for _, e := range batch.Entries {
		bundle.WriteString(base64.StdEncoding.EncodeToString(e))
		bundle.WriteString("\n")
		entriesInBundle++
		if entriesInBundle == uint64(s.Params.EntryBundleSize) {
			bd, bf := layout.SeqPath(s.Path, bundleIndex)
			if err := os.MkdirAll(bd, dirPerm); err != nil {
				return 0, fmt.Errorf("failed to make seq directory structure: %w", err)
			}
			if err := s.createExclusive(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
				if !errors.Is(os.ErrExist, err) {
					return 0, err
				}
			}
			bundleIndex++
			entriesInBundle = 0
			bundle = &bytes.Buffer{}
		}
	}
	if entriesInBundle > 0 {
		bd, bf := layout.SeqPath(s.Path, bundleIndex)
		bf = fmt.Sprintf("%s.%d", bf, entriesInBundle)
		if err := os.MkdirAll(bd, dirPerm); err != nil {
			return 0, fmt.Errorf("failed to make seq directory structure: %w", err)
		}
		if err := s.createExclusive(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
			if !errors.Is(os.ErrExist, err) {
				return 0, err
			}
		}
	}

	return seq, s.Integrate(ctx, seq, batch.Entries)
}

func (s *Storage) Integrate(ctx context.Context, from uint64, batch [][]byte) error {
	newCP, err := doIntegrate(ctx, from, batch, s, rfc6962.DefaultHasher)
	if err != nil {
		klog.Errorf("Failed to integrate: %v", err)
		return err
	}
	klog.Infof("NewCP: %d (%x)", newCP.Size, newCP.Hash)
	newCPRaw, err := json.Marshal(newCP)
	if err != nil {
		klog.Errorf("Failed to marshall new checkpoint: %v", err)
		return err
	}
	if err := s.WriteCheckpoint(ctx, newCPRaw); err != nil {
		klog.Errorf("Failed to store new checkpoint: %v", err)
		return err
	}
	return nil
}

// createExclusive creates the named file before writing the data in d to it.
// It will error if the file already exists, or it's unable to fully write the
// data & close the file.
func (s *Storage) createExclusive(f string, d []byte) error {
	tmpFile, err := os.CreateTemp(s.Path, "")
	if err != nil {
		return fmt.Errorf("unable to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
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
	if err := os.Rename(tmpName, f); err != nil {
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
		if !errors.Is(os.ErrExist, err) {
			return fmt.Errorf("failed to rename temporary tile file: %w", err)
		}
		os.Remove(temp)
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
			_ = os.Remove(tmp)
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
	if err := s.createExclusive(oPath, newCPRaw); err != nil {
		return fmt.Errorf("failed to create checkpoint file: %w", err)
	}
	return nil
}

func (s *Storage) readCheckpoint() error {
	b, err := os.ReadFile(filepath.Join(s.Path, layout.CheckpointPath))
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil
		}
		return err
	}
	if len(b) == 0 {
		// uninitialised log
		return nil
	}
	cp := &f_log.Checkpoint{}
	if err := json.Unmarshal(b, cp); err != nil {
		return err
	}
	s.latestCP = *cp
	return nil
}

/*
// ReadCheckpoint reads and returns the contents of the log checkpoint file.
func ReadCheckpoint(rootDir string) ([]byte, error) {
	s := filepath.Join(rootDir, layout.CheckpointPath)
	return os.ReadFile(s)
}
*/
