package aws

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/log/writer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/serverless-log/api"
	"github.com/transparency-dev/serverless-log/api/layout"
	"k8s.io/klog/v2"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations.
type Storage struct {
	sync.Mutex
	params log.Params
	path   string
	pool   *writer.Pool

	cpFile *os.File

	curTree CurrentTreeFunc
	newTree NewTreeFunc

	curSize uint64

	bucket string
	s3     s3.Client
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
type NewTreeFunc func(size uint64, root []byte) ([]byte, error)

// CurrentTree is the signature of a function which retrieves the current integrated tree size and root hash.
type CurrentTreeFunc func([]byte) (uint64, []byte, error)

// New creates a new POSIX storage.
func New(path string, params log.Params, batchMaxAge time.Duration, curTree CurrentTreeFunc, newTree NewTreeFunc, bucketName string) *Storage {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	sdkConfig.Region = "us-east-1"
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return nil
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	r := &Storage{
		path:    path,
		params:  params,
		curTree: curTree,
		newTree: newTree,
		bucket:  bucketName,
		s3:      *s3Client,
	}
	r.pool = writer.NewPool(params.EntryBundleSize, batchMaxAge, r.sequenceBatch)

	currCP, err := r.ReadCheckpoint()
	if err != nil {
		klog.Infof("Couldn't load checkpoint:  %v", err)
	}
	curSize, _, err := curTree(currCP)
	if err != nil {
		klog.Infof("Can't get current tree: %v", err)
	}

	r.curSize = curSize

	return r
}

// lockCP places a POSIX advisory lock for the checkpoint.
// Note that a) this is advisory, and b) we use an adjacent file to the checkpoint
// (`checkpoint.lock`) to avoid inherent brittleness of the `fcntrl` API (*any* `Close`
// operation on this file (even if it's a different FD) from this PID, or overwriting
// of the file by *any* process breaks the lock.)
func (s *Storage) lockCP() error {
	return nil
	// var err error
	// if s.cpFile != nil {
	// 	panic("not unlocked")
	// }
	// s.cpFile, err = os.OpenFile(filepath.Join(s.path, layout.CheckpointPath+".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, filePerm)
	// if err != nil {
	// 	return err
	// }

	// flockT := syscall.Flock_t{
	// 	Type:   syscall.F_WRLCK,
	// 	Whence: io.SeekStart,
	// 	Start:  0,
	// 	Len:    0,
	// }
	// for {
	// 	if err := syscall.FcntlFlock(s.cpFile.Fd(), syscall.F_SETLKW, &flockT); err != syscall.EINTR {
	// 		return err
	// 	}
	// }
}

// unlockCP unlocks the `checkpoint.lock` file.
func (s *Storage) unlockCP() error {
	//if s.cpFile == nil {
	//	panic(errors.New("not locked"))
	//}
	//if err := s.cpFile.Close(); err != nil {
	//	return err
	//}
	//s.cpFile = nil
	return nil
}

// Sequence commits to sequence numbers for an entry
// Returns the sequence number assigned to the first entry in the batch, or an error.
func (s *Storage) Sequence(ctx context.Context, b []byte) (uint64, error) {
	return s.pool.Add(b)
}

// GetEntryBundle retrieves the Nth entries bundle.
// If size is != the max size of the bundle, a partial bundle is returned.
func (s *Storage) GetEntryBundle(ctx context.Context, index, size uint64) ([]byte, error) {
	bd, bf := layout.SeqPath(s.path, index)
	if size < uint64(s.params.EntryBundleSize) {
		bf = fmt.Sprintf("%s.%d", bf, size)
	}
	return os.ReadFile(filepath.Join(bd, bf))
}

// sequenceBatch writes the entries from the provided batch into the entry bundle files of the log.
//
// This func starts filling entries bundles at the next available slot in the log, ensuring that the
// sequenced entries are contiguous from the zeroth entry (i.e left-hand dense).
// We try to minimise the number of partially complete entry bundles by writing entries in chunks rather
// than one-by-one.
func (s *Storage) sequenceBatch(ctx context.Context, batch writer.Batch) (uint64, error) {
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The POSIX `LockCP()` ensures that distinct tasks are serialised.
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

	currCP, err := s.ReadCheckpoint()
	if err != nil {
		klog.Fatalf("Couldn't load checkpoint:  %v", err)
	}
	size, _, err := s.curTree(currCP)

	if err != nil {
		return 0, err
	}
	s.curSize = size

	if len(batch.Entries) == 0 {
		return 0, nil
	}
	seq := s.curSize
	bundleIndex, entriesInBundle := seq/uint64(s.params.EntryBundleSize), seq%uint64(s.params.EntryBundleSize)
	bundle := &bytes.Buffer{}
	if entriesInBundle > 0 {
		// If the latest bundle is partial, we need to read the data it contains in for our newer, larger, bundle.
		part, err := s.GetEntryBundle(ctx, bundleIndex, entriesInBundle)
		if err != nil {
			return 0, err
		}
		bundle.Write(part)
	}
	// Add new entries to the bundle
	for _, e := range batch.Entries {
		bundle.WriteString(base64.StdEncoding.EncodeToString(e))
		bundle.WriteString("\n")
		entriesInBundle++
		if entriesInBundle == uint64(s.params.EntryBundleSize) {
			//  This bundle is full, so we need to write it out...
			bd, bf := layout.SeqPath(s.path, bundleIndex)
			if err := os.MkdirAll(bd, dirPerm); err != nil {
				return 0, fmt.Errorf("failed to make seq directory structure: %w", err)
			}
			if err := createExclusive(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
				if !errors.Is(os.ErrExist, err) {
					return 0, err
				}
			}
			// ... and prepare the next entry bundle for any remaining entries in the batch
			bundleIndex++
			entriesInBundle = 0
			bundle = &bytes.Buffer{}
		}
	}
	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		bd, bf := layout.SeqPath(s.path, bundleIndex)
		bf = fmt.Sprintf("%s.%d", bf, entriesInBundle)
		if err := os.MkdirAll(bd, dirPerm); err != nil {
			return 0, fmt.Errorf("failed to make seq directory structure: %w", err)
		}
		if err := createExclusive(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
			if !errors.Is(os.ErrExist, err) {
				return 0, err
			}
		}
	}

	// For simplicitly, well in-line the integration of these new entries into the Merkle structure too.
	return seq, s.doIntegrate(ctx, seq, batch.Entries)
}

// doIntegrate handles integrating new entries into the log, and updating the checkpoint.
func (s *Storage) doIntegrate(ctx context.Context, from uint64, batch [][]byte) error {
	newSize, newRoot, err := writer.Integrate(ctx, from, batch, s, rfc6962.DefaultHasher)
	if err != nil {
		klog.Errorf("Failed to integrate: %v", err)
		return err
	}
	if err := s.NewTree(newSize, newRoot); err != nil {
		return fmt.Errorf("newTree: %v", err)
	}
	return nil
}

// GetTile returns the tile at the given tile-level and tile-index.
// If no complete tile exists at that location, it will attempt to find a
// partial tile for the given tree size at that location.
func (s *Storage) GetTile(_ context.Context, level, index, logSize uint64) (*api.Tile, error) {
	tileSize := layout.PartialTileSize(level, index, logSize)
	p := filepath.Join(layout.TilePath(s.path, level, index, tileSize))
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

	tDir, tFile := layout.TilePath("", level, index, tileSize%256)
	tPath := filepath.Join(tDir, tFile)

	if err := s.WriteFile(tPath, t); err != nil {
		return err
	}

	return nil
}

// WriteCheckpoint stores a raw log checkpoint on disk.
func (s *Storage) WriteCheckpoint(newCPRaw []byte) error {
	if err := s.WriteFile(layout.CheckpointPath, newCPRaw); err != nil {
		klog.Infof("Couldn't write checkpoint: %v", err)
	}
	return nil
}

// Readcheckpoint returns the latest stored checkpoint.
func (s *Storage) ReadCheckpoint() ([]byte, error) {
	path := filepath.Join(s.path, layout.CheckpointPath)
	result, err := s.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		klog.Infof("Couldn't get object %v:%v. Here's why: %v\n", s.bucket, path, err)
		return nil, err
	}
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		klog.Infof("Couldn't read object body from %v. Here's why: %v\n", path, err)
		return nil, err
	}
	return body, nil
}

func (s *Storage) NewTree(size uint64, hash []byte) error {
	cpRaw, err := s.newTree(size, hash)
	if err != nil {
		klog.Infof("could not create new tree: %v", err)
	}
	return s.WriteCheckpoint(cpRaw)
}

// createExclusive creates a file at the given path and name before writing the data in d to it.
// It will error if the file already exists, or it's unable to fully write the
// data & close the file.
func createExclusive(f string, d []byte) error {
	tmpFile, err := os.CreateTemp(filepath.Dir(f), "")
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

// WriteCheckpoint stores a raw log checkpoint on disk.
func (s *Storage) WriteFile(path string, data []byte) error {
	fullPath := filepath.Join(s.path, path)
	_, err := s.s3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		klog.Infof("Couldn't write data at path %s: %v", path, err)
	}
	return nil
}
