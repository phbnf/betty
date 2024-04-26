package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/log/writer"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/serverless-log/api"
	"github.com/transparency-dev/serverless-log/api/layout"
	"golang.org/x/mod/sumdb/note"
	"k8s.io/klog/v2"

	f_log "github.com/transparency-dev/formats/log"
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations.
type Storage struct {
	sync.Mutex
	pool   *writer.Pool
	params log.Params
	cp     string

	signer   note.Signer
	verifier note.Verifier

	client        *container.Client
	cpClient      *blockblob.Client
	lease         *lease.BlobClient
	leaseID       *string
	containerName string
	blobName      string
	path          string

	curSize uint64
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
// type NewTreeFunc func(size uint64, root []byte) error

// CurrentTree is the signature of a function which retrieves the current integrated tree size and root hash.
// type CurrentTreeFunc func() (uint64, []byte, error)

// New creates a new POSIX storage.
func New(ctx context.Context, tpath string, params log.Params, batchMaxAge time.Duration, signer note.Signer, verifier note.Verifier, accountName, accountKey string) *Storage {
	r := &Storage{
		path:     tpath,
		params:   params,
		signer:   signer,
		verifier: verifier,
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		klog.Exitf("can't create credential: %v", err.Error())
	}

	client, err := container.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), credential, nil)
	if err != nil {
		klog.Exitf("can't create container client: %v", err.Error())
	}
	r.client = client
	cpClient := client.NewBlockBlobClient(path.Join(r.path, layout.CheckpointPath))
	r.cpClient = cpClient
	blobClient, err := lease.NewBlobClient[blockblob.Client](cpClient, nil)
	if err != nil {
		klog.Exitf("can't create lease client: %v", err.Error())
	}
	r.lease = blobClient
	r.lockCP(ctx)
	if err := r.NewTree(ctx, 0, []byte("Empty"), false); err != nil {
		panic(err)
	}
	curSize, _, err := r.CurTree(ctx)
	if err != nil {
		panic(err)
	}
	r.unlockCP(ctx)
	r.curSize = curSize
	r.pool = writer.NewPool(params.EntryBundleSize, batchMaxAge, r.sequenceBatch)

	return r
}

// lockCP places a POSIX advisory lock for the checkpoint.
// Note that a) this is advisory, and b) we use an adjacent file to the checkpoint
// (`checkpoint.lock`) to avoid inherent brittleness of the `fcntrl` API (*any* `Close`
// operation on this file (even if it's a different FD) from this PID, or overwriting
// of the file by *any* process breaks the lock.)
func (s *Storage) lockCP(ctx context.Context) error {
	var err error
	acquireLeaseResponse, err := s.lease.AcquireLease(ctx, -1, nil)
	if err != nil {
		return err
	}
	klog.V(2).Infof("Acquired lease ID %v", acquireLeaseResponse.LeaseID)
	s.leaseID = acquireLeaseResponse.LeaseID
	return nil
}

// unlockCP unlocks the `checkpoint.lock` file.
func (s *Storage) unlockCP(ctx context.Context) error {
	_, err := s.lease.BreakLease(ctx, nil)
	if err != nil {
		return err
	}
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
	blobClient := s.client.NewBlobClient(filepath.Join(bd, bf))
	blobDownloadResponse, err := blobClient.DownloadStream(context.TODO(), nil)
	if err != nil {
		return nil, fmt.Errorf("can't fetch entry bundle: %v", err)
	}
	reader := blobDownloadResponse.Body
	downloadData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("can't read entry bundle: %v", err)
	}
	return downloadData, nil
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
	if err := s.lockCP(ctx); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockCP(ctx); err != nil {
			panic(err)
		}
		s.Unlock()
	}()

	size, _, err := s.CurTree(ctx)
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
			client := s.client.NewBlockBlobClient(filepath.Join(bd, bf))
			if _, err = client.UploadBuffer(ctx, bundle.Bytes(), nil); err != nil {
				return 0, err
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
		client := s.client.NewBlockBlobClient(filepath.Join(bd, bf))
		if _, err = client.UploadBuffer(ctx, bundle.Bytes(), nil); err != nil {
			return 0, err
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
	if err := s.NewTree(ctx, newSize, newRoot, false); err != nil {
		return fmt.Errorf("newTree: %v", err)
	}
	return nil
}

// GetTile returns the tile at the given tile-level and tile-index.
// If no complete tile exists at that location, it will attempt to find a
// partial tile for the given tree size at that location.
func (s *Storage) GetTile(ctx context.Context, level, index, logSize uint64) (*api.Tile, error) {
	tileSize := layout.PartialTileSize(level, index, logSize)
	p := filepath.Join(layout.TilePath(s.path, level, index, tileSize))
	var tile api.Tile

	blobClient := s.client.NewBlobClient(p)
	blobDownloadResponse, err := blobClient.DownloadStream(context.TODO(), nil)
	if err != nil {
		//if *blobDownloadResponse.ErrorCode != "BlobNotFound" {
		return &tile, nil //fmt.Errorf("can't fetch entry bundle: %v", err)
		//}
	}
	reader := blobDownloadResponse.Body
	t, err := io.ReadAll(reader)
	if err != nil {
		return &tile, fmt.Errorf("can't read entry bundle: %v", err)
	}

	if err := tile.UnmarshalText(t); err != nil {
		return nil, fmt.Errorf("failed to parse tile: %w", err)
	}
	return &tile, nil
}

// StoreTile writes a tile out to disk.
// Fully populated tiles are stored at the path corresponding to the level &
// index parameters, partially populated (i.e. right-hand edge) tiles are
// stored with a .xx suffix where xx is the number of "tile leaves" in hex.
func (s *Storage) StoreTile(ctx context.Context, level, index uint64, tile *api.Tile) error {
	tileSize := uint64(tile.NumLeaves)
	klog.V(2).Infof("StoreTile: level %d index %x ts: %x", level, index, tileSize)
	if tileSize == 0 || tileSize > 256 {
		return fmt.Errorf("tileSize %d must be > 0 and <= 256", tileSize)
	}
	t, err := tile.MarshalText()
	if err != nil {
		return fmt.Errorf("failed to marshal tile: %w", err)
	}

	tDir, tFile := layout.TilePath(s.path, level, index, tileSize%256)
	tPath := filepath.Join(tDir, tFile)

	client := s.client.NewBlockBlobClient(tPath)
	if _, err := client.UploadBuffer(ctx, t, nil); err != nil {
		return err
	}

	return nil
}

// WriteCheckpoint stores a raw log checkpoint on disk.
func (s *Storage) WriteCheckpoint(ctx context.Context, newCPRaw []byte) error {
	lease := blob.LeaseAccessConditions{LeaseID: s.leaseID}
	acc := azblob.AccessConditions{LeaseAccessConditions: &lease}
	if _, err := s.cpClient.UploadBuffer(ctx, newCPRaw, &blockblob.UploadBufferOptions{AccessConditions: &acc}); err != nil {
		return fmt.Errorf("failed to upload checkpoint file: %w", err)
	}
	return nil
}

// WriteCheckpointNoLease stores a raw log checkpoint on disk.
func (s *Storage) WriteCheckpointNoLease(ctx context.Context, newCPRaw []byte) error {
	if _, err := s.cpClient.UploadBuffer(ctx, newCPRaw, nil); err != nil {
		return fmt.Errorf("failed to upload checkpoint file: %w", err)
	}
	return nil
}

// Readcheckpoint returns the latest stored checkpoint.
func (s *Storage) ReadCheckpoint(ctx context.Context) ([]byte, error) {

	get, err := s.cpClient.DownloadStream(ctx, nil)
	if err != nil {
		return []byte{}, err
	}

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return []byte{}, err
	}

	err = retryReader.Close()
	if err != nil {
		return []byte{}, err
	}

	cp := downloadedData.Bytes()

	return cp, nil
}

func (s *Storage) CurTree(ctx context.Context) (uint64, []byte, error) {
	b, err := s.ReadCheckpoint(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("ReadCheckpoint: %v", err)
	}
	cp, _, _, err := f_log.ParseCheckpoint(b, s.verifier.Name(), s.verifier)
	if err != nil {
		return 0, nil, err
	}
	return cp.Size, cp.Hash, nil
}

func (s *Storage) NewTree(ctx context.Context, size uint64, hash []byte, noLease bool) error {
	cp := &f_log.Checkpoint{
		Origin: s.signer.Name(),
		Size:   size,
		Hash:   hash,
	}
	n, err := note.Sign(&note.Note{Text: string(cp.Marshal())}, s.signer)
	if err != nil {
		return err
	}
	if noLease {
		return s.WriteCheckpointNoLease(ctx, n)
	}
	return s.WriteCheckpoint(ctx, n)
}
