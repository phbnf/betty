package aws

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/AlCutter/betty/log"
	"github.com/AlCutter/betty/log/writer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/serverless-log/api"
	"github.com/transparency-dev/serverless-log/api/layout"
	"k8s.io/klog/v2"
)

const (
	lockTable = "bettylog"
)

// Storage implements storage functions for a POSIX filesystem.
// It leverages the POSIX atomic operations.
type Storage struct {
	sync.Mutex
	params log.Params
	path   string
	pool   *writer.Pool

	curTree CurrentTreeFunc
	newTree NewTreeFunc

	curSize uint64

	bucket string
	s3     s3.Client
	id     int64
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
		id:      rand.Int63(),
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

type CPLock struct {
	Logname string `json:"logname"`
	ID      int64  `json:"id"`
}

// lockCP places a POSIX advisory lock for the checkpoint.
// lockCP places a lock in DyamoDB for the checkpoint.
// It puts a ID alongside this lock, which can only be removed
// by the instance that put it.
// If it cannot put a lock, retries indefinitely.
func (s *Storage) lockCP() error {
	// Create DynamoDB client
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		klog.Fatalf("Cannot load config: %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)

	item := CPLock{
		Logname: s.path,
		ID:      s.id,
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		klog.Fatalf("Got error marshalling new movie item: %s", err)
	}

	cond := expression.AttributeNotExists(expression.Name("Logname"))
	expr, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		klog.Fatalf("Cannot create dynamodb condition: %v", err)
	}

	input := &dynamodb.PutItemInput{
		Item:                      av,
		TableName:                 aws.String(lockTable),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
	}

	// TODO(phboneff): fix context
	output, err := svc.PutItem(context.TODO(), input)
	if err != nil {
		var cdte *dynamodbtypes.ConditionalCheckFailedException
		if errors.As(err, &cdte) {
			// TODO(phboneff): better retry
			s.lockCP()
		} else {
			klog.Fatalf("Got error calling PutItem: %s", err)
		}
	}
	klog.V(2).Infof("PutItem output: %+v", output)

	klog.V(2).Infof("Successfully Acquired lock for %s to table %s", item.Logname, lockTable)
	return nil
}

type CPUnlock struct {
	Logname string `json:"logname"`
}

// unlockCP unlocks the Checkpoint in DynamoDB for the storage's ID.
func (s *Storage) unlockCP() error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		klog.Fatalf("Cannot load config: %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)

	item := CPUnlock{
		Logname: s.path,
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		klog.Fatalf("Got error marshalling new movie item: %s", err)
	}

	keyCond := expression.Key("id").Equal(expression.Value(s.id))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		klog.Fatalf("Cannot create dynamodb condition: %v", err)
	}

	input := &dynamodb.DeleteItemInput{
		Key:                 av,
		TableName:           aws.String(lockTable),
		ConditionExpression: expr.Condition(),
	}

	_, err = svc.DeleteItem(context.TODO(), input)
	if err != nil {
		klog.Fatalf("Got error calling DeleteItem: %s", err)
	}

	klog.V(2).Infof("Successfully Removed lock for %s to table %s", item.Logname, lockTable)
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
	return s.ReadFile(filepath.Join(bd, bf))
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
		currCP, err := s.ReadCheckpoint()
		if err != nil {
			klog.Fatalf("Couldn't load checkpoint:  %v", err)
		}
		size, _, _ := s.curTree(currCP)
		klog.V(2).Infof("I am removing the lock, from checkpoint size: %d\n", size)
		s.Unlock()
	}()

	currCP, err := s.ReadCheckpoint()
	if err != nil {
		klog.Fatalf("Couldn't load checkpoint:  %v", err)
	}
	size, _, err := s.curTree(currCP)
	klog.V(2).Infof("I have the lock, from checkpoint size: %d\n", size)

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
			if err := s.WriteFile(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
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
		if err := s.WriteFile(filepath.Join(bd, bf), bundle.Bytes()); err != nil {
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
	if err := s.NewTree(newSize, newRoot); err != nil {
		return fmt.Errorf("newTree: %v", err)
	}
	return nil
}

// GetTile returns the tile at the given tile-level and tile-index.
func (s *Storage) GetTile(_ context.Context, level, index, logSize uint64) (*api.Tile, error) {
	tileSize := layout.PartialTileSize(level, index, logSize)
	p := filepath.Join(layout.TilePath(s.path, level, index, tileSize))
	t, err := s.ReadFile(p)
	if err != nil {
		var nske *types.NoSuchKey
		if !errors.As(err, &nske) {
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

// StoreTile writes a tile out to S3.
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

	tDir, tFile := layout.TilePath(s.path, level, index, tileSize%256)
	tPath := filepath.Join(tDir, tFile)

	if err := s.WriteFile(tPath, t); err != nil {
		return err
	}

	return nil
}

// WriteCheckpoint stores a raw log checkpoint on disk.
func (s *Storage) WriteCheckpoint(newCPRaw []byte) error {
	path := filepath.Join(s.path, layout.CheckpointPath)
	size, _, _ := s.curTree(newCPRaw)
	klog.V(2).Infof("Writting checkpoint of size %d\n", size)
	if err := s.WriteFile(path, newCPRaw); err != nil {
		klog.Infof("Couldn't write checkpoint: %v", err)
	}
	return nil
}

// Readcheckpoint returns the latest stored checkpoint.
func (s *Storage) ReadCheckpoint() ([]byte, error) {
	return s.ReadFile(filepath.Join(s.path, layout.CheckpointPath))
}

func (s *Storage) NewTree(size uint64, hash []byte) error {
	cpRaw, err := s.newTree(size, hash)
	if err != nil {
		klog.Infof("could not create new tree: %v", err)
	}
	return s.WriteCheckpoint(cpRaw)
}

// WriteFile stores a file on S3.
func (s *Storage) WriteFile(path string, data []byte) error {
	_, err := s.s3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		klog.Infof("Couldn't write data at path %s: %v", path, err)
	}
	return nil
}

// ReadFile reas a file from S3.
func (s *Storage) ReadFile(path string) ([]byte, error) {
	result, err := s.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		klog.V(2).Infof("Couldn't get object %v:%v. Here's why: %v\n", s.bucket, path, err)
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
