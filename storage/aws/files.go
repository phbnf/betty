// package aws provides an interface to store log on top of S3.
// It uses DynamoDB for sequencing.
// To use this, populate:
//  - ~/.aws/config with a region
//  - ~/.aws/credential

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
	lockS3Table    = "bettylog"
	lockDDBTable   = "bettyddblock"
	entriesTable   = "bettyentries"
	sequencedTable = "bettysequenced"
)

// Storage implements storage functions on top of S3.
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
	ddb    dynamodb.Client
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
type NewTreeFunc func(size uint64, root []byte) ([]byte, error)

// CurrentTree is the signature of a function which retrieves the current integrated tree size and root hash.
type CurrentTreeFunc func([]byte) (uint64, []byte, error)

// New creates a new S3 storage.
func New(path string, params log.Params, batchMaxAge time.Duration, curTree CurrentTreeFunc, newTree NewTreeFunc, bucketName string) *Storage {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		klog.V(1).Infof("Couldn't load default configuration: %v", err)
		return nil
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	ddbClient := dynamodb.NewFromConfig(sdkConfig)

	r := &Storage{
		path:    path,
		params:  params,
		curTree: curTree,
		newTree: newTree,
		bucket:  bucketName,
		s3:      *s3Client,
		id:      rand.Int63(),
		ddb:     *ddbClient,
	}

	r.pool = writer.NewPool(params.EntryBundleSize, batchMaxAge, r.sequenceBatchAndIntegrate)

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

type Lock struct {
	Logname string `json:"logname"`
	ID      int64  `json:"id"`
}

// lockAWS places a lock in DyamoDB on a given table.
// It puts a ID alongside this lockAWS, which can only be removed
// by the instance that put it.
// If it cannot put a lockAWS, retries indefinitely.
func (s *Storage) lockAWS(table string) error {

	item := Lock{
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
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
	}

	// TODO(phboneff): fix context
	output, err := s.ddb.PutItem(context.TODO(), input)
	if err != nil {
		var cdte *dynamodbtypes.ConditionalCheckFailedException
		if errors.As(err, &cdte) {
			// TODO(phboneff): better retry
			s.lockAWS(table)
		} else {
			klog.Fatalf("Got error calling PutItem: %s", err)
		}
	}
	klog.V(2).Infof("PutItem output: %+v", output)

	klog.V(2).Infof("Successfully Acquired lock for %s to table %s", item.Logname, lockS3Table)
	return nil
}

type Unlock struct {
	Logname string `json:"logname"`
}

// unlockAWS releases a lock in DynamoDB at a given table for the storage's ID.
func (s *Storage) unlockAWS(table string) error {
	item := Unlock{
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
		TableName:           aws.String(table),
		ConditionExpression: expr.Condition(),
	}

	_, err = s.ddb.DeleteItem(context.TODO(), input)
	if err != nil {
		klog.Fatalf("Got error calling DeleteItem: %s", err)
	}

	klog.V(2).Infof("Successfully Removed lock for %s to table %s", item.Logname, lockS3Table)
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

// sequenceBatchAndIntegrate writes the entries from the provided batch into the entry bundle files of the log.
//
// This func starts filling entries bundles at the next available slot in the log, ensuring that the
// sequenced entries are contiguous from the zeroth entry (i.e left-hand dense).
// We try to minimise the number of partially complete entry bundles by writing entries in chunks rather
// than one-by-one.
func (s *Storage) sequenceBatchAndIntegrate(ctx context.Context, batch writer.Batch) (uint64, error) {
	_, err := s.sequenceBatch(ctx, batch)
	if err != nil {
		return 0, fmt.Errorf("s.sequenceBatch(): %v", err)
	}

	return s.integrate(ctx)
}

func (s *Storage) sequenceBatch(ctx context.Context, batch writer.Batch) (uint64, error) {
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The Dynamodb `LockCP()` ensures that distinct tasks are serialised.
	s.Lock()
	if err := s.lockAWS(lockDDBTable); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockAWS(lockDDBTable); err != nil {
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

	if err := s.stageEntries(ctx, batch.Entries, seq); err != nil {
		return 0, fmt.Errorf("couldn't sequence batch: %v", err)
	}

	if err := s.WriteSequencedIndex(seq + uint64(len(batch.Entries))); err != nil {
		return 0, fmt.Errorf("couldn't commit to the sequenced Index: %v", err)
	}
	return seq, nil
}

func (s *Storage) integrate(ctx context.Context) (uint64, error) {
	// TODO(phboneff): add this again on a differt lock
	//s.Lock()
	if err := s.lockAWS(lockS3Table); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockAWS(lockS3Table); err != nil {
			panic(err)
		}
		// TODO(phboneff): add this again on a differt lock
		//s.Unlock()
	}()

	entries, firstIdx, err := s.getSequencedEntries(ctx)
	if err != nil {
		return 0, fmt.Errorf("getStagedEntries: %v", err)
	}

	if len(entries) == 0 {
		klog.V(2).Info("nothing to integrate")
		return 0, nil
	}

	currCP, err := s.ReadCheckpoint()
	if err != nil {
		klog.Fatalf("Couldn't load checkpoint:  %v", err)
	}
	size, _, _ := s.curTree(currCP)

	if size != firstIdx {
		return 0, fmt.Errorf("the index of the first entry to integrate %d doesn't match with the sequenced index %d", firstIdx, size)
	}

	// TODO(phboneff): careful, need to make sure that two nodes don't override the same bundle. This is prob
	// done by reading the bundle form the table at all times, and not from this function
	bundleIndex, entriesInBundle := firstIdx/uint64(s.params.EntryBundleSize), firstIdx%uint64(s.params.EntryBundleSize)
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
	for _, e := range entries {
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
	err = s.doIntegrate(ctx, firstIdx, entries)
	if err != nil {
		return 0, fmt.Errorf("doIntegrate: %v", err)
	}

	// Then delete the entries that we have just integrated
	fmt.Println("alskjdgflkasjdlkgsajlkdg")
	return firstIdx, s.deleteSequencedEntries(ctx, firstIdx, uint64(len(entries)))
}

type Entry struct {
	Logname string
	Idx     uint64
	Value   []byte
}

func (s *Storage) stageEntries(ctx context.Context, entries [][]byte, startSize uint64) error {
	for i, e := range entries {
		// TODO(phboneff): see if I can bundle everything in on transation
		item := Entry{
			Logname: s.path,
			Idx:     startSize + uint64(i),
			Value:   e,
		}

		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			klog.Fatalf("Got error marshalling new movie item: %s", err)
		}

		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(entriesTable),
		}

		// TODO(phboneff): fix context
		_, err = s.ddb.PutItem(context.TODO(), input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) getSequencedEntries(ctx context.Context) ([][]byte, uint64, error) {
	keyCond := expression.Key("Logname").Equal(expression.Value(s.path))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		klog.Fatalf("Cannot create dynamodb condition: %v", err)
	}

	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		TableName:                 aws.String(entriesTable),
		ConsistentRead:            aws.Bool(true),
	}

	output, err := s.ddb.Query(ctx, input)
	if err != nil {
		return nil, 0, fmt.Errorf("error reading staged entries from DynamoDB: %v", err)
	}
	ret := make([][]byte, len(output.Items))
	var start uint64
	entries := []Entry{}
	if err := attributevalue.UnmarshalListOfMaps(output.Items, &entries); err != nil {
		return nil, 0, fmt.Errorf("can't unmarshall entries: %v", err)
	}
	for i, e := range entries {
		ret[i] = e.Value
	}
	if len(entries) > 0 {
		start = entries[0].Idx
	}

	// return the actual start index!
	return ret, start, nil
}

func (s *Storage) deleteSequencedEntries(ctx context.Context, start, len uint64) error {
	//TODO: batching. But it only allows 25 entries at a time
	for i := start; i < start+len; i++ {
		// TODO(phboneff): see if I can bundle everything in on transation
		item := struct {
			Logname string
			Idx     uint64
		}{
			Logname: s.path,
			Idx:     i,
		}

		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			klog.Fatalf("Got error marshalling key to delete entries: %s", err)
		}

		input := &dynamodb.DeleteItemInput{
			Key:       av,
			TableName: aws.String(entriesTable),
		}

		// TODO(phboneff): fix context
		_, err = s.ddb.DeleteItem(context.TODO(), input)
		if err != nil {
			return err
		}
	}
	return nil
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

// WriteCheckpoint stores a raw log checkpoint.
func (s *Storage) ExportCheckpoint(newCPRaw []byte) error {
	path := filepath.Join(s.path, layout.CheckpointPath)
	size, _, _ := s.curTree(newCPRaw)
	klog.V(2).Infof("Writting checkpoint of size %d\n", size)
	if err := s.WriteFile(path, newCPRaw); err != nil {
		klog.Infof("Couldn't write checkpoint: %v", err)
	}
	return nil
}

// Readcheckpoint returns the latest stored checkpoint.
func (s *Storage) ReadExportCheckpoint() ([]byte, error) {
	return s.ReadFile(filepath.Join(s.path, layout.CheckpointPath))
}

// WriteCheckpoint stores a raw log checkpoint.
func (s *Storage) WriteCheckpoint(newCPRaw []byte) error {
	// TODO(phboneff): make this write to DynamoDB instead
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
	// TODO(phboneff): make this read from DynamoDB instead
	return s.ReadFile(filepath.Join(s.path, layout.CheckpointPath))
}

type SequencedKey struct {
	Logname string `json:"logname"`
}

type SequencedVal struct {
	Idx uint64
}

func (s *Storage) ReadSequencedIndex() (uint64, error) {
	item := SequencedKey{
		Logname: s.path,
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		klog.Fatalf("Got error marshalling sequenced key: %s", err)
	}

	input := &dynamodb.GetItemInput{
		Key:       av,
		TableName: aws.String(sequencedTable),
	}

	output, err := s.ddb.GetItem(context.TODO(), input)
	if err != nil {
		return 0, fmt.Errorf("error reading sequenced index from DynamoDB: %v", err)
	}
	val := SequencedVal{}
	if err := attributevalue.UnmarshalMap(output.Item, &val); err != nil {
		return 0, fmt.Errorf("can't unmarshall sequenced index: %v", err)
	}

	return val.Idx, nil
}

func (s *Storage) WriteSequencedIndex(idx uint64) error {
	av, err := attributevalue.MarshalMap(struct {
		Logname string
		Idx     uint64
	}{
		Logname: s.path,
		Idx:     idx,
	})
	if err != nil {
		klog.Fatalf("Got error marshalling sequenced key: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(sequencedTable),
	}

	_, err = s.ddb.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("error reading sequenced index from DynamoDB: %v", err)
	}

	return nil
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
