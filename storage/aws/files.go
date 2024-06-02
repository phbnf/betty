// package aws provides an interface to store log on top of S3.
// It uses DynamoDB for sequencing.
// To use this, populate:
//  - ~/.aws/config with a region
//  - ~/.aws/credential

package aws

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
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
	dedupTable     = "bettydedup"
)

// Storage implements storage functions on top of S3.
type Storage struct {
	sync.Mutex
	ddbMutex sync.Mutex
	params   log.Params
	path     string
	pool     *writer.Pool

	curTree CurrentTreeFunc
	newTree NewTreeFunc

	curSize uint64

	bucket string
	s3     s3.Client
	id     int64
	ddb    dynamodb.Client

	integrateBundleBatchSize int
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
type NewTreeFunc func(size uint64, root []byte) ([]byte, error)

// CurrentTree is the signature of a function which retrieves the current integrated tree size and root hash.
type CurrentTreeFunc func([]byte) (uint64, []byte, error)

// New creates a new S3 and DDB Storage
func New(ctx context.Context, path string, params log.Params, batchMaxAge time.Duration, curTree CurrentTreeFunc, newTree NewTreeFunc, bucketName string, integrateBundleBatchSize int, withlock bool) *Storage {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		klog.V(1).Infof("Couldn't load default configuration: %v", err)
		return nil
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	ddbClient := dynamodb.NewFromConfig(sdkConfig)

	r := &Storage{
		path:                     path,
		params:                   params,
		curTree:                  curTree,
		newTree:                  newTree,
		bucket:                   bucketName,
		s3:                       *s3Client,
		id:                       rand.Int63(),
		ddb:                      *ddbClient,
		integrateBundleBatchSize: integrateBundleBatchSize,
	}

	if withlock {
		r.pool = writer.NewPool(params.EntryBundleSize, batchMaxAge, r.sequenceBatch)
	} else {
		r.pool = writer.NewPool(params.EntryBundleSize, batchMaxAge, r.sequenceBatchNoLock)
	}

	currCP, err := r.ReadCheckpoint()
	if err != nil {
		klog.Infof("Couldn't load checkpoint:  %v", err)
	}
	curSize, _, err := curTree(currCP)
	if err != nil {
		klog.Infof("Can't get current tree: %v", err)
	}

	r.curSize = curSize

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// TODO: handle errors
				more := true
				for more {
					more, err = r.Integrate(ctx)
					if err != nil {
						klog.V(1).Infof("r.Integrate(): %v", err)
					}
				}
			}
		}
	}()

	return r
}

func (s *Storage) IntegrationLoop(ctx context.Context, frequency time.Duration) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			more := true
			for more {
				var err error
				more, err = s.Integrate(ctx)
				if err != nil {
					klog.V(1).Infof("r.Integrate(): %v", err)
				}
			}
		}
	}
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
	_, err = s.ddb.PutItem(context.TODO(), input)
	if err != nil {
		var cdte *dynamodbtypes.ConditionalCheckFailedException
		if errors.As(err, &cdte) {
			// TODO: stop retrying after some point
			s.lockAWS(table)
		} else {
			klog.Fatalf("Got error calling PutItem: %s", err)
		}
	}

	klog.V(2).Infof("Successfully Acquired lock for %s to table %s", item.Logname, table)
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

	klog.V(2).Infof("Successfully Removed lock for %s to table %s", item.Logname, table)
	return nil
}

// Sequence commits to sequence numbers for an entry
// Returns the sequence number assigned to the first entry in the batch, or an error.
func (s *Storage) Sequence(ctx context.Context, b []byte, dedup bool) (uint64, error) {
	var key string
	if dedup {
		hashB := sha256.Sum256(b)
		key = base64.StdEncoding.EncodeToString(hashB[:])
		idx, ok, err := s.ContainsHash(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("can't check entry hashing to %s for deduplication: %v", key, err)
		}
		if ok {
			return idx, nil
		}
	}
	idx, err := s.pool.Add(b)
	if dedup {
		if err != nil {
			return 0, fmt.Errorf("can't add %s to pool: %v", key, err)
		}
		if err := s.AddHash(ctx, key, idx); err != nil {
			return 0, fmt.Errorf("can't check entry hashing to %s for deduplication: %v", key, err)
		}
	}
	return idx, nil
}

type dedup struct {
	Logname string
	Hash    string
	Idx     uint64
}

func (s *Storage) AddHash(ctx context.Context, key string, idx uint64) error {
	item := dedup{
		Logname: s.path,
		Hash:    key,
		Idx:     idx,
	}
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("got error marshalling new dedup value: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:                                av,
		TableName:                           aws.String(dedupTable),
		ConditionExpression:                 aws.String("attribute_not_exists(Idx)"),
		ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureNone,
	}

	_, err = s.ddb.PutItem(ctx, input)
	if err != nil {
		// Means that the entry was submitted after we checked for dedup, but before sequencing
		var ccf *dynamodbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil
		}
		return fmt.Errorf("couldn't add index for key%v: %v", key, err)
	}
	return nil
}

func (s *Storage) ContainsHash(ctx context.Context, key string) (uint64, bool, error) {
	item := struct {
		Logname string
		Hash    string
	}{
		Logname: s.path,
		Hash:    key,
	}
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return 0, false, fmt.Errorf("got error marshalling new dedup key: %s", err)
	}

	input := &dynamodb.GetItemInput{
		Key:                  av,
		TableName:            aws.String(dedupTable),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("Idx"),
	}

	output, err := s.ddb.GetItem(ctx, input)
	if err != nil {
		return 0, false, fmt.Errorf("couldn't check that the log contains %v: %v", key, err)
	} else if len(output.Item) > 0 {
		idx := &struct {
			Idx uint64
		}{}
		if err := attributevalue.UnmarshalMap(output.Item, idx); err != nil {
			return 0, false, fmt.Errorf("couldn't check that the log contains %v: %v", key, err)
		}
		klog.V(2).Infof("Found matching entry in the log at index: %d", idx.Idx)
		return idx.Idx, true, nil
	}
	return 0, false, nil
}

// sequenceBatchAndIntegrate writes the entries from the provided batch into the entry bundle files of the log.
//
// This func starts filling entries bundles at the next available slot in the log, ensuring that the
// sequenced entries are contiguous from the zeroth entry (i.e left-hand dense).
// We try to minimise the number of partially complete entry bundles by writing entries in chunks rather
// than one-by-one.
func (s *Storage) sequenceBatchAndIntegrate(ctx context.Context, batch writer.Batch) error {
	_, err := s.sequenceBatch(ctx, batch)
	if err != nil {
		return fmt.Errorf("s.sequenceBatch(): %v", err)
	}

	more := true
	for more {
		more, err = s.Integrate(ctx)
		if err != nil {
			klog.V(1).Infof("s.Integrate(): %v", err)
		}
	}
	return err
}

func (s *Storage) sequenceBatch(ctx context.Context, batch writer.Batch) (uint64, error) {
	n := time.Now()
	// Double locking:
	// - The mutex `Lock()` ensures that multiple concurrent calls to this function within a task are serialised.
	// - The Dynamodb `LockAWS()` ensures that distinct tasks are serialised.
	s.ddbMutex.Lock()
	if err := s.lockAWS(lockDDBTable); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockAWS(lockDDBTable); err != nil {
			panic(err)
		}
		s.ddbMutex.Unlock()
	}()

	seq, err := s.ReadSequencedIndex()
	if err != nil {
		return 0, fmt.Errorf("can't read the current sequenced index: %v", err)
	}
	klog.V(1).Infof("took %v to read the current sequenced index", time.Since(n))
	n = time.Now()

	if err := s.sequenceEntriesAsBundles(ctx, batch.Entries, seq); err != nil {
		return 0, fmt.Errorf("couldn't sequence batch: %v", err)
	}
	klog.V(1).Infof("took %v to stage the %v sequenced entries", time.Since(n), len(batch.Entries))
	n = time.Now()

	if err := s.WriteSequencedIndex(seq + uint64(len(batch.Entries))); err != nil {
		return 0, fmt.Errorf("couldn't commit to the sequenced Index: %v", err)
	}
	klog.V(1).Infof("took %v to write the new sequenced index", time.Since(n))
	return seq, nil
}

func (s *Storage) sequenceBatchNoLock(ctx context.Context, batch writer.Batch) (uint64, error) {
	t := time.Now()
	seq, err := s.ReadSequencedIndex()
	if err != nil {
		return 0, fmt.Errorf("can't read the current sequenced index: %v", err)
	}
	klog.V(1).Infof("took %v to read the current sequenced index %d", time.Since(t), seq)

	// done by reading the bundle form the table at all times, and not from this function
	bundleIndex, entriesInBundle := seq/uint64(s.params.EntryBundleSize), seq%uint64(s.params.EntryBundleSize)
	appendCount := min(uint64(s.params.EntryBundleSize)-entriesInBundle, uint64(len(batch.Entries)))
	newCount := uint64(len(batch.Entries)) - appendCount

	values := make([]string, appendCount)
	for i := uint64(0); i < appendCount; i++ {
		values[i] = string(batch.Entries[i])
	}
	entries, err := attributevalue.MarshalList(values)
	if err != nil {
		return 0, fmt.Errorf("error marshaling entries list: %v", err)
	}

	values_next := make([]string, newCount)
	for i := 0; uint64(i) < newCount; i++ {
		values_next[i] = string(batch.Entries[i+int(appendCount)])
	}
	entries_next, err := attributevalue.MarshalList(values_next)
	if err != nil {
		return 0, fmt.Errorf("error marshaling entries list: %v", err)
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{
			dynamodbtypes.TransactWriteItem{
				Update: &dynamodbtypes.Update{
					Key: map[string]dynamodbtypes.AttributeValue{
						"Logname": &dynamodbtypes.AttributeValueMemberS{
							Value: s.path,
						},
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":increment": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", appendCount+newCount),
						},
						":seq": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", seq),
						},
					},
					UpdateExpression:    aws.String("SET Idx = Idx + :increment"),
					ConditionExpression: aws.String("Idx = :seq OR attribute_not_exists(Idx)"),
					TableName:           aws.String(sequencedTable),
				},
			},
			dynamodbtypes.TransactWriteItem{
				Update: &dynamodbtypes.Update{
					Key: map[string]dynamodbtypes.AttributeValue{
						"Logname": &dynamodbtypes.AttributeValueMemberS{
							Value: s.path,
						},
						"Idx": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", bundleIndex),
						},
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":entries": &dynamodbtypes.AttributeValueMemberL{
							Value: entries,
						},
						":empty_list": &dynamodbtypes.AttributeValueMemberL{
							Value: []dynamodbtypes.AttributeValue{},
						},
					},
					UpdateExpression: aws.String("SET Entries = list_append(if_not_exists(Entries, :empty_list), :entries)"),
					TableName:        aws.String(entriesTable),
				},
			},
			dynamodbtypes.TransactWriteItem{
				Put: &dynamodbtypes.Put{
					Item: map[string]dynamodbtypes.AttributeValue{
						"Logname": &dynamodbtypes.AttributeValueMemberS{
							Value: s.path,
						},
						"Idx": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", (bundleIndex + 1)),
						},
						"Entries": &dynamodbtypes.AttributeValueMemberL{
							Value: entries_next,
						},
					},
					TableName: aws.String(entriesTable),
				},
			},
		},
	}
	_, err = s.ddb.TransactWriteItems(ctx, input)
	if err != nil {
		klog.V(2).Infof("couldnt' write sequencing transation: %v", err)
	}
	return seq, nil

}

func (s *Storage) Integrate(ctx context.Context) (bool, error) {
	t := time.Now()
	s.Lock()
	if err := s.lockAWS(lockS3Table); err != nil {
		panic(err)
	}
	defer func() {
		if err := s.unlockAWS(lockS3Table); err != nil {
			panic(err)
		}
		s.Unlock()
	}()

	klog.V(1).Infof("took %v to place an integration lock", time.Since(t))
	t = time.Now()

	currCP, err := s.ReadCheckpoint()
	if err != nil {
		klog.Fatalf("Couldn't load checkpoint: %v", err)
	}
	size, _, _ := s.curTree(currCP)
	klog.V(1).Infof("took %v to read the checkpoint to integrate to", time.Since(t))
	t = time.Now()

	// TODO: check that theindex returned here by the bundle actually matched the bundle that we will write to
	batches, more, err := s.getSequencedBundles(ctx, size/uint64(s.params.EntryBundleSize), s.integrateBundleBatchSize)
	if err != nil {
		return false, fmt.Errorf("getSequencesBundles: %v", err)
	}
	if len(batches) == 0 {
		klog.V(2).Info("nothing to integrate")
		return false, nil
	}
	klog.V(1).Infof("took %v to read sequences bundles", time.Since(t))
	t = time.Now()
	firstBundleIndex := batches[0].Idx

	entries := make([][]byte, 0)
	for _, b := range batches {
		// Write bundles to S3
		bd, bf := layout.SeqPath(s.path, b.Idx)
		if len(b.Entries) < s.params.EntryBundleSize {
			bf = fmt.Sprintf("%s.%d", bf, len(b.Entries))
		}
		// TODO: do something more eleguqnt than this
		data := []byte(strings.Join(b.Entries, "\n"))
		s.WriteFile(filepath.Join(bd, bf), data)
		for i, e := range b.Entries {
			// Only start integrating entries past the current checkpoint
			if b.Idx*(uint64(s.params.EntryBundleSize))+uint64(i) >= size {
				entries = append(entries, []byte(e))
			}
		}
	}
	klog.V(1).Infof("took %v to serialize the entries to integrate and write them to S3", time.Since(t))
	t = time.Now()

	err = s.doIntegrate(ctx, size, entries)
	if err != nil {
		return false, fmt.Errorf("doIntegrate: %v", err)
	}
	klog.V(1).Infof("took %v to integrate entries", time.Since(t))
	t = time.Now()

	// TODO: don't delete entries yet. This can be done asynchronously just keep track of the last sequenced index
	// Then delete the bundles that we will never need to integrate again
	firstEntryIndex := firstBundleIndex * uint64(s.params.EntryBundleSize)
	fullBundles := (firstEntryIndex%uint64(s.params.EntryBundleSize) + uint64(len(entries))) / uint64(s.params.EntryBundleSize)
	klog.V(2).Infof("Will delete %d bundles from %d", fullBundles, firstBundleIndex)
	if err := s.deleteSequencedBundles(ctx, firstBundleIndex, fullBundles); err != nil {
		return false, fmt.Errorf("deleteSequencedBundles(): err")
	}
	klog.V(1).Infof("took %v to delete old bundles", time.Since(t))

	return more, nil
}

type Batch struct {
	Logname string
	Idx     uint64
	Entries []string
}

func (s *Storage) sequenceEntriesAsBundles(ctx context.Context, entries [][]byte, firstIdx uint64) error {
	// done by reading the bundle form the table at all times, and not from this function
	bundleIndex, entriesInBundle := firstIdx/uint64(s.params.EntryBundleSize), firstIdx%uint64(s.params.EntryBundleSize)
	bundle := [][]byte{}
	if entriesInBundle > 0 {
		// If the latest bundle is partial, we need to read the data it contains in for our newer, larger, bundle.
		part, err := s.getSequencedBundle(ctx, bundleIndex)
		// TODO: check that this is not a "not found error"
		if err != nil {
			return err
		}
		bundle = append(bundle, part...)
	}
	// Add new entries to the bundle
	for _, e := range entries {
		encoded := []byte(base64.StdEncoding.EncodeToString(e))
		bundle = append(bundle, encoded)
		entriesInBundle++
		if entriesInBundle == uint64(s.params.EntryBundleSize) {
			//  This bundle is full, so we need to write it out...
			if err := s.stageBundle(ctx, bundle, bundleIndex); err != nil {
				return err
			}
			// ... and prepare the next entry bundle for any remaining entries in the batch
			bundleIndex++
			entriesInBundle = 0
			bundle = [][]byte{}
		}
	}
	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		if err := s.stageBundle(ctx, bundle, bundleIndex); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) stageBundle(ctx context.Context, entries [][]byte, bundleIdx uint64) error {
	value := make([]string, len(entries))
	for i, e := range entries {
		value[i] = string(e)
	}
	item := Batch{
		Logname: s.path,
		Idx:     bundleIdx,
		Entries: value,
	}
	av, err := attributevalue.MarshalMap(item)
	fmt.Println(av)
	if err != nil {
		klog.Fatalf("Got error marshalling new movie item: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(entriesTable),
	}

	// TODO(phboneff): fix context
	_, err = s.ddb.PutItem(ctx, input)
	if err != nil {
		klog.Fatalf("Couldn't write bundle: %v", err)
	}

	return nil
}

func (s *Storage) getSequencedBundles(ctx context.Context, startBundleIdx uint64, nBundle int) ([]Batch, bool, error) {
	// TODO: handle more bundles better than this
	keyCond := expression.Key("Logname").Equal(expression.Value(s.path)).And(expression.Key("Idx").GreaterThanEqual(expression.Value(startBundleIdx)))
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
		Limit:                     aws.Int32(int32(nBundle) + 1),
	}

	output, err := s.ddb.Query(ctx, input)
	klog.V(1).Infof("This is the remaning number of things to integrate: %v", output.ScannedCount)
	if err != nil {
		return nil, false, fmt.Errorf("error reading staged entries from DynamoDB: %v", err)
	}
	batches := []Batch{}
	if len(output.Items) == 0 {
		return batches, false, nil
	}
	n := len(output.Items)
	if n > nBundle {
		n = nBundle
	}
	if err := attributevalue.UnmarshalListOfMaps(output.Items[:n], &batches); err != nil {
		return nil, false, fmt.Errorf("can't unmarshall entries: %v", err)
	}

	// return the actual start index!
	return batches, (len(output.Items) > nBundle), nil
}

func (s *Storage) getSequencedBundle(ctx context.Context, idx uint64) ([][]byte, error) {
	key := struct {
		Logname string
		Idx     uint64
	}{
		Logname: s.path,
		Idx:     idx,
	}

	av, err := attributevalue.MarshalMap(key)
	if err != nil {
		klog.Fatalf("Got error marshalling sequenced key: %s", err)
	}

	input := &dynamodb.GetItemInput{
		TableName:      aws.String(entriesTable),
		Key:            av,
		ConsistentRead: aws.Bool(true),
	}

	output, err := s.ddb.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error reading staged entries from DynamoDB: %v", err)
	}
	val := Batch{}
	if err := attributevalue.UnmarshalMap(output.Item, &val); err != nil {
		return nil, fmt.Errorf("can't unmarshall sequenced index: %v", err)
	}
	ret := make([][]byte, len(val.Entries))
	for i, e := range val.Entries {
		ret[i] = []byte(e)
	}

	return ret, nil
}

func (s *Storage) deleteSequencedBundles(ctx context.Context, start, len uint64) error {
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
			klog.Fatalf("Got error marshalling key to delete bundles: %s", err)
		}

		input := &dynamodb.DeleteItemInput{
			Key:       av,
			TableName: aws.String(entriesTable),
		}

		_, err = s.ddb.DeleteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("could not delete bundle %d: %v", i, err)
		}
	}
	klog.V(2).Infof("successfully removed bundles %d to %d from the sequenced table", start, start+len)
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
		Key:            av,
		TableName:      aws.String(sequencedTable),
		ConsistentRead: aws.Bool(true),
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
