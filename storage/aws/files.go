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
	lockS3Table       = "bettylog"
	lockDDBTable      = "bettyddblock"
	entriesTable      = "bettyentries"
	bundleSlicesTable = "bettybundlesslices"
	sequencedTable    = "bettysequenced"
	dedupTable        = "bettydedupnoname"
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

	sdkConfig aws.Config
	bucket    string
	s3        s3.Client
	id        int64
	ddb       dynamodb.Client

	sequencedBundleMaxSize   int
	integrateBundleBatchSize int

	dedupSeq    bool
	intWithLock bool

	sequencedKeys []struct {
		Idx    int64
		Offset int64
	}
}

// NewTreeFunc is the signature of a function which receives information about newly integrated trees.
type NewTreeFunc func(size uint64, root []byte) ([]byte, error)

// CurrentTree is the signature of a function which retrieves the current integrated tree size and root hash.
type CurrentTreeFunc func([]byte) (uint64, []byte, error)

// New creates a new S3 and DDB Storage
func New(ctx context.Context, path string, params log.Params, batchMaxAge time.Duration, batchSize int, curTree CurrentTreeFunc, newTree NewTreeFunc, bucketName string, sequencedBundleMaxSize, integrateBundleBatchSize int, seqWithLock, intWithLock, dedupSeq bool) *Storage {
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
		sdkConfig:                sdkConfig,
		s3:                       *s3Client,
		id:                       rand.Int63(),
		ddb:                      *ddbClient,
		integrateBundleBatchSize: integrateBundleBatchSize,
		sequencedBundleMaxSize:   sequencedBundleMaxSize,
		dedupSeq:                 dedupSeq,
		intWithLock:              intWithLock,
	}

	if seqWithLock {
		r.pool = writer.NewPool(batchSize, batchMaxAge, r.sequenceBatch)
	} else {
		r.pool = writer.NewPool(batchSize, batchMaxAge, r.sequenceBatchNoLock)
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

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				err := r.deleteSequencedBundles(ctx)
				if err != nil {
					klog.V(1).Infof("r.deleteSequencedBundles(): %v", err)
				}
			}
		}
	}()

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
	t := time.Now()
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
		ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	// TODO(phboneff): fix context
	output, err := s.ddb.PutItem(context.TODO(), input)
	if err != nil {
		var cdte *dynamodbtypes.ConditionalCheckFailedException
		if errors.As(err, &cdte) {
			// TODO: stop retrying after some point
			s.lockAWS(table)
		} else {
			klog.Fatalf("Got error calling PutItem: %s", err)
		}
	}
	klog.V(1).Infof("lockAWS - T: %v, R:%v, W:%v", *output.ConsumedCapacity.CapacityUnits, output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)
	klog.V(2).Infof("took %v to place a lock on table %s", time.Since(t), table)
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
		Key:                    av,
		TableName:              aws.String(table),
		ConditionExpression:    expr.Condition(),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	output, err := s.ddb.DeleteItem(context.TODO(), input)
	if err != nil {
		klog.Fatalf("Got error calling DeleteItem: %s", err)
	}

	klog.V(1).Infof("unlockAWS - R:%v, W:%v", output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)
	klog.V(2).Infof("Successfully Removed lock for %s to table %s", item.Logname, table)
	return nil
}

// Sequence commits to sequence numbers for an entry
// Returns the sequence number assigned to the first entry in the batch, or an error.
func (s *Storage) Sequence(ctx context.Context, b []byte) (uint64, error) {
	var key string
	idx, err := s.pool.Add(b)
	if err != nil {
		klog.V(1).Infof("can't add %s to pool: %v", key, err)
	}
	return idx, nil
}

// ContainsHashes looks for indexes matching keys in dedupTable
func (s *Storage) ContainsHashes(ctx context.Context, keys []string) (map[string]uint64, error) {
	t := time.Now()
	defer func() {
		klog.V(1).Infof("took %v to check if a duplicate exists", time.Since(t))
	}()
	ret := make(map[string]uint64)
	nKeys := len(keys)

	for i := 0; i < len(keys); i += 100 {
		allRequests := []map[string]dynamodbtypes.AttributeValue{}
		for _, k := range keys[i:min(i+100, nKeys)] {
			av := map[string]dynamodbtypes.AttributeValue{
				"Hash": &dynamodbtypes.AttributeValueMemberS{
					Value: k,
				},
			}
			allRequests = append(allRequests, av)
		}
		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				dedupTable: dynamodbtypes.KeysAndAttributes{Keys: allRequests},
			},
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		}

		t1 := time.Now()
		output, err := s.ddb.BatchGetItem(ctx, input)
		if err != nil {
			fmt.Println("Got an error doing BatchGetItem")
			return nil, fmt.Errorf("couldn't check that the log contains %v: %v", keys, err)
		} else if len(output.Responses) > 0 {
			// TODO check for element inclusion first
			for _, r := range output.Responses[dedupTable] {
				idx := &struct {
					Hash string
					Idx  uint64
				}{}
				if err := attributevalue.UnmarshalMap(r, idx); err != nil {
					return nil, fmt.Errorf("couldn't check that the log contains %v: %v", keys, err)
				}
				klog.V(2).Infof("Found matching entry in the log at index: %d", idx.Idx)
				ret[idx.Hash] = idx.Idx
			}
		}
		tC, tR, tW := 0.0, 0.0, 0.0
		for _, c := range output.ConsumedCapacity {
			if c.ReadCapacityUnits != nil {
				tR += *c.ReadCapacityUnits
			}
			if c.WriteCapacityUnits != nil {
				tW += *c.WriteCapacityUnits
			}
			if c.CapacityUnits != nil {
				tC += *c.CapacityUnits
			}
		}
		klog.V(1).Infof("ContainsHashes - Card: %v, C: %v, R: %v, W: %v", len(allRequests), tC, tR, tW)
		klog.V(1).Infof("took %v to do a BatchGetItem duplicate query", time.Since(t1))
	}
	return ret, nil
}

// DedupHashes store hashes and their matching index in dedupTable
func (s *Storage) DedupHashes(ctx context.Context, kv map[string]uint64) error {
	t := time.Now()
	defer func() {
		klog.V(1).Infof("took %v to write dedup hashes", time.Since(t))
	}()

	allRequests := []dynamodbtypes.WriteRequest{}
	for k, v := range kv {
		av := &dynamodbtypes.PutRequest{
			Item: map[string]dynamodbtypes.AttributeValue{
				"Hash": &dynamodbtypes.AttributeValueMemberS{
					Value: k,
				},
				"Idx": &dynamodbtypes.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", v),
				},
			},
		}
		allRequests = append(allRequests, dynamodbtypes.WriteRequest{PutRequest: av})
	}

	nRequests := len(allRequests)
	for i := 0; i < len(allRequests); i += 25 {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]dynamodbtypes.WriteRequest{
				dedupTable: allRequests[i:min(i+25, nRequests)],
			},
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		}

		t1 := time.Now()
		output, err := s.ddb.BatchWriteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("couldn't write new dedup entries %v: %v", kv, err)
		}
		tC, tR, tW := 0.0, 0.0, 0.0
		for _, c := range output.ConsumedCapacity {
			if c.ReadCapacityUnits != nil {
				tR += *c.ReadCapacityUnits
			}
			if c.WriteCapacityUnits != nil {
				tW += *c.WriteCapacityUnits
			}
			if c.CapacityUnits != nil {
				tC += *c.CapacityUnits
			}
		}
		klog.V(1).Infof("DedupHashes - Card: %v, C: %v, R: %v, W: %v", len(input.RequestItems[dedupTable]), tC, tR, tW)
		klog.V(1).Infof("took %v to do a BatchWriteItem duplicate query", time.Since(t1))
	}
	return nil
}

type latencySequence struct {
	lock     time.Duration
	readIdx  time.Duration
	stage    time.Duration
	writeIdx time.Duration
}

// sequenceBatch sequences a batch placing a lock in DynamoDB
func (s *Storage) sequenceBatch(ctx context.Context, batch writer.Batch) ([]uint64, error) {
	t := time.Now()
	startTime := t
	l := latencySequence{}
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
	l.lock = time.Since(t)
	t = time.Now()

	seq, err := s.ReadSequencedIndex()
	if err != nil {
		return nil, fmt.Errorf("can't read the current sequenced index: %v", err)
	}
	l.readIdx = time.Since(t)
	t = time.Now()

	ret, err := s.sequenceEntriesAsBundlesSlices(ctx, batch.Entries, seq)
	if err != nil {
		return nil, fmt.Errorf("couldn't sequence batch: %v", err)
	}
	l.stage = time.Since(t)
	t = time.Now()

	if err := s.WriteSequencedIndex(seq + uint64(len(batch.Entries))); err != nil {
		return nil, fmt.Errorf("couldn't commit to the sequenced Index: %v", err)
	}
	l.writeIdx = time.Since(t)

	klog.V(1).Infof("sequenceBatch: %v [lock: %v, readIdx: %v, stage: %v, writeIdx: %v]",
		time.Since(startTime),
		l.lock,
		l.readIdx,
		l.stage,
		l.writeIdx,
	)
	return ret, nil
}

type latencySequenceNolock struct {
	readIdx     time.Duration
	transaction time.Duration
}

// sequencebatchNoLock sequenced a batch in DynamoDB as an atomic transaction without using locks
func (s *Storage) sequenceBatchNoLock(ctx context.Context, batch writer.Batch) ([]uint64, error) {
	s.ddbMutex.Lock()
	defer func() {
		s.ddbMutex.Unlock()
	}()
	t := time.Now()
	l := latencySequenceNolock{}
	startTime := t
	seq, err := s.ReadSequencedIndex()
	currSeq := seq
	if err != nil {
		return nil, fmt.Errorf("can't read the current sequenced index: %v", err)
	}

	keys := make([]string, len(batch.Entries))
	ret := make([]uint64, len(batch.Entries))
	for i, e := range batch.Entries {
		hashB := sha256.Sum256(e)
		key := base64.StdEncoding.EncodeToString(hashB[:])
		keys[i] = key
	}
	dedupedIndex := make(map[string]uint64)
	if s.dedupSeq {
		var err error
		dedupedIndex, err = s.ContainsHashes(ctx, keys)
		if err != nil {
			return nil, fmt.Errorf("ContainsHashes: %v", err)
		}
	}
	newlySequenced := make(map[string]uint64)

	l.readIdx = time.Since(t)
	t = time.Now()

	bundleIndex, entriesInBundle := seq/uint64(s.params.EntryBundleSize), seq%uint64(s.params.EntryBundleSize)
	offset := entriesInBundle
	klog.V(1).Infof("Initial value of offset: %d", offset)
	writes := []dynamodbtypes.TransactWriteItem{}
	values := []string{}
	for i, e := range batch.Entries {
		if idx, ok := dedupedIndex[keys[i]]; ok {
			ret[i] = idx
			continue
		}
		newlySequenced[keys[i]] = currSeq
		values = append(values, string(e))
		ret[i] = currSeq
		currSeq++
		entriesInBundle++
		if entriesInBundle == uint64(s.params.EntryBundleSize) || len(values) == s.sequencedBundleMaxSize {
			entries, err := attributevalue.MarshalList(values)
			if err != nil {
				return nil, fmt.Errorf("error marshaling entries list: %v", err)
			}
			writes = append(writes, dynamodbtypes.TransactWriteItem{
				Put: &dynamodbtypes.Put{
					Item: map[string]dynamodbtypes.AttributeValue{
						"Idx": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", bundleIndex),
						},
						"Offset": &dynamodbtypes.AttributeValueMemberN{
							Value: fmt.Sprintf("%d", offset),
						},
						"Entries": &dynamodbtypes.AttributeValueMemberL{
							Value: entries,
						},
					},
					TableName: aws.String(bundleSlicesTable),
				},
			})
			if entriesInBundle == uint64(s.params.EntryBundleSize) {
				entriesInBundle = 0
				offset = 0
				bundleIndex++
			} else {
				offset += uint64(len(values))
			}
			values = []string{}
		}
	}
	if len(values) != 0 {
		entries, err := attributevalue.MarshalList(values)
		if err != nil {
			return nil, fmt.Errorf("error marshaling entries list: %v", err)
		}
		writes = append(writes, dynamodbtypes.TransactWriteItem{
			Put: &dynamodbtypes.Put{
				Item: map[string]dynamodbtypes.AttributeValue{
					"Idx": &dynamodbtypes.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", bundleIndex),
					},
					"Offset": &dynamodbtypes.AttributeValueMemberN{
						Value: fmt.Sprintf("%d", offset),
					},
					"Entries": &dynamodbtypes.AttributeValueMemberL{
						Value: entries,
					},
				},
				TableName: aws.String(bundleSlicesTable),
			},
		})
	}

	input := &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
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
							Value: fmt.Sprintf("%d", currSeq-seq),
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
		},
	}
	input.TransactItems = append(input.TransactItems, writes...)
	klog.V(1).Infof("write value of offset: %d", offset)
	output, err := s.ddb.TransactWriteItems(ctx, input)
	maxTries := 10
	for retry := 1; err != nil && retry < maxTries; retry++ {
		output, err = s.ddb.TransactWriteItems(ctx, input)
		klog.V(1).Infof("couldnt' write sequencing transation, will retry at most %d times: %v", maxTries-retry, err)
		// TODO(phboneff): retry if didn't work
		var cdte *dynamodbtypes.TransactionCanceledException
		if errors.As(err, &cdte) {
			klog.V(1).Infof("%v", cdte)
		}
	}
	l.transaction = time.Since(t)
	if err != nil {
		klog.V(1).Infof("couldnt' write sequencing transation: %v", err)
		return nil, err
	}
	tR, tW := 0.0, 0.0
	for _, c := range output.ConsumedCapacity {
		if c.ReadCapacityUnits != nil {
			tR += *c.ReadCapacityUnits
		}
		if c.WriteCapacityUnits != nil {
			tW += *c.WriteCapacityUnits
		}
	}
	klog.V(1).Infof("sequenceBatchNoLock - R:%v, W:%v", tR, tW)
	klog.V(1).Infof("sequenceBatchNoLock: %v [readIDx: %v, transaction: %v]", time.Since(startTime), l.readIdx, l.transaction)

	if s.dedupSeq {
		go s.DedupHashes(ctx, newlySequenced)
	}
	return ret, nil
}

type latencyIntegration struct {
	lock                   time.Duration
	readCP                 time.Duration
	readNextSequencedIndex time.Duration
	readBundle             time.Duration
	serialize              time.Duration
	integration            time.Duration
	delete                 time.Duration
}

// Integrate dumps last entries from the sequenceTable to S3, and integrates them
func (s *Storage) Integrate(ctx context.Context) (bool, error) {
	l := latencyIntegration{}
	t := time.Now()
	startTime := t

	defer func() {
		klog.V(1).Infof("Integrate: %v [lock: %v, readCP: %v, readBundle: %v, serialize: %v, integration: %v, delete: %v]",
			time.Since(startTime),
			l.lock,
			l.readCP,
			l.readBundle,
			l.serialize,
			l.integration,
			l.delete,
		)
	}()

	if s.intWithLock {
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
	}

	l.lock = time.Since(t)
	t = time.Now()

	currCP, err := s.ReadCheckpoint()
	if err != nil {
		klog.Fatalf("Couldn't load checkpoint: %v", err)
	}
	size, _, _ := s.curTree(currCP)
	l.readCP = time.Since(t)
	t = time.Now()

	nextSequencedIndex, err := s.ReadSequencedIndex()
	if err != nil {
		klog.Fatalf("ReadSequencedIndex(): %v", err)
	}
	if size == nextSequencedIndex {
		return false, nil
	}
	l.readNextSequencedIndex = time.Since(t)
	t = time.Now()

	// TODO: check that theindex returned here by the bundle actually matched the bundle that we will write to
	// TODO: avoid fetching entries that we have already integrated
	batches, more, err := s.getSequencedBundlesSlices(ctx, size/uint64(s.params.EntryBundleSize), s.integrateBundleBatchSize)
	if err != nil {
		return false, fmt.Errorf("getSequencesBundles: %v", err)
	}
	l.readBundle = time.Since(t)
	t = time.Now()
	klog.V(2).Infof("bundle batches to integrate: %v", len(batches))
	if len(batches) == 0 {
		return false, nil
	}

	entries := make([][]byte, 0)
	for _, b := range batches {
		// Write bundles to S3
		// no need to write entries that have already been written
		if len(b.Entries)+int(b.Idx)*s.params.EntryBundleSize < int(size) {
			continue
		}
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
	l.serialize = time.Since(t)
	t = time.Now()

	err = s.doIntegrate(ctx, size, entries)
	if err != nil {
		return false, fmt.Errorf("doIntegrate: %v", err)
	}
	l.integration = time.Since(t)

	return more, nil
}

type Batch struct {
	Logname string
	Idx     uint64
	Entries []string
}

type BatchSlice struct {
	Idx     uint64
	Offset  uint64
	Len     int
	Entries []string
}

// sequenceEntriesAsBundlesSlices splits a bundle into slices, dedupes entries, and writes them to the sequenceTable
func (s *Storage) sequenceEntriesAsBundlesSlices(ctx context.Context, entries [][]byte, firstIdx uint64) ([]uint64, error) {
	// done by reading the bundle form the table at all times, and not from this function
	bundleIndex, entriesInBundle := firstIdx/uint64(s.params.EntryBundleSize), firstIdx%uint64(s.params.EntryBundleSize)
	offset := entriesInBundle
	seq := firstIdx
	bundleSlice := [][]byte{}
	// Add new entries to the bundle
	keys := make([]string, len(entries))
	ret := make([]uint64, len(entries))
	for i, e := range entries {
		hashB := sha256.Sum256(e)
		key := base64.StdEncoding.EncodeToString(hashB[:])
		keys[i] = key
	}
	dedupedIndex := make(map[string]uint64)
	if s.dedupSeq {
		var err error
		dedupedIndex, err = s.ContainsHashes(ctx, keys)
		if err != nil {
			return nil, fmt.Errorf("ContainsHashes: %v", err)
		}
	}
	for i, e := range entries {
		encoded := []byte(base64.StdEncoding.EncodeToString(e))
		if idx, ok := dedupedIndex[keys[i]]; ok {
			ret[i] = idx
			continue
		}
		bundleSlice = append(bundleSlice, encoded)
		entriesInBundle++
		ret[i] = seq
		seq++
		if entriesInBundle == uint64(s.params.EntryBundleSize) || len(bundleSlice) == s.sequencedBundleMaxSize {
			//  This bundle is full, so we need to write it out...
			if err := s.stageBundleSlice(ctx, bundleSlice, bundleIndex, offset); err != nil {
				return nil, err
			}
			// ... and prepare the next entry bundle for any remaining entries in the batch
			if entriesInBundle == uint64(s.params.EntryBundleSize) {
				bundleIndex++
				entriesInBundle = 0
				offset = 0
			} else {
				offset += uint64(len(bundleSlice))
			}
			bundleSlice = [][]byte{}
		}
	}
	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		if err := s.stageBundleSlice(ctx, bundleSlice, bundleIndex, offset); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// stageBundleSlice stores a single slice in the sequence table
func (s *Storage) stageBundleSlice(ctx context.Context, entries [][]byte, bundleIdx uint64, offset uint64) error {
	value := make([]string, len(entries))
	for i, e := range entries {
		value[i] = string(e)
	}
	item := struct {
		Idx     uint64
		Offset  uint64
		Entries []string
	}{
		Idx:     bundleIdx,
		Offset:  offset,
		Entries: value,
	}
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		klog.Fatalf("Got error marshalling new movie item: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:                   av,
		TableName:              aws.String(bundleSlicesTable),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	output, err := s.ddb.PutItem(ctx, input)
	if err != nil {
		return err
	}
	klog.V(1).Infof("stageBundleSlice- R:%v, W:%v", output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)

	return nil
}

// getSequencedBundlesSlices reads Bundles from the sequencedtable
func (s *Storage) getSequencedBundlesSlices(ctx context.Context, startBundleIdx uint64, nBundle int) ([]Batch, bool, error) {
	// TODO: read bundles in paralell rater than sequencially
	batches := make([]Batch, 0, nBundle)
	for i := 0; i < nBundle; i++ {
		keyCond := expression.Key("Idx").Equal(expression.Value(startBundleIdx + uint64(i)))
		expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
		if err != nil {
			klog.Fatalf("Cannot create dynamodb condition: %v", err)
		}

		input := &dynamodb.QueryInput{
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeValues: expr.Values(),
			ExpressionAttributeNames:  expr.Names(),
			TableName:                 aws.String(bundleSlicesTable),
			ConsistentRead:            aws.Bool(true),
			ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityTotal,
		}

		output, err := s.ddb.Query(ctx, input)
		if err != nil {
			return nil, false, fmt.Errorf("error reading staged entries from DynamoDB: %v", err)
		}
		klog.V(1).Infof("getSequencedBundlesSlices - T: %v, R:%v, W:%v", *output.ConsumedCapacity.CapacityUnits, output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)
		batchSlices := []BatchSlice{}
		batches = append(batches, Batch{})
		if len(output.Items) == 0 {
			continue
		}
		if err := attributevalue.UnmarshalListOfMaps(output.Items, &batchSlices); err != nil {
			return nil, false, fmt.Errorf("can't unmarshall entries: %v", err)
		}
		for _, slice := range batchSlices {
			klog.V(2).Infof("fetched bundle starting at %d with offset %d", slice.Idx, slice.Offset)
			batches[i].Logname = s.path
			batches[i].Idx = slice.Idx
			batches[i].Entries = append(batches[i].Entries, slice.Entries...)
		}
		klog.V(1).Infof("getSequencedBundlesSlices - T: %v, R:%v, W:%v", *output.ConsumedCapacity.CapacityUnits, output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)
		klog.V(1).Infof("This is the remaning number of things to integrate: %v", output.ScannedCount)
	}

	// TODO: there must be a cheaper way to do this read
	keyCond := expression.Key("Idx").Equal(expression.Value(startBundleIdx + uint64(nBundle)))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		klog.Fatalf("Cannot create dynamodb condition: %v", err)
	}

	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		TableName:                 aws.String(bundleSlicesTable),
		Limit:                     aws.Int32(1),
		ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	output, err := s.ddb.Query(ctx, input)
	if err != nil {
		return nil, false, fmt.Errorf("error reading staged entries from DynamoDB: %v", err)
	}

	return batches, (len(output.Items) > 0), nil
}

func (s *Storage) deleteSequencedBundles(ctx context.Context) error {
	if len(s.sequencedKeys) == 0 {
		klog.Infof("no more sequenced bundles to garbage collect cached, will fetch new keys")
		currCP, err := s.ReadCheckpoint()
		if err != nil {
			klog.Infof("Couldn't load checkpoint:  %v", err)
		}
		curSize, _, err := s.curTree(currCP)
		if err != nil {
			klog.Infof("Can't get current tree: %v", err)
		}
		// 20 should be enough to make sure we don't delete a batch that we haven't integrated yet
		// TODO: better protect this
		if err := s.updateSequencedKeys(ctx, int64(curSize/uint64(s.params.EntryBundleSize))-20); err != nil {
			return fmt.Errorf("updateSequencedKeys(): %v", err)
		}
	}

	for len(s.sequencedKeys) > 0 {
		t := time.Now()

		allRequests := []dynamodbtypes.WriteRequest{}
		last := min(len(s.sequencedKeys), 25)
		for _, v := range s.sequencedKeys[0:last] {
			key, err := attributevalue.MarshalMap(v)
			if err != nil {
				return fmt.Errorf("error masrsalling keys of bundles to delete: %v", err)
			}
			av := &dynamodbtypes.DeleteRequest{
				Key: key,
			}
			allRequests = append(allRequests, dynamodbtypes.WriteRequest{DeleteRequest: av})
		}
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]dynamodbtypes.WriteRequest{
				bundleSlicesTable: allRequests,
			},
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		}

		output, err := s.ddb.BatchWriteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("couldn't delete sequenced bundles: %v", err)
		}
		tC, tR, tW := 0.0, 0.0, 0.0
		for _, c := range output.ConsumedCapacity {
			if c.ReadCapacityUnits != nil {
				tR += *c.ReadCapacityUnits
			}
			if c.WriteCapacityUnits != nil {
				tW += *c.WriteCapacityUnits
			}
			if c.CapacityUnits != nil {
				tC += *c.CapacityUnits
			}
		}
		klog.V(1).Infof("deleteSequencedBundles- C: %v, R: %v, W: %v", tC, tR, tW)
		klog.V(1).Infof("took %v to do a BatchWriteItem delete sequenced bundles query", time.Since(t))
		s.sequencedKeys = s.sequencedKeys[last:]
	}
	return nil
}

func (s *Storage) updateSequencedKeys(ctx context.Context, limit int64) error {
	filter := expression.Name("Idx").LessThan(expression.Value(limit))
	proj := expression.NamesList(expression.Name("Idx"), expression.Name("Offset"))
	expr, err := expression.NewBuilder().WithFilter(filter).WithProjection(proj).Build()
	if err != nil {
		klog.Fatalf("Cannot create dynamodb condition: %v", err)
	}

	input := &dynamodb.ScanInput{
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		TableName:                 aws.String(bundleSlicesTable),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityTotal,
		Limit:                     aws.Int32(500),
	}

	output, err := s.ddb.Scan(ctx, input)
	if err != nil {
		return fmt.Errorf("error reading staged entries indexes from DynamoDB: %v", err)
	}
	if err := attributevalue.UnmarshalListOfMaps(output.Items, &s.sequencedKeys); err != nil {
		return fmt.Errorf("couldn't get sequenced bundles to delete: %v", err)
	}
	klog.V(1).Infof("found %v sequenced bundle batches to delete: %v", len(s.sequencedKeys), s.sequencedKeys)
	return nil
}

// doIntegrate handles integrating new entries into the log, and updating the checkpoint.
func (s *Storage) doIntegrate(ctx context.Context, from uint64, batch [][]byte) error {
	klog.V(2).Infof("will integrate %d entries from size %d", len(batch), from)
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
		Key:                    av,
		TableName:              aws.String(sequencedTable),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	output, err := s.ddb.GetItem(context.TODO(), input)
	if err != nil {
		return 0, fmt.Errorf("error reading sequenced index from DynamoDB: %v", err)
	}
	val := SequencedVal{}
	if err := attributevalue.UnmarshalMap(output.Item, &val); err != nil {
		return 0, fmt.Errorf("can't unmarshall sequenced index: %v", err)
	}

	klog.V(1).Infof("ReadSequencedIndex - T: %v, R:%v, W:%v", *output.ConsumedCapacity.CapacityUnits, output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)
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
		Item:                   av,
		TableName:              aws.String(sequencedTable),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	}

	output, err := s.ddb.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("error reading sequenced index from DynamoDB: %v", err)
	}

	klog.V(1).Infof("WriteSequencedIndex - T: %v, R:%v, W:%v", *output.ConsumedCapacity.CapacityUnits, output.ConsumedCapacity.ReadCapacityUnits, output.ConsumedCapacity.WriteCapacityUnits)

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
