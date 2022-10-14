// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package streams

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	noPendingDuration = time.Minute
)

type partitionOwners[T any] struct {
	owners map[int32]*producerNode[T]
	mux    *sync.Mutex
}

func (po partitionOwners[T]) owned(p int32, pn *producerNode[T]) bool {
	po.mux.Lock()
	defer po.mux.Unlock()
	return po.owners[p] == pn
}

func (po partitionOwners[T]) set(p int32, pn *producerNode[T]) {
	po.mux.Lock()
	po.owners[p] = pn
	po.mux.Unlock()
}

func (po partitionOwners[T]) conditionallyUpdate(p int32, pn *producerNode[T]) {
	po.mux.Lock()
	if _, ok := po.owners[p]; !ok {
		po.owners[p] = pn
	}
	po.mux.Unlock()
}

func (po partitionOwners[T]) clear(p int32) {
	po.mux.Lock()
	delete(po.owners, p)
	po.mux.Unlock()
}

// a container that allows to know who produced the records
// needed for txn error conditions while there are revoked partitions
// allows us to filter recordsToProduce on a retry and exclude partitions that have been revoked
type pendingRecord struct {
	record    *Record
	partition int32
	cb        func(*Record, error)
}

var pendingRecordPool = sak.NewPool(10, func() []pendingRecord {
	return make([]pendingRecord, 0)
}, func(a []pendingRecord) []pendingRecord {
	for i := range a {
		a[i].record = nil
	}
	return a[0:0]
})

type eosProducerPool[T StateStore] struct {
	producerNodeQueue chan *producerNode[T]
	onDeck            *producerNode[T]
	commitQueue       chan *producerNode[T]
	buffer            chan *EventContext[T]
	cfg               EosConfig
	source            *Source
	producerNodes     []*producerNode[T]
	flushTimer        *time.Ticker
	partitionOwners   partitionOwners[T]
	startTime         time.Time
	errorChannel      chan error
	commitClient      *kgo.Client
}

func newEOSProducerPool[T StateStore](source *Source, commitLog *eosCommitLog, cfg EosConfig, commitClient *kgo.Client, metrics chan Metric) *eosProducerPool[T] {
	pp := &eosProducerPool[T]{
		cfg:               cfg,
		producerNodeQueue: make(chan *producerNode[T], cfg.PoolSize),
		commitQueue:       make(chan *producerNode[T], cfg.PendingTxnCount),
		buffer:            make(chan *EventContext[T], 1024),
		producerNodes:     make([]*producerNode[T], 0, cfg.PoolSize),
		errorChannel:      make(chan error, 1024), //giving this some size so we don't block on errors in other go routines
		source:            source,
		commitClient:      commitClient,
		partitionOwners: partitionOwners[T]{
			owners: make(map[int32]*producerNode[T]),
			mux:    new(sync.Mutex),
		},
		flushTimer: time.NewTicker(noPendingDuration),
	}
	var first *producerNode[T]
	var prev *producerNode[T]
	var last *producerNode[T]
	for i := 0; i < cfg.PoolSize; i++ {
		p := newProducerNode(i, source, commitLog, pp.partitionOwners, commitClient, metrics, pp.errorChannel)
		pp.producerNodes = append(pp.producerNodes, p)
		if first == nil {
			first = p
		} else {
			prev.next = p
		}
		last = p
		prev = p
		pp.producerNodeQueue <- p
	}
	last.next = first
	pp.onDeck = <-pp.producerNodeQueue
	go pp.forwardExecutionContexts()
	go pp.commitLoop()
	return pp
}

// buffer the event context until a producer node is available
func (pp *eosProducerPool[T]) addEventContext(ec *EventContext[T]) {
	pp.buffer <- ec
}

func (pp *eosProducerPool[T]) maxPendingItems() int {
	return pp.cfg.MaxBatchSize * pp.cfg.PoolSize
}

func (pp *eosProducerPool[T]) doForwardExecutionContexts(ec *EventContext[T]) {
	if ec.isRevoked() {
		// if we're revoked, don't even add this to the onDeck producer
		ec.producerChan <- nil
		ec.revocationWaiter.Done()
		return
	}
	txnStarted := pp.onDeck.addEventContext(ec)
	pp.partitionOwners.conditionallyUpdate(ec.partition(), pp.onDeck)
	ec.producerChan <- pp.onDeck
	// off to the races
	if pp.shouldTryFlush() {
		pp.tryFlush()
	} else if txnStarted {
		pp.flushTimer.Reset(pp.cfg.BatchDelay)
	}
}

func (pp *eosProducerPool[T]) forwardExecutionContexts() {
	for {
		select {
		case ec := <-pp.buffer:
			pp.doForwardExecutionContexts(ec)
		case <-pp.flushTimer.C:
			pp.tryFlush()
		}
	}
}

func (pp *eosProducerPool[T]) shouldTryFlush() bool {
	return sak.Max(pp.onDeck.eventContextCnt, atomic.LoadInt64(&pp.onDeck.produceCnt)) >= int64(pp.cfg.TargetBatchSize)
}

func (pp *eosProducerPool[T]) shouldForceFlush() bool {
	return sak.Max(pp.onDeck.eventContextCnt, atomic.LoadInt64(&pp.onDeck.produceCnt)) == int64(pp.cfg.MaxBatchSize)
}

func (pp *eosProducerPool[T]) tryFlush() {
	if pp.shouldForceFlush() {
		// the onDeck producer is full
		// force a swap, blocking until successful
		pp.commitQueue <- pp.onDeck
		pp.onDeck = <-pp.producerNodeQueue
	} else if pp.onDeck.eventContextCnt > 0 {
		// the committing channel is full, reset the purge timer
		// to push any lingering items.
		select {
		// try to swap so we can commit as fast as possible
		case pp.commitQueue <- pp.onDeck:
			pp.onDeck = <-pp.producerNodeQueue
		default:
			// we have pending items, try again in 5ms
			// if new items come in during this interval, this timer may get reset
			// and the flush proces will begin again
			pp.flushTimer.Reset(pp.cfg.BatchDelay)
		}
	} else {
		// we don't have pending items, no reason to burn CPU, set the timer to an hour
		pp.flushTimer.Reset(noPendingDuration)
	}

}

func (pp *eosProducerPool[T]) commitLoop() {
	for {
		var err error
		select {
		case p := <-pp.commitQueue:
			err = pp.commit(p)
		case err = <-pp.errorChannel:
		}
		if err != nil {
			if instructions := pp.source.eosErrorHandler()(err); instructions != Continue {
				pp.source.fail(err)
				switch instructions {
				case FailConsumer:
					return
				case FatallyExit:
					panic(err)
				}
			}
		}
	}
}

func (pp *eosProducerPool[T]) commit(p *producerNode[T]) error {
	if pp.startTime.IsZero() {
		pp.startTime = time.Now()
	}
	err := p.commit()
	if err != nil {
		log.Errorf("txn commit error: %v", err)
		return err
	}
	pp.producerNodeQueue <- p
	return err
}

type eventContextDll[T any] struct {
	root, tail *EventContext[T]
}

type producerNode[T any] struct {
	client            *kgo.Client
	commitClient      *kgo.Client
	txnContext        context.Context
	txnContextCancel  func()
	commitLog         *eosCommitLog
	next              *producerNode[T]
	metrics           chan Metric
	source            *Source
	recordsToProduce  []pendingRecord
	produceCnt        int64
	byteCount         int64
	eventContextCnt   int64
	currentPartitions map[int32]eventContextDll[T]
	partitionOwners   partitionOwners[T]
	shouldMarkCommit  bool
	commitWaiter      sync.Mutex // this is a mutex masquerading as a WaitGroup
	partitionLock     sync.RWMutex
	produceLock       sync.Mutex
	firstEvent        time.Time
	id                int
	txnErrorHandler   TxnErrorHandler
	errorChannel      chan error
	// errs                  []error
}

func newProducerNode[T StateStore](id int, source *Source, commitLog *eosCommitLog, partitionOwners partitionOwners[T], commitClient *kgo.Client, metrics chan Metric, errorChannel chan error) *producerNode[T] {
	client := sak.Must(NewClient(
		source.stateCluster(),
		kgo.RecordPartitioner(NewOptionalPartitioner(kgo.StickyKeyPartitioner(nil))),
		kgo.TransactionalID(uuid.NewString()),
		kgo.TransactionTimeout(30*time.Second),
	))

	return &producerNode[T]{
		client:            client,
		metrics:           metrics,
		commitClient:      commitClient,
		source:            source,
		errorChannel:      errorChannel,
		id:                id,
		commitLog:         commitLog,
		shouldMarkCommit:  source.shouldMarkCommit(),
		currentPartitions: make(map[int32]eventContextDll[T]),
		partitionOwners:   partitionOwners,
		txnErrorHandler:   source.eosErrorHandler(),
		recordsToProduce:  pendingRecordPool.Borrow(),
	}
}

func (p *producerNode[T]) addEventContext(ec *EventContext[T]) bool {
	p.commitWaiter.Lock()
	defer p.commitWaiter.Unlock()

	startTxn := false
	p.partitionLock.Lock()
	if p.eventContextCnt == 0 {
		p.firstEvent = time.Now()
		startTxn = true
	}
	p.eventContextCnt++
	partition := ec.partition()
	// we'll use a linked list in reverse order, since we want the larget offset anyway
	if dll, ok := p.currentPartitions[partition]; ok {
		ec.prev = dll.tail
		dll.tail.next = ec
		dll.tail = ec
		// we're not using a ptr, so be sure to set the value
		p.currentPartitions[partition] = dll
	} else {
		ec.revocationWaiter.Add(1)
		p.currentPartitions[partition] = eventContextDll[T]{
			root: ec,
			tail: ec,
		}
	}
	p.partitionLock.Unlock()
	return startTxn
}

func (p *producerNode[T]) beginTransaction() {
	if err := p.client.BeginTransaction(); err != nil {
		log.Errorf("could not begin txn err: %v", err)
		select {
		case p.errorChannel <- err:
		default:
		}
	}
}

func (p *producerNode[T]) finalizeEventContexts(first, last *EventContext[T]) error {
	commitRecordProduced := false
	// we'll now iterate in reverse order - committing the largest offset
	for ec := last; ec != nil; ec = ec.prev {
		select {
		case <-ec.done:
		case <-p.txnContext.Done():
			return fmt.Errorf("txn timeout exceeded. waiting for event context to finish: %+v", ec)
		}
		offset := ec.Offset()
		// if less than 0, this is an interjection, no record to commit
		if !commitRecordProduced && offset >= 0 {
			// we only want to produce the highest offset, since these are in reverse order
			// produce a commit record for the first real offset we see
			commitRecordProduced = true
			crd := p.commitLog.commitRecord(ec.TopicPartition(), offset)
			p.ProduceRecord(ec, crd, nil)
		}
		ec.revocationWaiter.Done()
	}
	return nil
}

func (p *producerNode[T]) commit() error {
	commitStart := time.Now()
	p.commitWaiter.Lock()
	defer p.commitWaiter.Unlock()

	p.produceLock.Lock()
	p.flushRemaining()
	for tp := range p.currentPartitions {
		p.partitionOwners.set(tp, p)
	}
	p.produceLock.Unlock()
	for _, dll := range p.currentPartitions {
		if err := p.finalizeEventContexts(dll.root, dll.tail); err != nil {
			log.Errorf("eos finalization error: %v", err)
			return err
		}
	}
	err := p.client.Flush(p.txnContext)
	if err != nil {
		log.Errorf("eos producer error: %v", err)
		return err
	}
	action := kgo.TryCommit
	if p.produceCnt == 0 {
		action = kgo.TryAbort
	}
	err = p.client.EndTransaction(p.txnContext, action)
	if err != nil {
		log.Errorf("eos producer txn error: %v", err)
		return err
	}
	p.clearState(commitStart)
	return nil
}

func (p *producerNode[T]) clearState(executionTime time.Time) {
	p.txnContextCancel()
	p.txnContext = nil
	for _, ecs := range p.currentPartitions {
		ecs.root.revocationWaiter.Done()
		if p.shouldMarkCommit {
			for ec := ecs.tail; ec != nil; ec = ec.prev {
				if !ec.IsInterjection() {
					p.commitClient.MarkCommitRecords(&ec.input.kRecord)
				}
			}
		}
	}

	partitionCount := len(p.currentPartitions)
	p.relinquishOwnership()
	if p.metrics != nil && p.produceCnt > 0 {
		p.metrics <- Metric{
			Operation:      TxnCommitOperation,
			Topic:          p.source.Topic(),
			GroupId:        p.source.GroupId(),
			StartTime:      p.firstEvent,
			ExecuteTime:    executionTime,
			EndTime:        time.Now(),
			Count:          int(p.produceCnt),
			Bytes:          int(p.byteCount),
			PartitionCount: partitionCount,
			Partition:      -1,
		}
	}
	p.produceCnt = 0
	p.eventContextCnt = 0
	p.byteCount = 0

	pendingRecordPool.Release(p.recordsToProduce)
	p.recordsToProduce = pendingRecordPool.Borrow()
}

/*
relinquishOwnership is needed to maintain produce ordering. If we have more than 2 producerNodes, we can not simply relinquich control
of partitions as the onDeck producerNode may take ownership while the pending produceNode may have events for the same partition.
In this case, the ondeck producerNode would get priority over the pending, which would break event ordering.
Since we round robin through producerNodes, we can iterate through them in order and check to see if the producerNode
has any events for the partition in question.
If so, transfer ownership immediately which will open up production for pending events.
While this does create lock contention, it allows us to produce concurrently accross nodes. It also allows us to start producing records *before*
we begin the commit process.
*/
func (p *producerNode[T]) relinquishOwnership() {
	for nextNode := p.next; nextNode != p; nextNode = nextNode.next {
		nextNode.partitionLock.RLock()
	}
	for tp := range p.currentPartitions {
		wasTransfered := false
		for nextNode := p.next; nextNode != p; nextNode = nextNode.next {
			nextNode.produceLock.Lock()
			if _, ok := nextNode.currentPartitions[tp]; ok {
				nextNode.flushPartition(tp)
				p.partitionOwners.set(tp, nextNode)
				nextNode.produceLock.Unlock()
				wasTransfered = true
				break
			}
			nextNode.produceLock.Unlock()
		}
		if !wasTransfered {
			p.partitionOwners.clear(tp)
		}
		delete(p.currentPartitions, tp)
	}
	for nextNode := p.next; nextNode != p; nextNode = nextNode.next {
		nextNode.partitionLock.RUnlock()
	}
}

func (p *producerNode[T]) flushPartition(partition int32) {
	newPends := pendingRecordPool.Borrow()
	for _, pending := range p.recordsToProduce {
		if pending.partition != partition {
			newPends = append(newPends, pending)
			continue
		}
		p.produceKafkaRecord(pending.record, pending.cb)
	}
	pendingRecordPool.Release(p.recordsToProduce)
	p.recordsToProduce = newPends
}

func (p *producerNode[T]) flushRemaining() {
	if p.txnContext == nil {
		// this is the first record produced, let's start our context with timeout now
		// for now, setting it to txn timeout - 1 second
		p.txnContext, p.txnContextCancel = context.WithTimeout(context.Background(), 29*time.Second)
		p.beginTransaction()
	}
	for _, pending := range p.recordsToProduce {
		p.produceKafkaRecord(pending.record, pending.cb)
	}
}

func (p *producerNode[T]) ProduceRecord(ec *EventContext[T], record *Record, cb func(*Record, error)) {
	p.produceLock.Lock()
	p.produceCnt++
	// set the timestamp if not set
	// we want to capture any time that this record spends in the recordsToProduce buffer
	if record.kRecord.Timestamp.IsZero() {
		record.kRecord.Timestamp = time.Now()
	}
	if p.partitionOwners.owned(ec.partition(), p) {
		p.produceKafkaRecord(record, cb)
	} else {
		p.recordsToProduce = append(p.recordsToProduce, pendingRecord{
			record:    record,
			partition: ec.partition(),
			cb:        cb,
		})
	}
	p.produceLock.Unlock()
}

func (p *producerNode[T]) produceKafkaRecord(record *Record, cb func(*Record, error)) {
	if p.txnContext == nil {
		// this is the first record produced, let's start our context with timeout now
		// for now, setting it to txn timeout - 1 second
		p.txnContext, p.txnContextCancel = context.WithTimeout(context.Background(), 29*time.Second)
		p.beginTransaction()
	}
	p.client.Produce(p.txnContext, record.ToKafkaRecord(), func(r *kgo.Record, err error) {
		record.kRecord = *r
		atomic.AddInt64(&p.byteCount, int64(recordSize(*r)))
		if err != nil {
			log.Errorf("%v, record %+v", err, r)
			select {
			case p.errorChannel <- err:
			default:
			}
		}
		if cb != nil {
			cb(record, err)
		}
		record.Release()
	})
}
