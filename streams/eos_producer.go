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
	"sync"
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

func (po partitionOwners[T]) set(p int32, pn *producerNode[T]) {
	po.mux.Lock()
	po.owners[p] = pn
	po.mux.Unlock()
}

func (po partitionOwners[T]) conditionallyUpdate(p int32, pn *producerNode[T]) (wasNil bool, wasEqual bool) {
	po.mux.Lock()
	if existing, ok := po.owners[p]; !ok {
		wasNil = true
		po.owners[p] = pn
	} else {
		wasEqual = existing == pn
	}
	po.mux.Unlock()
	return
}

func (po partitionOwners[T]) clear(p int32) {
	po.mux.Lock()
	delete(po.owners, p)
	po.mux.Unlock()
}

// a container that allows to know who produced the records
// needed for txn error conditions while there are revoked partitions
// allows us to filter recordsToProduce on a retry and exclude partitions that have been revoked
type recordAndEventContext[T any] struct {
	record       *Record
	eventContext *EventContext[T]
}

type eosProducerPool[T StateStore] struct {
	producerNodeQueue chan *producerNode[T]
	onDeck            *producerNode[T]
	commitQueue       chan *producerNode[T]
	buffer            chan *EventContext[T]
	cfg               EosConfig
	producerNodes     []*producerNode[T]
	flushTimer        *time.Ticker
	partitionOwners   partitionOwners[T]
	startTime         time.Time
	// cluster           Cluster
}

func newEOSProducerPool[T StateStore](source *Source, commitLog *eosCommitLog, cfg EosConfig, commitClient *kgo.Client, metrics chan Metric) *eosProducerPool[T] {
	pp := &eosProducerPool[T]{
		cfg:               DefaultEosConfig,
		producerNodeQueue: make(chan *producerNode[T], cfg.PoolSize),
		commitQueue:       make(chan *producerNode[T], cfg.PendingTxnCount),
		buffer:            make(chan *EventContext[T], 1024),
		producerNodes:     make([]*producerNode[T], 0, cfg.PoolSize),
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
		p := newProducerNode(i, source, commitLog, pp.partitionOwners, commitClient, metrics)
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

// instruct the eosProducerPool that the partition has been revoked
// calls revokePartition on each producerNode in the pool
func (pp *eosProducerPool[T]) revokePartition(tp TopicPartition) {
	for _, p := range pp.producerNodes {
		p.revokePartition(tp)
	}
}

// buffer the event context until a producer node is available
func (pp *eosProducerPool[T]) addEventContext(ec *EventContext[T]) {
	pp.buffer <- ec
}

func (pp *eosProducerPool[T]) doForwardExecutionContexts(ec *EventContext[T]) {
	if ec.isRevoked() {
		// if we're revoked, don't even add this to the onDeck producer
		return
	}

	txnStarted := pp.onDeck.addEventContext(ec)
	if wasNil, wasEqual := pp.partitionOwners.conditionallyUpdate(ec.partition(), pp.onDeck); wasNil {
		// this partition is not owned by the committing producer, so it is safe to start producing.
		// update all of the event contexts for this partition only.
		// once setProducer is called, any buffered records for this event will now be sent to the kafka broker.
		pp.onDeck.setProducerFor(ec.partition())
	} else if wasEqual {
		// this was the correct producer, no need to update all and create an n^2 issue
		ec.setProducer(pp.onDeck)
	}
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
	return sak.Max(pp.onDeck.eventContextCnt, len(pp.onDeck.recordsToProduce)) >= pp.cfg.TargetBatchSize
}

func (pp *eosProducerPool[T]) shouldForceFlush() bool {
	return sak.Max(pp.onDeck.eventContextCnt, len(pp.onDeck.recordsToProduce)) == pp.cfg.MaxBatchSize
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
	for p := range pp.commitQueue {
		if pp.startTime.IsZero() {
			pp.startTime = time.Now()
		}
		if err := p.commit(); err != nil {
			log.Errorf("%v", err)
		}
		pp.producerNodeQueue <- p
		log.Tracef("committed %d executions in: %v", pp.onDeck.eventContextCnt, sincer{p.firstEvent})
	}
}

type eventContextDll[T any] struct {
	root, tail *EventContext[T]
}

type producerNode[T any] struct {
	client           *kgo.Client
	commitClient     *kgo.Client
	commitLog        *eosCommitLog
	next             *producerNode[T]
	metrics          chan Metric
	source           *Source
	recordsToProduce []recordAndEventContext[T]
	eventContextCnt  int
	currentParitions map[int32]eventContextDll[T]
	partitionOwners  partitionOwners[T]
	shouldMarkCommit bool
	commitWaiter     sync.Mutex // this is a mutex masquerading as a WaitGroup
	partitionLock    sync.RWMutex
	produceLock      sync.Mutex
	firstEvent       time.Time
	id               int
	eosErrorHandler  EosErrorHandler
	// errs                  []error
}

func newProducerNode[T StateStore](id int, source *Source, commitLog *eosCommitLog, partitionOwners partitionOwners[T], commitClient *kgo.Client, metrics chan Metric) *producerNode[T] {
	client, err := NewClient(
		source.stateCluster(),
		kgo.RecordPartitioner(NewOptionalPartitioner(kgo.StickyKeyPartitioner(nil))),
		kgo.TransactionalID(uuid.NewString()),
		kgo.TransactionTimeout(6*time.Second),
	)
	if err != nil {
		panic(err)
	}
	return &producerNode[T]{
		client:           client,
		metrics:          metrics,
		commitClient:     commitClient,
		source:           source,
		id:               id,
		commitLog:        commitLog,
		shouldMarkCommit: source.shouldMarkCommit(),
		currentParitions: make(map[int32]eventContextDll[T]),
		partitionOwners:  partitionOwners,
		eosErrorHandler:  source.eosErrorHandler(),
	}
}

// if `TopicPartition` is currently being tracked by this producerNode, blocks until the current transaction succeeds or fails.
// otherwise, returns immediately.
func (p *producerNode[T]) revokePartition(tp TopicPartition) {
	// we should just be able to wait for any pending commits to flush
	// and that should ensure that any pending events for this topic partition
	// should be flushed
	p.partitionLock.RLock()
	dll, hasPartition := p.currentParitions[tp.Partition]
	mustProduce := false
	for ec := dll.root; ec != nil; ec = ec.next {
		if ec.includeInTxn() {
			mustProduce = true
			break
		}
	}
	p.partitionLock.RUnlock()

	if hasPartition && mustProduce {
		log.Debugf("waiting for producerNode: %d to commit before revoking %+v", p.id, tp)
		p.commitWaiter.Lock()
		defer p.commitWaiter.Unlock()
	}
	log.Debugf("revoked %+v from producerNode: %d", tp, p.id)
}

func (p *producerNode[T]) addEventContext(ec *EventContext[T]) bool {
	// if ec is an Interjection, offset will be -1
	startTxn := false
	p.partitionLock.Lock()

	if p.eventContextCnt == 0 {
		// we will commit something, block revocations on ownded partitions until we're done
		p.commitWaiter.Lock()
		p.firstEvent = time.Now()
		startTxn = true
	}

	partition := ec.partition()
	// we'll use a linked list in reverse order, since we want the larget offset anyway
	if dll, ok := p.currentParitions[partition]; ok {
		ec.prev = dll.tail
		dll.tail.next = ec
		dll.tail = ec
		// we're not using a ptr, so be sure to set the value
		p.currentParitions[partition] = dll
	} else {
		p.currentParitions[partition] = eventContextDll[T]{
			root: ec,
			tail: ec,
		}
	}

	p.eventContextCnt++
	if startTxn {
		if err := p.client.BeginTransaction(); err != nil {
			log.Errorf("txn err: %v", err)
		}
	}
	p.partitionLock.Unlock()
	return startTxn
}

func (p *producerNode[T]) setProducerFor(partition int32) bool {
	if dll, ok := p.currentParitions[partition]; ok {
		for ec := dll.root; ec != nil; ec = ec.next {
			ec.setProducer(p)
		}
		return true
	}
	return false
}

func (p *producerNode[T]) finalizeEventContexts(first, last *EventContext[T]) {

	// `setProducer` should be call in ascending offset order in case the partition is revoked
	// during this call. This ensures that any event that are omitted will have larger offsets, ensuring order event processing
	for ec := first; ec != nil; ec = ec.next {
		// if the partition was owned by another producer we must give the event context
		// a chance to produce any buffered records
		ec.setProducer(p)
	}

	commitRecordProduced := false
	// we'll now iterate in reverse order - committing the largest offset
	for ec := last; ec != nil; ec = ec.prev {
		if !ec.includeInTxn() {
			// this partition has been revoked before it has produced anything
			// it's safe to skip
			continue
		}

		offset := ec.Offset()
		// if less than 0, this is an interjection, no record to commit
		if !commitRecordProduced && offset >= 0 {
			// we only want to produce the highest offset, since these are in reverse order
			// produce a commit record for the first real offset we see
			commitRecordProduced = true
			crd := p.commitLog.commitRecord(ec.TopicPartition(), offset)
			p.produceRecord(ec, crd)
		}
		ec.waitUntilComplete()
	}

}

func (p *producerNode[T]) commit() error {
	commitStart := time.Now()
	defer p.commitWaiter.Unlock()
	for tp := range p.currentParitions {
		p.partitionOwners.set(tp, p)
	}
	ctx, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	p.partitionLock.RLock()
	for _, dll := range p.currentParitions {
		p.finalizeEventContexts(dll.root, dll.tail)
	}
	p.partitionLock.RUnlock()
	err := p.client.Flush(ctx)
	cancelFlush()
	if err != nil {
		log.Errorf("eos producer error: %v", err)
	}
	err = p.client.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		log.Errorf("eos producer txn error: %v", err)
		p.clearRevokedState()
	} else {
		p.clearState(commitStart)
	}
	return err
}

func (p *producerNode[T]) clearState(executionTime time.Time) {
	if p.shouldMarkCommit {
		for _, ecs := range p.currentParitions {
			for ec := ecs.tail; ec != nil; ec = ec.prev {
				if !ec.IsInterjection() {
					p.commitClient.MarkCommitRecords(&ec.input.kRecord)
				}
			}
		}
	}

	byteCount := 0
	p.eventContextCnt = 0
	var empty recordAndEventContext[T]
	for i, rtp := range p.recordsToProduce {
		byteCount += recordSize(rtp.record.kRecord)
		rtp.record.Release()
		rtp.eventContext.producer = nil
		rtp.eventContext.prev = nil
		rtp.eventContext.next = nil
		p.recordsToProduce[i] = empty
	}

	partitionCount := len(p.currentParitions)
	p.relinquishOwnership()
	if p.metrics != nil && len(p.recordsToProduce) > 0 {
		p.metrics <- Metric{
			Operation:      TxnCommitOperation,
			Topic:          p.source.Topic(),
			GroupId:        p.source.GroupId(),
			StartTime:      p.firstEvent,
			ExecuteTime:    executionTime,
			EndTime:        time.Now(),
			Count:          len(p.recordsToProduce),
			Bytes:          byteCount,
			PartitionCount: partitionCount,
			Partition:      -1,
		}
	}
	p.recordsToProduce = p.recordsToProduce[0:0]
}

/*
relinquishOwnership is needed to maintain produce ordering. If we have more than 2 producerNodes, we can not simply relinquich control
of partitions as the onDeck producerNode may take ownership while the pending produceNode may have events for the same partition.
In this case, the ondeck producerNode would get priority over the pending, which would break event ordering.
Since we round robin through producerNodes, we can iterate through them in order and check to see if the producerNode
has any events for the partition in question.
If so, transfer ownership immediately which will open up production for pending events.
While this does create lock contention, it allows us to produce concurrently accross nodes. It also allows us to start producing records *before*
we bgin the commit process.
*/
func (p *producerNode[T]) relinquishOwnership() {
	for tp := range p.currentParitions {
		wasTranfered := false
		for nextNode := p.next; nextNode != p; nextNode = nextNode.next {
			nextNode.partitionLock.RLock()
			if _, ok := nextNode.currentParitions[tp]; ok {
				p.partitionOwners.set(tp, nextNode)
				nextNode.setProducerFor(tp)
				nextNode.partitionLock.RUnlock()
				wasTranfered = true
				break
			}
			nextNode.partitionLock.RUnlock()
		}
		if !wasTranfered {
			p.partitionOwners.clear(tp)
		}
		delete(p.currentParitions, tp)
	}
}

func (p *producerNode[T]) clearRevokedState() {
	// we're in an error state in our eosProducer
	// we may retry the commit, but if any partitions were revoked
	// we do not want to retry them. So let's remove them from internal state

	revokedPartitions := make(map[int32]struct{})
	for partition, dll := range p.currentParitions {
		if dll.root.isRevoked() {
			revokedPartitions[partition] = struct{}{}
			for ec := dll.root; ec != nil; ec = ec.next {
				ec.abandon() // we're not going to retry these items, mark them so
			}
		}
	}
	if len(revokedPartitions) > 0 {
		// we have revocations, remove them from recordsToProduce so if we retry, we are not breaking eos
		validRecords := make([]recordAndEventContext[T], 0, len(p.recordsToProduce))
		for _, record := range p.recordsToProduce {
			if _, ok := revokedPartitions[record.eventContext.partition()]; !ok {
				validRecords = append(validRecords, record)
			}
		}
		p.recordsToProduce = validRecords
	}

}

func (p *producerNode[T]) produceRecord(ec *EventContext[T], record *Record) {
	p.produceLock.Lock()
	p.recordsToProduce = append(p.recordsToProduce, recordAndEventContext[T]{
		record:       record,
		eventContext: ec,
	})
	p.produceLock.Unlock()

	p.client.Produce(context.TODO(), record.ToKafkaRecord(), func(r *kgo.Record, err error) {
		if err != nil {
			// TODO: proper way to handle producer errors
			// p.errs = append(p.errs, err)
			log.Errorf("%v, record %v", err, r)
		}
	})
}
