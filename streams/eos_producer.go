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

// a container that allows to know who produced the records
// needed for txn error conditions while there are revoked partitions
// allows us to filter recordsToProduce on a retry and exclude partitions that have been revoked
type recordAndEventContext[T any] struct {
	record       *Record
	eventContext *EventContext[T]
}

type eosProducerPool[T any] struct {
	producerNodeQueue chan *producerNode[T]
	onDeck            *producerNode[T]
	commitQueue       chan *producerNode[T]
	signal            chan struct{}
	cfg               EosConfig
	producerNodes     []*producerNode[T]
	flushTimer        *time.Ticker
	root              *EventContext[T]
	tail              *EventContext[T]
	sllLock           sync.Mutex // lock for out singly linked list of EventContexts
	startTime         time.Time
	cluster           Cluster
}

func newEOSProducerPool[T StateStore](cluster Cluster, commitLog *eosCommitLog, cfg EosConfig) *eosProducerPool[T] {
	if cfg.IsZero() {
		cfg = DefaultEosConfig
	}
	cfg.validate()
	pp := &eosProducerPool[T]{
		cfg:               DefaultEosConfig,
		producerNodeQueue: make(chan *producerNode[T], cfg.PoolSize),
		commitQueue:       make(chan *producerNode[T], cfg.PendingTxnCount),
		signal:            make(chan struct{}, 1),
		producerNodes:     make([]*producerNode[T], 0, cfg.PoolSize),
		flushTimer:        time.NewTicker(noPendingDuration),
		cluster:           cluster,
	}
	for i := 0; i < cfg.PoolSize; i++ {
		p := newProducerNode[T](cluster, commitLog)
		pp.producerNodes = append(pp.producerNodes, p)
		pp.producerNodeQueue <- p
	}
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
	pp.sllLock.Lock()
	if pp.root == nil {
		pp.root, pp.tail = ec, ec
	} else {
		pp.tail.next = ec
		pp.tail = ec
	}
	pp.sllLock.Unlock()
	select {
	case pp.signal <- struct{}{}:
	default:
	}
}

func (pp *eosProducerPool[T]) doForwardExecutionContexts() {
	for {
		pp.sllLock.Lock()
		ec := pp.root
		if ec == nil {
			pp.sllLock.Unlock()
			return
		}
		pp.root = ec.next
		ec.next = nil
		pp.sllLock.Unlock()

		// off to the races
		if ec.trySetProducer(pp.onDeck) {
			if len(pp.onDeck.eventContexts) > pp.cfg.MinBatchSize {
				pp.tryFlush()
			} else if len(pp.onDeck.eventContexts) == 1 {
				pp.flushTimer.Reset(pp.cfg.BatchDelay)
			}
		}
	}
}

func (pp *eosProducerPool[T]) forwardExecutionContexts() {
	for {
		select {
		case <-pp.signal:
			pp.doForwardExecutionContexts()
		case <-pp.flushTimer.C:
			pp.tryFlush()
		}

	}
}

func (pp *eosProducerPool[T]) shouldForceFlush() bool {
	return sak.Max(len(pp.onDeck.eventContexts), len(pp.onDeck.recordsToProduce)) == pp.cfg.MaxBatchSize
}

func (pp *eosProducerPool[T]) tryFlush() {
	if pp.shouldForceFlush() {
		// the onDeck producer is full
		// force a swap, blocking until successful
		pp.commitQueue <- pp.onDeck
		pp.onDeck = <-pp.producerNodeQueue
	} else if len(pp.onDeck.eventContexts) > 0 {
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
		log.Tracef("committed %d executions in: %v", len(p.eventContexts), sincer{p.firstEvent})
	}
}

type producerNode[T any] struct {
	client                *kgo.Client
	commitLog             *eosCommitLog
	recordsToProduce      []recordAndEventContext[T]
	eventContexts         []*EventContext[T]
	currentTopicParitions map[TopicPartition]*EventContext[T]
	commitWaiter          sync.WaitGroup
	partitionLock         sync.RWMutex
	produceLock           sync.Mutex
	firstEvent            time.Time
	commiting             bool
	// errs                  []error
}

func newProducerNode[T StateStore](cluster Cluster, commitLog *eosCommitLog) *producerNode[T] {
	client, err := NewClient(
		cluster,
		kgo.RecordPartitioner(NewOptionalPartitioner(kgo.StickyKeyPartitioner(nil))),
		kgo.TransactionalID(uuid.NewString()),
		kgo.TransactionTimeout(6*time.Second),
	)
	if err != nil {
		panic(err)
	}
	return &producerNode[T]{client: client,
		commitLog:             commitLog,
		currentTopicParitions: make(map[TopicPartition]*EventContext[T]),
	}
}

// if `TopicPartition` is currently being tracked by this producerNode, blocks until the current transaction succeeds or fails.
// otherwise, returns immediately.
func (p *producerNode[T]) revokePartition(tp TopicPartition) {
	// we should just be able to wait for any pending commits to flush
	// and that should ensure that any pending events for this topic partition
	// should be flushed
	p.partitionLock.RLock()
	_, hasPartition := p.currentTopicParitions[tp]
	p.partitionLock.RUnlock()

	if hasPartition {
		p.commitWaiter.Wait()
	}
}

func (p *producerNode[T]) addEventContext(ec *EventContext[T]) {
	// if ec is an Interjection, offset will be -1
	offset := ec.Offset()

	p.partitionLock.Lock()
	if offset > 0 {
		p.currentTopicParitions[ec.TopicPartition()] = ec
	} else if _, ok := p.currentTopicParitions[ec.TopicPartition()]; !ok {
		// we don't want to override an actual offset with a -1
		// so only set this if there are no other offsets being tracked
		p.currentTopicParitions[ec.TopicPartition()] = ec
	}
	p.partitionLock.Unlock()

	if len(p.eventContexts) == 0 {
		p.commitWaiter.Add(1)
		if err := p.client.BeginTransaction(); err != nil {
			log.Errorf("txn err: %v", err)
		}
		p.firstEvent = time.Now()
	}
	p.eventContexts = append(p.eventContexts, ec)
}

func (p *producerNode[T]) commit() error {

	p.produceLock.Lock()
	p.commiting = true
	for _, rtp := range p.recordsToProduce {
		p.doProduceRecord(rtp.eventContext, rtp.record)
	}
	p.produceLock.Unlock()

	for _, ec := range p.eventContexts {
		ec.waitUntilComplete()
	}
	ctx, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	p.partitionLock.RLock()
	for tp, ec := range p.currentTopicParitions {
		offset := ec.Offset()
		if offset > 0 {
			crd := p.commitLog.commitRecord(tp, offset)
			p.produceRecord(ec, crd)
		}
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
	}
	if err == nil {
		p.clearState()
	} else {
		p.clearRevokedState()
		log.Errorf("commit error: %v", err)
	}
	p.commiting = false
	p.commitWaiter.Done()
	return err
}

func (p *producerNode[T]) clearState() {
	for i := range p.eventContexts {
		p.eventContexts[i] = nil
	}
	for i := range p.recordsToProduce {
		p.recordsToProduce[i] = recordAndEventContext[T]{}
	}
	p.partitionLock.Lock()
	for tp := range p.currentTopicParitions {
		delete(p.currentTopicParitions, tp)
	}
	p.partitionLock.Unlock()
	p.eventContexts = p.eventContexts[0:0]
	p.recordsToProduce = p.recordsToProduce[0:0]
}

func (p *producerNode[T]) clearRevokedState() {
	// we're in an error state in our eosProducer
	// we may retry the commit, but if any partitions were revoked
	// we do not want to retry them. So let's remove them from internal state

	// this may require a lot of allocations, so let's check to see if there are any revocations first
	// we'll end up looping through p.eventContexts twice, but it's better not to reallocate
	// if not necessary, so this is likely more efficient most of the time
	revokedPartitions := make(map[TopicPartition]struct{})
	p.partitionLock.Lock()
	for i, ec := range p.eventContexts {
		if ec.isRevoked() {
			revokedPartitions[ec.TopicPartition()] = struct{}{}
			delete(p.currentTopicParitions, ec.TopicPartition())
			p.eventContexts[i] = nil
		}
	}
	p.partitionLock.Unlock()

	if len(revokedPartitions) > 0 {
		// we have revocations, remove them from recordsToProduce so if we retry, we are not breaking eos
		// also ensure we remove any nil eventContexts from the previous step
		validRecords := make([]recordAndEventContext[T], 0, len(p.recordsToProduce))
		for _, record := range p.recordsToProduce {
			if _, ok := revokedPartitions[record.eventContext.TopicPartition()]; !ok {
				validRecords = append(validRecords, record)
			}
		}

		validEvents := make([]*EventContext[T], 0, len(p.eventContexts))
		// we previously nulled out invalid events, just create a new slice without them
		for _, ec := range p.eventContexts {
			if ec != nil {
				validEvents = append(validEvents, ec)
			}
		}

		p.recordsToProduce = validRecords
		p.eventContexts = validEvents
	}
}

func (p *producerNode[T]) produceRecord(ec *EventContext[T], record *Record) {
	p.produceLock.Lock()
	p.recordsToProduce = append(p.recordsToProduce, recordAndEventContext[T]{
		record:       record,
		eventContext: ec,
	})
	shouldProduce := p.commiting
	p.produceLock.Unlock()

	if shouldProduce {
		p.doProduceRecord(ec, record)
	}
}

func (p *producerNode[T]) doProduceRecord(ec *EventContext[T], record *Record) {
	p.client.Produce(context.TODO(), record.ToKafkaRecord(), func(r *kgo.Record, err error) {
		if err != nil {
			// TODO: proper way to handle producer errors
			// p.errs = append(p.errs, err)
			log.Errorf("%v, record %v", err, r)
		}
		record.release()
	})
}
