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

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type eosProducerPool[T any] struct {
	producerNodeQueue chan *producerNode[T]
	onDeck            *producerNode[T]
	commitQueue       chan *producerNode[T]
	root              *EventContext[T]
	tail              *EventContext[T]
	signal            chan struct{}
	maxBatchSize      int
	minBatchSize      int
	producerNodes     []*producerNode[T]
	flushTimer        *time.Ticker
	startTime         time.Time
	cluster           Cluster
	sllLock           sync.Mutex
}

const (
	noPendingDuration = time.Minute
	pendingDuration   = 10 * time.Millisecond
)

func newEOSProducerPool[T StateStore](cluster Cluster, commitLog *eosCommitLog, poolSize, minBatchSize, maxBatchSize, pendingTxns int) *eosProducerPool[T] {
	pp := &eosProducerPool[T]{
		producerNodeQueue: make(chan *producerNode[T], poolSize),
		commitQueue:       make(chan *producerNode[T], pendingTxns),
		// input:        make(chan ecAddRequest[T, V], Max(maxBatchSize*Max(pendingTxns, 4), 1000)),
		minBatchSize:  minBatchSize,
		maxBatchSize:  maxBatchSize,
		signal:        make(chan struct{}, 1),
		producerNodes: make([]*producerNode[T], 0, poolSize),
		flushTimer:    time.NewTicker(noPendingDuration),
		cluster:       cluster,
	}
	for i := 0; i < poolSize; i++ {
		p := newProducerNode[T](cluster, commitLog)
		pp.producerNodes = append(pp.producerNodes, p)
		pp.producerNodeQueue <- p
	}
	pp.onDeck = <-pp.producerNodeQueue
	go pp.forwardExecutionContexts()
	go pp.commitLoop()
	return pp
}

func (pp *eosProducerPool[T]) revokePartition(tp TopicPartition) {
	for _, p := range pp.producerNodes {
		p.revokePartition(tp)
	}
}

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
			if len(pp.onDeck.eventContexts) > 100 {
				pp.tryFlush()
			} else if len(pp.onDeck.eventContexts) == 1 {
				pp.flushTimer.Reset(pendingDuration)
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
	// disabling this check for now as it causes a deadlock when using async event completions
	// return false

	return len(pp.onDeck.eventContexts) == pp.maxBatchSize || len(pp.onDeck.recordsToProduce) >= pp.maxBatchSize
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
			pp.flushTimer.Reset(pendingDuration)
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
	recordsToProduce      []*Record
	eventContexts         []*EventContext[T]
	currentTopicParitions map[TopicPartition]int64
	commitWaiter          sync.WaitGroup
	partitionLock         sync.RWMutex
	firstEvent            time.Time
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
		currentTopicParitions: make(map[TopicPartition]int64),
	}
}

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
		p.currentTopicParitions[ec.TopicPartition()] = offset
	} else if _, ok := p.currentTopicParitions[ec.TopicPartition()]; !ok {
		// we don't want to override an actual offset with a -1
		// so only set this if there are no other offsets being tracked
		p.currentTopicParitions[ec.TopicPartition()] = offset
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
	for _, ec := range p.eventContexts {
		ec.waitUntilComplete()
	}
	ctx, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	p.partitionLock.RLock()
	for tp, offset := range p.currentTopicParitions {
		if offset > 0 {
			crd := p.commitLog.commitRecord(tp, offset)
			p.produceRecord(ctx, crd)
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
		log.Errorf("commit error: %v", err)
	}
	p.commitWaiter.Done()
	return err
}

func (p *producerNode[T]) clearState() {
	for i := range p.eventContexts {
		p.eventContexts[i] = nil
	}
	for i := range p.recordsToProduce {
		p.recordsToProduce[i] = nil
	}
	p.partitionLock.Lock()
	for tp := range p.currentTopicParitions {
		delete(p.currentTopicParitions, tp)
	}
	p.partitionLock.Unlock()
	p.eventContexts = p.eventContexts[0:0]
	p.recordsToProduce = p.recordsToProduce[0:0]
}

func (p *producerNode[T]) produceRecord(ctx context.Context, record *Record) {
	p.recordsToProduce = append(p.recordsToProduce, record)
	if ctx.Err() == nil {
		p.client.Produce(ctx, record.ToKafkaRecord(), func(r *kgo.Record, err error) {
			if err != nil {
				// p.mux.Lock()
				// defer p.mux.Unlock()
				// TODO: proper way to handle producer errors
				// p.errs = append(p.errs, err)
				log.Errorf("%v, record %v", err, r)
			}
			record.release()
		})
	}
}
