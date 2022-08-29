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
	revokeLock        sync.RWMutex
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
		p := newProducerNode[T](cluster, commitLog, &pp.revokeLock)
		pp.producerNodes = append(pp.producerNodes, p)
		pp.producerNodeQueue <- p
	}
	pp.onDeck = <-pp.producerNodeQueue
	go pp.forwardExecutionContexts()
	go pp.commitLoop()
	return pp
}

func (pp *eosProducerPool[T]) revokePartitions(tps []TopicPartition) {
	pp.revokeLock.Lock()
	defer pp.revokeLock.Unlock()
	for _, p := range pp.producerNodes {
		p.revokePartitions(tps)
	}
}

func (pp *eosProducerPool[T]) addEventContext(ec *EventContext[T]) {
	if ec.Ctx().Err() != nil {
		return
	}
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
		pp.onDeck.addEventContext(ec)
		ec.setProducer(pp.onDeck)
		if len(pp.onDeck.eventContexts) > 100 {
			pp.tryFlush()
		} else if len(pp.onDeck.eventContexts) == 1 {
			pp.flushTimer.Reset(pendingDuration)
		}
	}
}

func (pp *eosProducerPool[T]) forwardExecutionContexts() {
	for {
		select {
		case <-pp.signal:
			pp.revokeLock.RLock()
			pp.doForwardExecutionContexts()
			pp.revokeLock.RUnlock()
		case <-pp.flushTimer.C:
			pp.revokeLock.RLock()
			pp.tryFlush()
			pp.revokeLock.RUnlock()
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
	revokeLock            *sync.RWMutex
	commitLog             *eosCommitLog
	recordsToProduce      []*Record
	eventContexts         []*EventContext[T]
	currentTopicParitions map[TopicPartition]int64
	errs                  []error
	transacting           bool
	firstEvent            time.Time
	mux                   sync.Mutex
}

func newProducerNode[T StateStore](cluster Cluster, commitLog *eosCommitLog, revokeLock *sync.RWMutex) *producerNode[T] {
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
		revokeLock:            revokeLock,
	}
}

func (p *producerNode[T]) revokePartitions(tps []TopicPartition) {
	// TODO: when a partion is revoked, if the node is not currently commiting, revoke and re-produce withour revoked partition
}

func (p *producerNode[T]) addEventContext(ec *EventContext[T]) {
	p.revokeLock.RLock()
	defer p.revokeLock.RUnlock()

	if !ec.IsInterjection() {
		// interjections have no offset, but they may produce changes that need to be produced
		p.currentTopicParitions[ec.TopicPartition()] = ec.Offset()
	}
	if len(p.eventContexts) == 0 {
		if err := p.client.BeginTransaction(); err != nil {
			log.Errorf("txn err: %v", err)
		}
		p.transacting = true
		p.firstEvent = time.Now()
	}
	p.eventContexts = append(p.eventContexts, ec)
}

func (p *producerNode[T]) commit() error {
	p.revokeLock.RLock()
	for _, ec := range p.eventContexts {
		ec.Wait()
	}
	ctx, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	for tp, offset := range p.currentTopicParitions {
		crd := p.commitLog.commitRecord(tp, offset)
		p.produceRecord(ctx, crd, nil)
	}
	err := p.client.Flush(ctx)
	cancelFlush()
	if err != nil {
		log.Errorf("eos producer error: %v", err)
	}
	err = p.client.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		log.Errorf("eos producer txn error: %v", err)
	}
	p.transacting = false
	p.revokeLock.RUnlock()
	if err == nil {
		p.clearState()
	} else {
		log.Errorf("commit error: %v", err)
	}
	return err
}

func (p *producerNode[T]) clearState() {
	for i := range p.eventContexts {
		p.eventContexts[i] = nil
	}
	for i := range p.recordsToProduce {
		p.recordsToProduce[i] = nil
	}
	for tp := range p.currentTopicParitions {
		delete(p.currentTopicParitions, tp)
	}
	p.eventContexts = p.eventContexts[0:0]
	p.recordsToProduce = p.recordsToProduce[0:0]
}

func (p *producerNode[T]) produceRecord(ctx context.Context, record *Record, syncWaiter *sync.WaitGroup) {
	p.revokeLock.RLock()
	defer p.revokeLock.RUnlock()
	p.recordsToProduce = append(p.recordsToProduce, record)
	if ctx.Err() == nil {
		p.client.Produce(ctx, record.ToKafkaRecord(), func(r *kgo.Record, err error) {
			if err != nil {
				p.mux.Lock()
				defer p.mux.Unlock()
				p.errs = append(p.errs, err)
				log.Errorf("%v, record %v", err, r)
			}
			if syncWaiter != nil {
				syncWaiter.Done()
			}
			record.release()

		})
	} else if syncWaiter != nil {
		syncWaiter.Done()
	}
}
