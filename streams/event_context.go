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

	"github.com/twmb/franz-go/pkg/kgo"
)

// Contains information about the current event. Is passed to EventProcessors and Interjections
type EventContext[T any] struct {
	// we're going to keep a reference to the partition worker context
	// so we can skip over any buffered events in the EOSProducer
	ctx            context.Context
	producer       *producerNode[T]
	changeLog      *changeLogData[T]
	next           *EventContext[T]
	pendingRecords []recordContainer
	input          IncomingRecord
	asynCompleter  asyncCompleter[T]
	produceLock    sync.Mutex
	wg             sync.WaitGroup
	topicPartition TopicPartition
	isInterjection bool
	rejected       bool
}

func (ec *EventContext[T]) waitUntilComplete() {
	ec.wg.Wait()
}

func (ec *EventContext[T]) isRevoked() bool {
	return ec.ctx.Err() != nil
}

// Returns true if this EventContext represents an Interjection
func (ec *EventContext[T]) IsInterjection() bool {
	return ec.isInterjection
}

// The offset for this event, -1 for an Interjection
func (ec *EventContext[T]) Offset() int64 {
	if ec.isInterjection {
		return -1
	}
	return ec.input.Offset()
}

// The TopicParition for this event. It is present for both normal events and Interjections
func (ec *EventContext[T]) TopicPartition() TopicPartition {
	return ec.topicPartition
}

// Forwards records to the transactional producer for your EventSource.
func (ec *EventContext[T]) Forward(records ...*Record) {
	ec.produceLock.Lock()
	defer ec.produceLock.Unlock()
	for _, record := range records {
		if ec.producer == nil {
			ec.pendingRecords = append(ec.pendingRecords, recordContainer{record, false})
		} else {
			ec.producer.produceRecord(ec, record)
		}
	}

}

func (ec *EventContext[T]) isRejected() bool {
	ec.produceLock.Lock()
	defer ec.produceLock.Unlock()
	return ec.rejected
}

// Forwards records to the transactional producer for your EventSource. When you add an item to your StateStore,
// you must call this method for that change to be recorded in the stream. This ensures that when the TopicPartition for this change
// is tansferred to a new consumer, it will also have this change.
func (ec *EventContext[T]) RecordChange(entries ...ChangeLogEntry) {
	ec.produceLock.Lock()
	defer ec.produceLock.Unlock()
	for _, entry := range entries {
		if ec.changeLog != nil {
			log.Tracef("RecordChange changeLogTopic: %s, topicPartition: %+v, value: %v", ec.changeLog.topic, ec.topicPartition, entry)
			record := entry.record.
				WithTopic(ec.changeLog.topic).
				WithPartition(ec.topicPartition.Partition)
			if ec.producer != nil {
				ec.producer.produceRecord(ec, record)
			} else {
				ec.pendingRecords = append(ec.pendingRecords, recordContainer{record, true})
			}
		} else {
			log.Warnf("EventContext.RecordChange was called but consumer is not stateful")
		}
	}
}

// AsyncJobComplete should be called when an async event processor has performed it's function.
// the finalize cunction should return Complete if there are no other pending asynchronous jobs for the event context in question,
// regardless of error state. `finalize` does no accept any arguments, so you're callback should encapsulate
// any pertinent data needed for processing. See [streams.AsyncJobScheduler] for an example.
func (ec *EventContext[T]) AsyncJobComplete(finalize func() (ExecutionState, error)) {
	if ec.isRejected() {
		// don't bother trying to complete this task. It will only cause confusion
		return
	}
	ec.asynCompleter.asyncComplete(asyncJob[T]{
		ctx:      ec,
		finalize: finalize,
	})
}

func (ec *EventContext[T]) trySetProducer(p *producerNode[T]) bool {
	ec.produceLock.Lock()
	defer ec.produceLock.Unlock()
	if ec.isRevoked() {
		// this event was buffered in the eosProducer, but the partition has since been revoked
		// let's drop this so as not to add to rebalance latency
		ec.rejected = true
		return false
	}

	ec.producer = p
	p.addEventContext(ec)
	for _, cont := range ec.pendingRecords {
		ec.producer.produceRecord(ec, cont.record)
	}
	ec.pendingRecords = []recordContainer{}
	return true
}

// Return the raw input record for this event or an uninitialized record and false if the EventContect represents an Interjections
func (ec *EventContext[T]) Input() (IncomingRecord, bool) {
	return ec.input, !ec.isInterjection
}

// Returns the StateStore for this event/TopicPartition
func (ec *EventContext[T]) Store() T {
	return ec.changeLog.store
}

func (ec *EventContext[T]) complete() {
	ec.wg.Done()
}

type recordContainer struct {
	record   *Record
	reusable bool
}

func newEventContext[T StateStore](ctx context.Context, record *kgo.Record, changeLog *changeLogData[T], pw *partitionWorker[T]) *EventContext[T] {
	input := newIncomingRecord(record)
	ec := &EventContext[T]{
		ctx:            ctx,
		topicPartition: input.TopicPartition(),
		changeLog:      changeLog,
		input:          input,
		isInterjection: false,
		asynCompleter:  pw.asyncCompleter,
	}
	ec.wg.Add(1)
	return ec
}

func newInterjectionContext[T any](ctx context.Context, topicPartition TopicPartition, changeLog *changeLogData[T], ac asyncCompleter[T]) *EventContext[T] {
	ec := &EventContext[T]{
		ctx:            ctx,
		topicPartition: topicPartition,
		isInterjection: true,
		changeLog:      changeLog,
		asynCompleter:  ac,
	}
	ec.wg.Add(1)
	return ec
}
