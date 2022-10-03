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
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Contains information about the current event. Is passed to EventProcessors and Interjections
type EventContext[T any] struct {
	// we're going to keep a reference to the partition worker context
	// so we can skip over any buffered events in the EOSProducer
	ctx              context.Context
	producerChan     chan *producerNode[T]
	producer         *producerNode[T]
	revocationWaiter *sync.WaitGroup
	next             *EventContext[T]
	prev             *EventContext[T]
	input            IncomingRecord
	asynCompleter    asyncCompleter[T]
	changeLog        changeLogData[T]
	done             chan struct{}
	topicPartition   TopicPartition
	interjection     *interjection[T]
	isInterjection   bool
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

// The parition for this event. It is present for both normal events and Interjections
func (ec *EventContext[T]) partition() int32 {
	return ec.topicPartition.Partition
}

// Forwards records to the transactional producer for your EventSource.
func (ec *EventContext[T]) Forward(records ...*Record) {
	for _, record := range records {
		ec.producer.produceRecord(ec, record, nil)
	}
}

// Forwards records to the transactional producer for your EventSource. When you add an item to your StateStore,
// you must call this method for that change to be recorded in the stream. This ensures that when the TopicPartition for this change
// is tansferred to a new consumer, it will also have this change.
func (ec *EventContext[T]) RecordChange(entries ...ChangeLogEntry) {
	for _, entry := range entries {
		if len(ec.changeLog.topic) > 0 {
			log.Tracef("RecordChange changeLogTopic: %s, topicPartition: %+v, value: %v", ec.changeLog.topic, ec.topicPartition, entry)
			record := entry.record.
				WithTopic(ec.changeLog.topic).
				WithPartition(ec.topicPartition.Partition)

			ec.producer.produceRecord(ec, record, nil)
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
	ec.asynCompleter.asyncComplete(asyncJob[T]{
		ctx:      ec,
		finalize: finalize,
	})
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
	// ec.wg.Done()
	close(ec.done)
}

func newEventContext[T StateStore](ctx context.Context, record *kgo.Record, changeLog changeLogData[T], pw *partitionWorker[T]) *EventContext[T] {
	input := newIncomingRecord(record)
	ec := &EventContext[T]{
		ctx:              ctx,
		producerChan:     make(chan *producerNode[T], 1),
		topicPartition:   input.TopicPartition(),
		changeLog:        changeLog,
		input:            input,
		isInterjection:   false,
		asynCompleter:    pw.asyncCompleter,
		revocationWaiter: (*sync.WaitGroup)(sak.Noescape(unsafe.Pointer(&pw.revocationWaiter))),
		done:             make(chan struct{}),
		// pendingRecords: pendingRecordPool.Borrow(),
	}
	return ec
}

func newInterjectionContext[T StateStore](ctx context.Context, interjection *interjection[T], topicPartition TopicPartition, changeLog changeLogData[T], pw *partitionWorker[T]) *EventContext[T] {
	ec := &EventContext[T]{
		ctx:              ctx,
		producerChan:     make(chan *producerNode[T], 1),
		topicPartition:   topicPartition,
		interjection:     interjection,
		isInterjection:   true,
		changeLog:        changeLog,
		asynCompleter:    pw.asyncCompleter,
		revocationWaiter: (*sync.WaitGroup)(sak.Noescape(unsafe.Pointer(&pw.revocationWaiter))),
		done:             make(chan struct{}),
		// pendingRecords: pendingRecordPool.Borrow(),
	}
	return ec
}
