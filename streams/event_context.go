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

type AsyncCompleter[T any] interface {
	AsyncComplete(AsyncJob[T])
}

type EventContextProducer[T any] interface {
	ProduceRecord(*EventContext[T], *Record, func(*Record, error))
}

// Contains information about the current event. Is passed to EventProcessors and Interjections
type EventContext[T any] struct {
	// we're going to keep a reference to the partition worker context
	// so we can skip over any buffered events in the EOSProducer
	ctx              context.Context
	producerChan     chan EventContextProducer[T]
	producer         EventContextProducer[T]
	revocationWaiter *sync.WaitGroup
	next             *EventContext[T]
	prev             *EventContext[T]
	input            IncomingRecord
	asyncCompleter   AsyncCompleter[T]
	changeLog        changeLogData[T]
	done             chan struct{}
	topicPartition   TopicPartition
	interjection     *interjection[T]
}

// A convenience function for creating unit tests for an EventContext from an incoming Kafka Record. All arguments other than `ctx`
// are optional unless you are interacting with those resources. For example, if you call EventContext.Forward/RecordChange, you will need to provide a mock producer.
// If you run the EventContext through an async process, you will need to provide a mock AsyncCompleter.
//
//	func TestMyHandler(t *testing.T) {
//		eventContext := streams.MockEventContext(context.TODO(), mockRecord(), "storeTopic", mockStore(), mockCompleter(), mockProducer())
//		if err := testMyHandler(eventContext, eventContext.Input()); {
//			t.Error(err)
//		}
//	}
func MockEventContext[T any](ctx context.Context, input *Record, stateStoreTopc string, store T, asyncCompleter AsyncCompleter[T], producer EventContextProducer[T]) *EventContext[T] {
	ec := &EventContext[T]{
		ctx: ctx,
		changeLog: changeLogData[T]{
			topic: stateStoreTopc,
			store: store,
		},
		asyncCompleter: asyncCompleter,
		producer:       producer,
	}
	if input != nil {
		ec.input = input.AsIncomingRecord()
		ec.topicPartition = input.TopicPartition()
	}
	return ec
}

// A convenience function for creating unit tests for an EventContext from an interjection.  All arguments other than `ctx`
// are optional unless you are interacting with those resources. For example, if you call EventContext.Forward/RecordChange, you will need to provide a mock producer.
// If you run the EventContext through an async process, you will need to provide a mock AsyncCompleter.
//
//	func TestMyInterjector(t *testing.T) {
//		eventContext := streams.MockInterjectionEventContext(context.TODO(), myTopicPartition, "storeTopic", mockStore(), mockCompleter(), mockProducer())
//		if err := testMyInterjector(eventContext, time.Now()); {
//			t.Error(err)
//		}
//	}
func MockInterjectionEventContext[T any](ctx context.Context, topicPartition TopicPartition, stateStoreTopc string, store T, asyncCompleter AsyncCompleter[T], producer EventContextProducer[T]) *EventContext[T] {
	ec := &EventContext[T]{
		topicPartition: topicPartition,
		changeLog: changeLogData[T]{
			topic: stateStoreTopc,
			store: store,
		},
		asyncCompleter: asyncCompleter,
		producer:       producer,
		interjection:   &interjection[T]{},
	}
	return ec
}

func (ec *EventContext[T]) isRevoked() bool {
	return ec.ctx.Err() != nil
}

// Returns true if this EventContext represents an Interjection
func (ec *EventContext[T]) IsInterjection() bool {
	return ec.interjection != nil
}

// The offset for this event, -1 for an Interjection
func (ec *EventContext[T]) Offset() int64 {
	if ec.IsInterjection() {
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

// Forwards produces records on the transactional producer for your EventSource.
// If the transaction fails, records produced in this fashion will not be visible to other consumers who have a fetch isolation of `read_commited`.
// An isolation level of `read_commited“ is required for Exactly Once Semantics
//
// It is important to note that GKES uses a Record pool. After the transaction has completed for this record, it is returned to the pool for reuse.
// Your application should not hold on to references to the Record(s) after Forward has been invoked.
func (ec *EventContext[T]) Forward(records ...*Record) {
	for _, record := range records {
		ec.producer.ProduceRecord(ec, record, nil)
	}
}

// Forwards records to the transactional producer for your EventSource. When you add an item to your StateStore,
// you must call this method for that change to be recorded in the stream. This ensures that when the TopicPartition for this change
// is tansferred to a new consumer, it will also have this change.
// If the transaction fails, records produced in this fashion will not be visible to other consumers who have a fetch isolation of `read_commited`.
// An isolation level of `read_commited“ is required for Exactly Once Semantics
//
// It is important to note that GKES uses a Record pool. After the transaction has completed for this record, it is returned to the pool for reuse.
// Your application should not hold on to references to the ChangeLogEntry(s) after RecordChange has been invoked.
func (ec *EventContext[T]) RecordChange(entries ...ChangeLogEntry) {
	for _, entry := range entries {
		if len(ec.changeLog.topic) > 0 {
			log.Tracef("RecordChange changeLogTopic: %s, topicPartition: %+v, value: %v", ec.changeLog.topic, ec.topicPartition, entry)
			record := entry.record.
				WithTopic(ec.changeLog.topic).
				WithPartition(ec.topicPartition.Partition)

			ec.producer.ProduceRecord(ec, record, nil)
		} else {
			log.Warnf("EventContext.RecordChange was called but consumer is not stateful")
		}
	}
}

// AsyncJobComplete should be called when an async event processor has performed it's function.
// the finalize cunction should return Complete if there are no other pending asynchronous jobs for the event context in question,
// regardless of error state. `finalize` does no accept any arguments, so you're callback should encapsulate
// any pertinent data needed for processing. If you are using [	], [AsyncJobScheduler] or [BatchProducer], you should not need to interact with this method directly.
func (ec *EventContext[T]) AsyncJobComplete(finalize func() ExecutionState) {
	ec.asyncCompleter.AsyncComplete(AsyncJob[T]{
		ctx:       ec,
		finalizer: finalize,
	})
}

// Return the raw input record for this event or an uninitialized record and false if the EventContect represents an Interjections
func (ec *EventContext[T]) Input() (IncomingRecord, bool) {
	return ec.input, !ec.IsInterjection()
}

// Returns the StateStore for this event/TopicPartition
func (ec *EventContext[T]) Store() T {
	return ec.changeLog.store
}

func (ec *EventContext[T]) complete() {
	close(ec.done)
}

func newEventContext[T StateStore](ctx context.Context, record *kgo.Record, changeLog changeLogData[T], pw *partitionWorker[T]) *EventContext[T] {
	input := newIncomingRecord(record)
	ec := &EventContext[T]{
		ctx:              ctx,
		producerChan:     make(chan EventContextProducer[T], 1),
		topicPartition:   input.TopicPartition(),
		changeLog:        changeLog,
		input:            input,
		interjection:     nil,
		asyncCompleter:   pw.asyncCompleter,
		revocationWaiter: (*sync.WaitGroup)(sak.Noescape(unsafe.Pointer(&pw.revocationWaiter))),
		done:             make(chan struct{}),
	}
	return ec
}

func newInterjectionContext[T StateStore](ctx context.Context, interjection *interjection[T], topicPartition TopicPartition, changeLog changeLogData[T], pw *partitionWorker[T]) *EventContext[T] {
	ec := &EventContext[T]{
		ctx:              ctx,
		producerChan:     make(chan EventContextProducer[T], 1),
		topicPartition:   topicPartition,
		interjection:     interjection,
		changeLog:        changeLog,
		asyncCompleter:   pw.asyncCompleter,
		revocationWaiter: (*sync.WaitGroup)(sak.Noescape(unsafe.Pointer(&pw.revocationWaiter))),
		done:             make(chan struct{}),
	}
	return ec
}
