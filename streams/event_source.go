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

import "time"

// EventSource provides an abstraction over raw kgo.Record/streams.IncomingRecord consumption, allowing the use of strongly typed event handlers.
// One of the key features of the EventSource is to allow for the routing of events based off of a type header. See RegisterEventType for details.
type EventSource[T StateStore] struct {
	root              *eventProcessorWrapper[T]
	tail              *eventProcessorWrapper[T]
	stateStoreFactory StateStoreFactory[T]
	defaultProcessor  EventProcessor[T, IncomingRecord]
	consumer          *eventSourceConsumer[T]
	interjections     []interjection[T]
	source            Source
	done              chan struct{}
}

// Create an EventSource.
// defaultProcessor will be invoked if a suitable EventProcessor can not be found, or the IncomingRecord has no RecordType header
func NewEventSource[T StateStore](source Source, stateStoreFactory StateStoreFactory[T], defaultProcessor EventProcessor[T, IncomingRecord]) (*EventSource[T], error) {
	es := &EventSource[T]{
		defaultProcessor:  defaultProcessor,
		stateStoreFactory: stateStoreFactory,
		source:            source,
		done:              make(chan struct{}, 1),
	}
	var err error
	es.consumer, err = newEventSourceConsumer(es)
	return es, err
}

func (es *EventSource[T]) ConsumeEvents() {
	go es.consumer.start()
}

// Signals the underlying *kgo.Client that the underlying consumer should exit the group.
// If you are using an IncrementalGroupRebalancer, this will trigger a graceful exit where owned partitions are surrendered
// according to it's configuration. If you are not, this call has the same effect as StopNow()
func (es *EventSource[T]) Stop() {
	go func() {
		<-es.consumer.leave()
		select {
		case es.done <- struct{}{}:
		default:
		}
	}()
}

func (es *EventSource[T]) Done() <-chan struct{} {
	return es.done
}

// Immediately stops the underlying consumer *kgo.Client by invoking sc.client.Close()
// This has the effect of immediately surrendering all owned partitions, then closing the client.
// If you are using an IncrementalGroupRebalancer, see the Stop() call documentation.
func (es *EventSource[T]) StopNow() {
	es.consumer.stop()
	select {
	case es.done <- struct{}{}:
	default:
	}
}

/*
ScheduleInterjection sets a timer for `interjector` to be run `every` time interval,
plus or minues a random time.Duration not greater than the absolute value of `jitter` on every invocation.
`interjector` will have access to EventContext.Store() and can create/delete store items, or forward events
just as a standard EventProcessor. Example:

 func cleanupStaleItems(ec *EventContext[myStateStore], when time.Time)  streams.ExecutionState {
	ec.Store().cleanup(when)
	return ec.Complete
 }
 // schedules cleanupStaleItems to be executed every 900ms - 1100ms
 eventSource.ScheduleInterjection(cleanupStaleItems, time.Second, 100 * time.Millisecond)
*/
func (es *EventSource[T]) ScheduleInterjection(interjector Interjector[T], every, jitter time.Duration) {
	es.interjections = append(es.interjections, interjection[T]{
		interjector: interjector,
		every:       every,
		jitter:      jitter,
	})
}

// Executes `cmd` in the context of the given TopicPartition. `callback`` is an optional, and will be excuted once the interjection is complete if non-nil.
// `callback` is used interally to make EachChangeLogPartition() a blocking call. It may or may not be useful depending on you use case.
func (es *EventSource[T]) Interject(tp TopicPartition, cmd Interjector[T], callback func()) {
	es.consumer.interject(tp, cmd, callback)
}

/*
A convenience function which allows you to Interject into every active partition assigned to the consumer
without create an individual timer per partition.
The uquivalent of calling Interject() on each active partition, blocking on each iteration until the Interjection can be processed.
Useful for gathering store statistics, but can be used in place of a standard Interjection. Example:

 itemCount := 0
 eventSource.EachChangeLogPartition(func (ec *EventContext[myStateStore], when time.Time) streams.ExecutionState {
	store := ec.Store()
	itemCount += stor.Len()
	return streams.Complete
 })
 fmt.Println("Number of items: ", itemCount)
*/
func (es *EventSource[T]) EachChangeLogPartition(interjector Interjector[T]) {
	es.consumer.forEachChangeLogPartition(interjector)
}

func (ec *EventSource[T]) createChangeLogReceiver(tp TopicPartition) T {
	return ec.stateStoreFactory(tp)
}

// Starts the event processing by invoking registered processors. If no processors exist for record.recordTpe, the defaultProcessor will be invoked.
func (es *EventSource[T]) handleEvent(ctx *EventContext[T], record IncomingRecord) (ExecutionState, error) {
	state := unknownType
	var err error
	if es.root != nil {
		state, err = es.root.process(ctx, record)
	}
	if state == unknownType {
		state, err = es.defaultProcessor(ctx, record)
	}
	return state, err
}

// A callback invoked when a new TopicPartition has been assigned to a EventSource. Your callback should return an empty StateStore.
type StateStoreFactory[T StateStore] func(TopicPartition) T

// A callback invoked when a new record has been received from the EventSource.
type IncomingRecordDecoder[V any] func(IncomingRecord) V

// A callback invoked when a new record has been received from the EventSource, after it has been transformed via IncomingRecordTransformer.
type EventProcessor[T any, V any] func(*EventContext[T], V) (ExecutionState, error)

// Registers eventType with a transformer (usuall a codec.Codec) with the supplied EventProcessor.
func RegisterEventType[T StateStore, V any](ec *EventSource[T], transformer IncomingRecordDecoder[V], eventProcessor EventProcessor[T, V], eventType string) {
	ep := newEventProcessor(eventType, transformer, eventProcessor)
	if ec.root == nil {
		ec.root, ec.tail = ep, ep
	} else {
		ec.tail.next = ep
		ec.tail = ep
	}
}

// Wraps an EventProcessor with a function that decodes the record before invoking eventProcessor.
type eventProcessorWrapper[T any] struct {
	eventType string
	exec      func(*EventContext[T], IncomingRecord) (ExecutionState, error)
	next      *eventProcessorWrapper[T]
}

func newEventProcessor[T any, V any](eventType string, decode IncomingRecordDecoder[V], eventProcessor EventProcessor[T, V]) *eventProcessorWrapper[T] {
	return &eventProcessorWrapper[T]{
		eventType: eventType,
		// doing some type gymnastics here.
		// we have 2 generic types declared here, but to have a eventProcessorWrapper[T,V], *next[T,X] would not work
		// golang generics do no yet allow for defining new type in struct method declarations
		// so we're relegated to exec being a closure. we could also use an interface, but don't want to pay the possible penalty
		// that comes with invoking a method on an interface, it's a small penalty if any, but a closure works just as well
		exec: func(ec *EventContext[T], record IncomingRecord) (ExecutionState, error) {
			return eventProcessor(ec, decode(record))
		},
	}
}

// process the record if records.recordType == eventProcessorWrapper.eventType, otherwise forward this record to the next processor.
func (ep *eventProcessorWrapper[T]) process(ctx *EventContext[T], record IncomingRecord) (ExecutionState, error) {
	if record.RecordType() == ep.eventType {
		return ep.exec(ctx, record)
	}
	if ep.next != nil {
		return ep.next.process(ctx, record)
	}
	return unknownType, nil
}
