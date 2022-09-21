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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EventSource provides an abstraction over raw kgo.Record/streams.IncomingRecord consumption, allowing the use of strongly typed event handlers.
type MetricsHandler func(interface{})
type Metric struct {
	StartMicro int64
	EndMicro   int64
	Count      int64
	Bytes      int64
	Partition  int32
	Operation  string
	Topic      string
}

func (m Metric) Duration() time.Duration {
	return time.Duration(m.EndMicro-m.StartMicro) / time.Microsecond
}

// One of the key features of the EventSource is to allow for the routing of events based off of a type header. See RegisterEventType for details.
type EventSource[T StateStore] struct {
	root              *eventProcessorWrapper[T]
	tail              *eventProcessorWrapper[T]
	stateStoreFactory StateStoreFactory[T]
	defaultProcessor  EventProcessor[T, IncomingRecord]
	consumer          *eventSourceConsumer[T]
	interjections     []interjection[T]
	source            *Source
	runStatus         sak.RunStatus
	done              chan struct{}
	metrics           chan Metric
}

// Create an EventSource.
// `defaultProcessor` will be invoked if a suitable EventProcessor can not be found, or the IncomingRecord has no RecordType header.
func NewEventSource[T StateStore](sourceConfig EventSourceConfig, stateStoreFactory StateStoreFactory[T], defaultProcessor EventProcessor[T, IncomingRecord],
	additionalClientOptions ...kgo.Opt) (*EventSource[T], error) {
	source, err := CreateSource(sourceConfig)
	if err != nil {
		return nil, err
	}
	var metrics chan Metric
	if source.config.MetricsHandler != nil {
		metrics = make(chan Metric, 2048)
	}

	es := &EventSource[T]{
		defaultProcessor:  defaultProcessor,
		stateStoreFactory: stateStoreFactory,
		source:            source,
		runStatus:         sak.NewRunStatus(context.Background()),
		done:              make(chan struct{}, 1),
		metrics:           metrics,
	}
	es.consumer, err = newEventSourceConsumer(es, additionalClientOptions...)
	return es, err
}

// ConsumeEvents starts the underlying Kafka consumer. This call is non-blocking,
// so if called from main(), it should be followed by some other blocking call to prevent the application from exiting.
// See [streams.EventSource.WaitForSignals] for an example.
func (es *EventSource[T]) ConsumeEvents() {
	go es.emitMetrics()
	go es.consumer.start()
}

func (es *EventSource[T]) EmitMetric(m Metric) {
	if es.metrics != nil {
		select {
		case es.metrics <- m:
		default:
			log.Warnf("metrics channel full, unable to emit metrics: %+v", m)
		}
	}
}

func (es *EventSource[T]) emitMetrics() {
	if es.metrics == nil {
		return
	}
	handler := es.source.config.MetricsHandler
	for {
		select {
		case m := <-es.metrics:
			handler(m)
		case <-es.runStatus.Done():
			close(es.metrics)
			return
		}
	}
}

/*
WaitForSignals is convenience function suitable for use in a main() function.
Blocks until `signals` are received then gracefully closes the consumer by calling [streams.EventSource.Stop].
If `signals` are not provided, syscall.SIGINT and syscall.SIGHUP are used. If `preHook` is non-nil, it will be invoked before
Stop() is invoked. If the preHook returns false, this call continues to block. If true is returned, `signal.Reset(signals...)`
is invoked and the consumer shutdown process begins. Simple example:

	func main(){
		myEventSource := initEventSource()
		myEventSource.ConsumeEvents()
		myEventSource.WaitForSignals(nil)
		fmt.Println("exiting")
	}

Prehook example:

	func main(){
		myEventSource := initEventSource()
		myEventSource.ConsumeEvents()
		myEventSource.WaitForSignals(func(s os.Signal) bool {
			fmt.Printf("starting shutdown from signal %v\n", s)
			shutDownSomeOtherProcess()
			return true
		})
		fmt.Println("exiting")
	}

In this example, The consumer will close on syscall.SIGINT or syscall.SIGHUP but not syscall.SIGUSR1:

	func main(){
		myEventSource := initEventSource()
		myEventSource.ConsumeEvents()
		myEventSource.WaitForSignals(func(s os.Signal) bool {
			if s == syscall.SIGUSR1 {
				fmt.Println("user signal received")
				performSomeTask()
				return false
			}
			return true
		}, syscall.SIGINT and syscall.SIGHUP, syscall.SIGUSR1)
		fmt.Println("exiting")
	}
*/
func (es *EventSource[T]) WaitForSignals(preHook func(os.Signal) bool, signals ...os.Signal) {
	if len(signals) == 0 {
		signals = []os.Signal{syscall.SIGINT, syscall.SIGHUP}
	}
	if preHook == nil {
		preHook = func(_ os.Signal) bool {
			return true
		}
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for s := range c {
		if preHook(s) {
			signal.Reset(signals...)
			break
		}
	}
	es.Stop()
	<-es.Done()
}

/*
WaitForChannel is similar to WaitForSignals, but blocks on a `chan struct{}` then invokes `callback` when finished.
Useful when you have multiple EventSources in a single application. Example:

	func main() {

		myEventSource1 := initEventSource1()
		myEventSource2.ConsumeEvents()

		myEventSource2 := initEventSource2()
		myEventSource2.ConsumeEvents()

		wg := &sync.WaitGroup{}
		wg.Add(2)

		eventSourceChannel = make(chan struct{})

		go myEventSource1.WaitForChannel(eventSourceChannel, wg.Done)
		go myEventSource2.WaitForChannel(eventSourceChannel, wg.Done)

		osChannel := make(chan os.Signal)
		signal.Notify(osChannel, syscall.SIGINT, syscall.SIGHUP)
		<-osChannel
		close(eventSourceChannel)
		wg.Wait()
		fmt.Println("exiting")
	}
*/
func (es *EventSource[T]) WaitForChannel(c chan struct{}, callback func()) {
	<-c
	es.Stop()
	<-es.Done()
	if callback != nil {
		callback()
	}
}

// Done blocks while the underlying Kafka consumer is active.
func (es *EventSource[T]) Done() <-chan struct{} {
	return es.done
}

// Signals the underlying *kgo.Client that the underlying consumer should exit the group.
// If you are using an IncrementalGroupRebalancer, this will trigger a graceful exit where owned partitions are surrendered
// according to it's configuration. If you are not, this call has the same effect as [streams.EventSource.StopNow].
//
// Calls to Stop are not blocking. To block during the shut down process, this call should be followed by `<-eventSource.Done()`
//
// To simplify running from main(), the [streams.EventSource.WaitForSignals] and [streams.EventSource.WaitForChannel] calls have been provided.
// So unless you have extremely complex application shutdown logic, you should not need to interact with this method directly.
func (es *EventSource[T]) Stop() {
	go func() {
		<-es.consumer.leave()
		es.runStatus.Halt()
		select {
		case es.done <- struct{}{}:
		default:
		}
	}()
}

// Immediately stops the underlying consumer *kgo.Client by invoking sc.client.Close()
// This has the effect of immediately surrendering all owned partitions, then closing the client.
// If you are using an IncrementalGroupRebalancer, this can be used as a force quit.
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

// Executes `cmd` in the context of the given TopicPartition. `callback“ is an optional, and will be executed once the interjection is complete if non-nil.
// `callback` is used interally to make InterjectAll a blocking call. `callback` may or may not be useful depending on your use case.
func (es *EventSource[T]) Interject(tp TopicPartition, cmd Interjector[T], callback func()) {
	es.consumer.interject(tp.Partition, cmd, callback)
}

/*
InterjectAll is a convenience function which allows you to Interject into every active partition assigned to the consumer without create an individual timer per partition.
The equivalent of calling Interject() on each active partition, blocking until all are performed. It is worth noting that the interjections are run in parallel, so care must be taken
not to create a deadlock between partitions via locking mechanisms such as a Mutex. If parallel processing is not of concern, [streams.EventSource.InterjectAllSync] is an alternative.
Useful for gathering store statistics, but can be used in place of a standard Interjection. Example:

	preCount := int64(0)
	postCount := int64(0)
	eventSource.InterjectAllAsync(func (ec *EventContext[myStateStore], when time.Time) streams.ExecutionState {
		store := ec.Store()
		atomic.AddInt64(&preCount, int64(store.Len()))
		store.performBookeepingTasks()
		atomic.AddInt64(&postCount, int64(store.Len()))
		return streams.Complete
	})
	fmt.Printf("Number of items before: %d, after: %d\n", preCount, postCount)
*/
func (es *EventSource[T]) InterjectAll(interjector Interjector[T]) {
	es.consumer.forEachChangeLogPartitionAsync(interjector)
}

/*
InterjectAllSync performs the same function as [streams.EventSource.InterjectAll], however it blocks on each iteration.
It may be useful if parallel processing is not of concern andyou want to avoid locking on a shared data structure. Example:

	itemCount := 0
	eventSource.InterjectAll(func (ec *EventContext[myStateStore], when time.Time) streams.ExecutionState {
		store := ec.Store()
		itemCount += store.Len()
		return streams.Complete
	})
	fmt.Println("Number of items: ", itemCount)
*/
func (es *EventSource[T]) InterjectAllSync(interjector Interjector[T]) {
	es.consumer.forEachChangeLogPartitionSync(interjector)
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
type IncomingRecordDecoder[V any] func(IncomingRecord) (V, error)

// A callback invoked when a new record has been received from the EventSource, after it has been transformed via IncomingRecordTransformer.
type EventProcessor[T any, V any] func(*EventContext[T], V) (ExecutionState, error)

// Registers eventType with a transformer (usuall a codec.Codec) with the supplied EventProcessor.
func RegisterEventType[T StateStore, V any](es *EventSource[T], transformer IncomingRecordDecoder[V], eventProcessor EventProcessor[T, V], eventType string) {
	ep := newEventProcessorWrapper(eventType, transformer, eventProcessor, es.source.deserializationErrorHandler())
	if es.root == nil {
		es.root, es.tail = ep, ep
	} else {
		es.tail.next = ep
		es.tail = ep
	}
}

type eventExecutor[T any] interface {
	Exec(*EventContext[T], IncomingRecord) (ExecutionState, error)
}

// Wraps an EventProcessor with a function that decodes the record before invoking eventProcessor.
// Doing some type gymnastics here.
// We have 2 generic types declared here, but to have a eventProcessorWrapper[T,V], *next[T,X] would not work.
// Golang generics do no yet allow for defining new type in struct method declarations, so we have a private interface
// wrapped by a generic.
type eventProcessorWrapper[T any] struct {
	eventType     string
	eventExecutor eventExecutor[T]
	next          *eventProcessorWrapper[T]
}

type eventProcessorExecutor[T any, V any] struct {
	process                    EventProcessor[T, V]
	decode                     IncomingRecordDecoder[V]
	handleDeserializationError DeserializationErrorHandler
}

func (epe *eventProcessorExecutor[T, V]) Exec(ec *EventContext[T], record IncomingRecord) (ExecutionState, error) {
	if event, err := epe.decode(record); err == nil {
		return epe.process(ec, event)
	} else {
		if epe.handleDeserializationError(ec, record.RecordType(), err) == CompleteAndContinue {
			return Complete, err
		}
		return Incomplete, err
	}
}

func newEventProcessorWrapper[T any, V any](eventType string, decoder IncomingRecordDecoder[V],
	eventProcessor EventProcessor[T, V], deserializationErrorHandler DeserializationErrorHandler) *eventProcessorWrapper[T] {
	return &eventProcessorWrapper[T]{
		eventType: eventType,
		eventExecutor: &eventProcessorExecutor[T, V]{
			process:                    eventProcessor,
			decode:                     decoder,
			handleDeserializationError: deserializationErrorHandler,
		},
	}
}

func (ep *eventProcessorWrapper[T]) exec(ec *EventContext[T], record IncomingRecord) (ExecutionState, error) {
	return ep.eventExecutor.Exec(ec, record)
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
