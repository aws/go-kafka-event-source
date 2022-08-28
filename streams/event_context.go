package streams

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Contains information about the current event. Is passed to EventProcessors and Interjections
type EventContext[T any] struct {
	ctx            context.Context
	producer       *producerNode[T]
	changeLog      *changeLogData[T]
	input          IncomingRecord
	asynCompleter  asyncCompleter[T]
	topicPartition TopicPartition
	pendingRecords []recordContainer
	next           *EventContext[T]
	produceLock    sync.Mutex
	sync.WaitGroup
	isInterjection bool
}

func (ec *EventContext[T]) Ctx() context.Context {
	return ec.ctx
}

// Returns true if this EventContext represents an Interjection
func (ec *EventContext[T]) IsInterjection() bool {
	return ec.isInterjection
}

// The offset for this event, currently 0 for an Interjection
func (ec *EventContext[T]) Offset() int64 {
	return ec.input.Offset()
}

// The TopicParition for this event. It is present fpr both normal events ans Interjections
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
			ec.producer.produceRecord(ec.ctx, record, nil)
		}
	}

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
				ec.producer.produceRecord(ec.ctx, record, nil)
			} else {
				ec.pendingRecords = append(ec.pendingRecords, recordContainer{record, true})
			}
		} else {
			log.Warnf("EventContext.RecordChange was called but consumer is not stateful")
		}
	}
}

func (ec *EventContext[T]) asyncComplete(finalize func(TopicPartition) (ExecutionState, error)) {
	ec.asynCompleter.asyncComplete(asyncJob[T]{
		ctx:      ec,
		finalize: finalize,
	})
}

func (ec *EventContext[T]) setProducer(p *producerNode[T]) {
	ec.produceLock.Lock()
	defer ec.produceLock.Unlock()
	ec.producer = p

	for _, cont := range ec.pendingRecords {
		ec.producer.produceRecord(ec.ctx, cont.record, nil)
	}
	ec.pendingRecords = []recordContainer{}
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
	ec.Done()
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
	ec.Add(1)
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
	ec.Add(1)
	return ec
}
