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
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Destination struct {
	// The topic to use for records being produced which have empty topic data
	DefaultTopic string
	// Optional, used in CreateDestination call.
	NumPartitions int
	// Optional, used in CreateDestination call.
	ReplicationFactor int
	// Optional, used in CreateDestination call.
	MinInSync int
	// The Kafka cluster where this destination resides.
	Cluster Cluster
}

// A simple kafka producer
type Producer struct {
	client      *kgo.Client
	destination Destination
}

// Create a new Producer. Destination provides cluster connect information.
func NewProducer(destination Destination) *Producer {
	client, err := NewClient(
		destination.Cluster,
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))

	if err != nil {
		panic(err)
	}
	p := &Producer{
		client:      client,
		destination: destination,
	}
	return p
}

// Produces a record, blocking until complete.
// If the record has not topic, the DefaultTopic of the producer's Destination will be used.
func (p *Producer) Produce(ctx context.Context, record *Record) (err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.ProduceAsync(ctx, record, func(_ *Record, kErr error) {
		err = kErr
		wg.Done()
	})
	wg.Wait()
	return
}

// Produces a record asynchronously. If callback is non-nill, it will be executed `callback` when the call is complete.
// If the record has not topic, the DefaultTopic of the producer's Destination will be used.
func (p *Producer) ProduceAsync(ctx context.Context, record *Record, callback func(*Record, error)) {
	if len(record.kRecord.Topic) == 0 {
		record = record.WithTopic(p.destination.DefaultTopic)
	}
	p.client.Produce(ctx, record.ToKafkaRecord(), func(r *kgo.Record, kErr error) {
		if callback != nil {
			callback(record, kErr)
		}
	})
}

type EventSourceProducer[T any] struct {
	client      *kgo.Client
	destination Destination
}

func NewEventSourceProducer[T any](destination Destination) *EventSourceProducer[T] {
	client, err := NewClient(
		destination.Cluster,
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))

	if err != nil {
		panic(err)
	}
	p := &EventSourceProducer[T]{
		client:      client,
		destination: destination,
	}
	return p
}

type BatchCallback[T any] func(*EventContext[T], []*Record, []error) (ExecutionState, error)

type produceBatcher[T any] struct {
	ctx      *EventContext[T]
	records  []*Record
	errs     []error
	callback BatchCallback[T]
	pending  int64
}

func (b *produceBatcher[T]) Key() TopicPartition {
	return b.ctx.TopicPartition()
}

// func (b *produceBatcher[T]) Ctx() context.Context {
// 	return b.ctx.Ctx()
// }

func (b *produceBatcher[T]) cleanup() {
	for _, r := range b.records {
		r.release()
	}
	b.records = nil
}

func (b *produceBatcher[T]) recordComplete(record *kgo.Record, err error) {
	b.errs = append(b.errs, err)
	if atomic.AddInt64(&b.pending, -1) == 0 && b.callback != nil {
		b.ctx.AsyncJobComplete(b.executeCallback)
	}
}

func (b *produceBatcher[T]) executeCallback() (ExecutionState, error) {
	state, err := b.callback(b.ctx, b.records, b.errs)
	b.cleanup()
	return state, err
}

func (p *EventSourceProducer[T]) Produce(ec *EventContext[T], records []*Record, cb BatchCallback[T]) {
	b := &produceBatcher[T]{
		ctx:      ec,
		records:  records,
		callback: cb,
		pending:  int64(len(records)),
	}
	p.produceBatch(b)
}

func (p *EventSourceProducer[T]) produceBatch(b *produceBatcher[T]) {
	for _, record := range b.records {
		if len(record.kRecord.Topic) == 0 {
			record = record.WithTopic(p.destination.DefaultTopic)
		}
		p.client.Produce(context.TODO(), record.ToKafkaRecord(), b.recordComplete)
	}
}
