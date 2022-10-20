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
// Defaults options are: kgo.ProducerLinger(5 * time.Millisecond) and
// kgo.RecordPartitioner(NewOptionalPartitioner(kgo.StickyKeyPartitioner(nil)))
func NewProducer(destination Destination, opts ...kgo.Opt) *Producer {
	client, err := NewClient(destination.Cluster, opts...)
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
	p.client.Produce(ctx, record.toKafkaRecord(), func(r *kgo.Record, kErr error) {
		if callback != nil {
			callback(record, kErr)
		}
	})
}

func (p *Producer) Close() {
	p.client.Close()
}

type BatchProducer[S any] struct {
	client      *kgo.Client
	destination Destination
}

// Provides similar functionality to [AsyncBatcher], but in the context of producing Kafka records.
// Since the underlying Kafka producer already batches in an ordered fashion, there is no need to add the overhead of the [AsyncBatcher].
// Records produced by a BatchProducer are not transactional, and therefore duplicates could be created.
// The use cases for the BatchProducer vs EventContext.Forward are as follows:
//
// - The topic you are producing to is not on the same Kafka cluster as your EventSource
//
// - Duplicates are OK and you do not want to wait for the transaction to complete before the consumers of these records can see the data (lower latency)
//
// If your use case does not fall into the above buckets, it is recommended to just use [EventConetxt.Forward]
func NewBatchProducer[S any](destination Destination, opts ...kgo.Opt) *BatchProducer[S] {
	client, err := NewClient(destination.Cluster, opts...)
	if err != nil {
		panic(err)
	}
	p := &BatchProducer[S]{
		client:      client,
		destination: destination,
	}
	return p
}

type produceBatcher[S any] struct {
	ctx      *EventContext[S]
	records  []*Record
	pending  int64
	callback BatchProducerCallback[S]
	userData any
}

func (b *produceBatcher[S]) Key() TopicPartition {
	return b.ctx.TopicPartition()
}

func (b *produceBatcher[S]) cleanup() {
	for _, r := range b.records {
		r.Release()
	}
	b.records = nil
}

func (b *produceBatcher[S]) recordComplete() {
	if atomic.AddInt64(&b.pending, -1) == 0 && b.callback != nil {
		b.ctx.AsyncJobComplete(b.executeCallback)
	}
}

func (b *produceBatcher[S]) executeCallback() ExecutionState {
	state := b.callback(b.ctx, b.records, b.userData)
	b.cleanup()
	return state
}

// Produces `records` and invokes BatchProducerCallback once all records have been produced or have errored out.
// If there was an error in producing, it can be retrieved with record.Error()
//
// It is important to note that GKES uses a Record pool. After the transaction has completed for this record, it is returned to the pool for reuse.
// Your application should not hold on to references to the Record(s) after BatchProducerCallback has been invoked.
func (p *BatchProducer[S]) Produce(ec *EventContext[S], records []*Record, cb BatchProducerCallback[S], userData any) ExecutionState {
	b := &produceBatcher[S]{
		ctx:      ec,
		records:  records,
		callback: cb,
		pending:  int64(len(records)),
		userData: userData,
	}
	p.produceBatch(b)
	return Incomplete
}

func (p *BatchProducer[S]) produceBatch(b *produceBatcher[S]) {
	for _, record := range b.records {
		if len(record.kRecord.Topic) == 0 {
			record = record.WithTopic(p.destination.DefaultTopic)
		}
		p.produceRecord(b, record)
	}
}

func (p *BatchProducer[S]) produceRecord(b *produceBatcher[S], record *Record) {
	p.client.Produce(context.TODO(), record.toKafkaRecord(), func(kr *kgo.Record, err error) {
		record.err = err
		b.recordComplete()
	})
}
