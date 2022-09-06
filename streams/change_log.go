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
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

var markKey = []byte("gkes__mark")
var placeholder = []byte{1}

type ChangeAttributes map[string][]byte

type StateStore interface {
	ReceiveChange(IncomingRecord) error
	Revoked()
}

type changeLogGroupConsumer[T StateStore] struct {
	client           *kgo.Client
	producer         *kgo.Client
	partitionedStore *partitionedChangeLog[T]
	activeWaiters    map[int32]*sync.WaitGroup
	prepWaiters      map[int32]*sync.WaitGroup
	buffers          map[int32]chan []*kgo.Record
	topic            string
	partitions       []int32
	prepMark         []byte
	activeMark       []byte
	count            uint64
	highWatermarks   map[TopicPartition]int64
}

func newPartitionWaitGroup() *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return wg
}

func newChangeLogGroupConsumer[T StateStore](source Source, partitions []int32,
	producer *kgo.Client, partitionedStore *partitionedChangeLog[T]) *changeLogGroupConsumer[T] {

	activeWaiters := make(map[int32]*sync.WaitGroup, len(partitions))
	prepWaiters := make(map[int32]*sync.WaitGroup, len(partitions))
	buffers := make(map[int32]chan []*kgo.Record, len(partitions))
	assignments := make(map[int32]kgo.Offset)
	for _, p := range partitions {
		assignments[p] = kgo.NewOffset().AtStart()
		activeWaiters[p] = newPartitionWaitGroup()
		prepWaiters[p] = newPartitionWaitGroup()
		buffers[p] = make(chan []*kgo.Record, 4096*2)
	}
	client, err := NewClient(source.stateCluster(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			partitionedStore.changeLogTopic: assignments,
		}),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)

	if err != nil {
		panic(err)
	}

	clgc := &changeLogGroupConsumer[T]{
		client:           client,
		prepMark:         []byte(uuid.NewString()),
		activeMark:       []byte(uuid.NewString()),
		partitionedStore: partitionedStore,
		buffers:          buffers,
		activeWaiters:    activeWaiters,
		prepWaiters:      prepWaiters,
		topic:            partitionedStore.changeLogTopic,
		partitions:       partitions,
		producer:         producer,
	}

	return clgc
}

func (clgc *changeLogGroupConsumer[T]) cancel() {
	for _, p := range clgc.partitions {
		clgc.partitionedStore.revoke(p)
	}
	// TODO: we have wait groups on this change log, it's not safe to call Done() as some may already be complete
	// we'll just let this finish and ignore the results
}

func (clgc *changeLogGroupConsumer[T]) start() {
	for p, b := range clgc.buffers {
		receiver, _ := clgc.partitionedStore.getStore(p)
		go clgc.populate(b, receiver)
	}
	go func() {
		clgc.waitUntilActive()
		clgc.client.Close()
		for _, c := range clgc.buffers {
			go close(c)
		}
	}()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			f := clgc.client.PollFetches(ctx)
			cancel()
			if f.IsClientClosed() {
				log.Debugf("client closed")
				return
			}
			for _, err := range f.Errors() {
				log.Errorf("%v", err)
			}
			f.EachPartition(clgc.add)
		}
	}()
}

func (clgc *changeLogGroupConsumer[T]) prepare() {
	go clgc.sendMarkerMessage(clgc.prepMark)
}

func (clgc *changeLogGroupConsumer[T]) activate() {
	go clgc.sendMarkerMessage(clgc.activeMark)
}

// func (clgc *changeLogGroupConsumer[T]) preparedWaiterFor(partition int32) func() {
// 	return clgc.prepWaiters[partition].Wait
// }

// func (clgc *changeLogGroupConsumer[T]) activeWaiterFor(partition int32) func() {
// 	return clgc.activeWaiters[partition].Wait
// }

func (clgc *changeLogGroupConsumer[T]) waitUntilPrepared() {
	for _, wg := range clgc.prepWaiters {
		wg.Wait()
	}
}

func (clgc *changeLogGroupConsumer[T]) waitUntilActive() {
	for _, wg := range clgc.activeWaiters {
		wg.Wait()
	}
}

func (clgc *changeLogGroupConsumer[T]) add(ftp kgo.FetchTopicPartition) {
	clgc.buffers[ftp.Partition] <- ftp.FetchPartition.Records
}

func (clgc *changeLogGroupConsumer[T]) processed() uint64 {
	return atomic.LoadUint64(&clgc.count)
}

func (clgc *changeLogGroupConsumer[T]) populate(c chan []*kgo.Record, receiver *changeLogPartition[T]) {
	for records := range c {
		for _, record := range records {
			if isMarkerRecord(record) {
				if bytes.Equal(record.Value, clgc.prepMark) {
					clgc.prepWaiters[record.Partition].Done()
				} else if bytes.Equal(record.Value, clgc.activeMark) {
					clgc.activeWaiters[record.Partition].Done()
				}
			} else if len(record.Headers) == 1 && record.Headers[0].Key == "gkes__watermark" {
				tp := TopicPartition{
					Partition: record.Partition,
					Topic:     string(record.Headers[0].Value),
				}
				offset := binary.LittleEndian.Uint64(record.Value)
				clgc.highWatermarks[tp] = int64(offset)
			} else {
				receiver.receiveChangeInternal(record)
				atomic.AddUint64(&clgc.count, 1)
			}
		}
	}
}

func (clgc *changeLogGroupConsumer[T]) sendMarkerMessage(mark []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(clgc.partitions))
	for _, p := range clgc.partitions {
		sendMarkerMessage(clgc.producer, TopicPartition{Partition: p, Topic: clgc.topic}, mark, &wg)
	}
	wg.Wait()
}

func sendMarkerMessage(producer *kgo.Client, tp TopicPartition, mark []byte, wg *sync.WaitGroup) {
	record := kgo.KeySliceRecord(markKey, mark)
	record.Topic = tp.Topic
	record.Partition = tp.Partition
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: "gkes__mark", Value: placeholder})
	log.Debugf("Sending marker message to: %+v", tp)
	producer.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		wg.Done()
	})
}

func isMarkerRecord(record *kgo.Record) bool {
	return len(record.Headers) == 1 && record.Headers[0].Key == "gkes__mark"
}
