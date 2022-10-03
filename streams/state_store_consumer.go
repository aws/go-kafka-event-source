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

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionState uint32

const (
	paused partitionState = iota
	prepping
	standby
	ready
	active
)
const markerKeyString = "gkes__mark"

var markerKey = []byte(markerKeyString)
var startEpochOffset = kgo.EpochOffset{
	Offset: 0,
	Epoch:  -1,
}

type StateStore interface {
	ReceiveChange(IncomingRecord) error
	Revoked()
}

type stateStorePartition[T StateStore] struct {
	buffer         chan []*kgo.Record
	client         *kgo.Client
	waiters        map[string]chan struct{}
	topicPartition TopicPartition
	state          partitionState
	count          uint64
	byteCount      uint64
	waiterLock     sync.Mutex
	highWatermark  int64
}

func (ssp *stateStorePartition[T]) add(ftp kgo.FetchTopicPartition) {
	ssp.buffer <- ftp.FetchPartition.Records
}

func (ssp *stateStorePartition[T]) pause() {
	ssp.setState(paused)
	topic := ssp.topicPartition.Topic
	partition := ssp.topicPartition.Partition
	ssp.client.PauseFetchPartitions(map[string][]int32{
		topic: {partition},
	})

}

func (ssp *stateStorePartition[T]) kill() {
	if ssp.buffer != nil {
		close(ssp.buffer)
		ssp.buffer = nil
	}
}

func (ssp *stateStorePartition[T]) cancel() {
	ssp.pause()
	ssp.kill()
}

func (ssp *stateStorePartition[T]) addWaiter() (c chan struct{}, mark []byte) {
	ssp.waiterLock.Lock()
	defer ssp.waiterLock.Unlock()
	c = make(chan struct{})
	s := uuid.NewString()
	mark = []byte(s)
	ssp.waiters[s] = c
	return
}

func (ssp *stateStorePartition[T]) removeWaiterForMark(mark []byte) (chan struct{}, bool) {
	ssp.waiterLock.Lock()
	defer ssp.waiterLock.Unlock()
	if c, ok := ssp.waiters[string(mark)]; ok {
		delete(ssp.waiters, string(mark))
		return c, true
	}
	return nil, false
}

func (ssp *stateStorePartition[T]) sync() {
	c, mark := ssp.addWaiter()
	sendMarkerMessage(ssp.client, ssp.topicPartition, mark)
	<-c
}

func (ssp *stateStorePartition[T]) processed() uint64 {
	return atomic.LoadUint64(&ssp.count)
}

func (ssp *stateStorePartition[T]) processedBytes() uint64 {
	return atomic.LoadUint64(&ssp.byteCount)
}

func (ssp *stateStorePartition[T]) partitionState() partitionState {
	return partitionState(atomic.LoadUint32((*uint32)(&ssp.state)))
}

func (ssp *stateStorePartition[T]) setState(state partitionState) {
	atomic.StoreUint32((*uint32)(&ssp.state), uint32(state))
}

func (ssp *stateStorePartition[T]) prep(intitialState partitionState, store changeLogPartition[T]) {
	ssp.setState(intitialState)
	ssp.count = 0
	ssp.byteCount = 0
	ssp.buffer = make(chan []*kgo.Record, 1024)
	topic := ssp.topicPartition.Topic
	partition := ssp.topicPartition.Partition
	ssp.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {partition: startEpochOffset},
	})
	ssp.client.ResumeFetchPartitions(map[string][]int32{
		topic: {partition},
	})
	go ssp.populate(store)
}

func (ssp *stateStorePartition[T]) isCompletionMarker(val []byte) (complete bool) {
	var waiter chan struct{}
	var ok bool
	if waiter, ok = ssp.removeWaiterForMark(val); !ok {
		return
	}
	switch ssp.partitionState() {
	case prepping:
		log.Debugf("transitioning from prepping to ready for %+v", ssp.topicPartition)
		ssp.setState(ready)
	case ready:
		log.Debugf("transitioning from ready to paused for %+v", ssp.topicPartition)
		complete = true
		ssp.cancel()
	}
	close(waiter)
	return
}

func (ssp *stateStorePartition[T]) populate(store changeLogPartition[T]) {
	log.Debugf("starting populator for %+v", ssp.topicPartition)
	for records := range ssp.buffer {
		if !ssp.handleRecordsAndContinue(records, store) {
			log.Debugf("closed populator for %+v", ssp.topicPartition)
			return
		}
	}
}

func (ssp *stateStorePartition[T]) handleRecordsAndContinue(records []*kgo.Record, store changeLogPartition[T]) bool {
	for _, record := range records {
		if isMarkerRecord(record) {
			if ssp.isCompletionMarker(record.Value) {
				return false
			}
		} else {
			store.receiveChangeInternal(record)
			// if err := ; err != nil {
			// 	log.Errorf("Error receiving change on topic: %s, partition: %d, offset: %d, err: %v",
			// 		record.Topic, record.Partition, record.Offset, err)
			// }
			atomic.AddUint64(&ssp.count, 1)
			atomic.AddUint64(&ssp.byteCount, uint64(recordSize(*record)))
		}
	}
	return true
}

type stateStoreConsumer[T StateStore] struct {
	partitions map[int32]*stateStorePartition[T]
	source     *Source
	client     *kgo.Client
	mux        sync.Mutex
	topic      string
}

func mewStateStoreConsumer[T StateStore](source *Source) *stateStoreConsumer[T] {
	partitionCount := int32(source.config.NumPartitions)
	stateStorePartitions := make(map[int32]*stateStorePartition[T], partitionCount)
	assignments := make(map[int32]kgo.Offset, partitionCount)
	partitions := make([]int32, partitionCount)

	topic := source.ChangeLogTopicName()

	for i := int32(0); i < partitionCount; i++ {
		partitions[i] = i
		assignments[i] = kgo.NewOffset().AtStart()
	}
	client, err := NewClient(source.stateCluster(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: assignments,
		}),
		kgo.FetchMaxWait(time.Second),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		panic(err)
	}
	client.PauseFetchPartitions(map[string][]int32{
		topic: partitions,
	})

	for i := int32(0); i < partitionCount; i++ {
		stateStorePartitions[i] = &stateStorePartition[T]{
			topicPartition: TopicPartition{Partition: i, Topic: topic},
			waiters:        make(map[string]chan struct{}, 2),
			client:         client,
			state:          paused,
			buffer:         make(chan []*kgo.Record, 1024),
			highWatermark:  -1,
		}
	}
	ssc := &stateStoreConsumer[T]{
		source:     source,
		partitions: stateStorePartitions,
		client:     client,
		topic:      topic,
	}
	go ssc.consume()
	return ssc
}

func (ssc *stateStoreConsumer[T]) consume() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		f := ssc.client.PollFetches(ctx)
		cancel()
		if f.IsClientClosed() {
			log.Debugf("client closed")
			return
		}
		for _, fetchErr := range f.Errors() {
			if fetchErr.Err != ctx.Err() {
				log.Errorf("%v", fetchErr)
			}
		}
		f.EachPartition(func(partitionFetch kgo.FetchTopicPartition) {
			ssp := ssc.partitions[partitionFetch.Partition]
			if ssp.partitionState() == paused {
				ssp.pause()
			} else {
				ssp.add(partitionFetch)
			}
		})
	}
}

func (ssc *stateStoreConsumer[T]) cancelPartition(p int32) {
	ssc.mux.Lock()
	defer ssc.mux.Unlock()
	ssp := ssc.partitions[p]
	ssp.cancel()
}

func (ssc *stateStoreConsumer[T]) preparePartition(p int32, store changeLogPartition[T]) *stateStorePartition[T] {
	ssc.mux.Lock()
	defer ssc.mux.Unlock()
	ssp := ssc.partitions[p]
	ssp.prep(prepping, store)
	return ssp
}

func (ssc *stateStoreConsumer[T]) activatePartition(p int32, store changeLogPartition[T]) *stateStorePartition[T] {
	ssc.mux.Lock()
	defer ssc.mux.Unlock()
	ssp := ssc.partitions[p]
	ssp.prep(ready, store)
	return ssp
}

func (ssc *stateStoreConsumer[T]) stop() {
	ssc.client.Close()
	ssc.mux.Lock()
	defer ssc.mux.Unlock()
	for _, ssp := range ssc.partitions {
		ssp.kill()
	}
}

func sendMarkerMessage(producer *kgo.Client, tp TopicPartition, mark []byte) error {
	record := kgo.KeySliceRecord(markerKey, mark)
	record.Topic = tp.Topic
	record.Partition = tp.Partition
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: markerKeyString})
	log.Debugf("sending marker message to: %+v", tp)
	c := make(chan struct{})
	var err error
	producer.Produce(context.Background(), record, func(r *kgo.Record, e error) {
		err = e
		close(c)
	})
	<-c
	return err
}
