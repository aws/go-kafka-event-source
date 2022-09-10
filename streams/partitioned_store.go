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
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type changeLogPartition[T StateStore] struct {
	store T
	topic string
}

type changeLogData[T any] struct {
	store T
	topic string
}

func (sp *changeLogPartition[T]) grab() T {
	return sp.store
}

// we just need to untype this generic contstarint from StateStore to any
// to eas up some type gymnastics downstream
func (sp *changeLogPartition[T]) changeLogData() *changeLogData[T] {
	return &changeLogData[T]{
		store: sp.store,
		topic: sp.topic,
	}
}

func (sp *changeLogPartition[T]) release() {
}

func (sp *changeLogPartition[T]) receiveChangeInternal(record *kgo.Record) error {
	// this is only called during partition prep, so locking is not necessary
	// this will improve performance a bit
	err := sp.store.ReceiveChange(newIncomingRecord(record))
	if err != nil {
		log.Errorf("Error receiving change on topic: %s, partition: %d, offset: %d, err: %v",
			record.Topic, record.Partition, record.Offset, err)
	}
	return err
}

func (sp *changeLogPartition[T]) revokedInternal() {
	sp.grab().Revoked()
	sp.release()
}

type TopicPartitionCallback[T any] func(TopicPartition) T
type partitionedChangeLog[T StateStore] struct {
	data           map[int32]*changeLogPartition[T]
	factory        TopicPartitionCallback[T]
	changeLogTopic string
	mux            sync.Mutex
}

func newPartitionedChangeLog[T StateStore](factory TopicPartitionCallback[T], changeLogTopic string) *partitionedChangeLog[T] {
	return &partitionedChangeLog[T]{
		changeLogTopic: changeLogTopic,
		data:           make(map[int32]*changeLogPartition[T]),
		factory:        factory}
}

func (ps *partitionedChangeLog[T]) Len() int {
	return len(ps.data)
}

func (ps *partitionedChangeLog[T]) getStore(partition int32) (sp *changeLogPartition[T], ok bool) {
	ps.mux.Lock()
	defer ps.mux.Unlock()
	sp, ok = ps.data[partition]
	return
}

func (ps *partitionedChangeLog[T]) assign(partition int32) *changeLogPartition[T] {
	ps.mux.Lock()
	defer ps.mux.Unlock()
	var ok bool
	var sp *changeLogPartition[T]
	log.Debugf("PartitionedStore assigning %d", partition)
	if sp, ok = ps.data[partition]; !ok {
		sp = &changeLogPartition[T]{
			store: ps.factory(ntp(partition, ps.changeLogTopic)),
			topic: ps.changeLogTopic,
		}
		ps.data[partition] = sp
	}
	return sp
}

func (ps *partitionedChangeLog[T]) revoke(partition int32) {
	ps.mux.Lock()
	defer ps.mux.Unlock()
	log.Debugf("PartitionedStore revoking %d", partition)
	if store, ok := ps.data[partition]; ok {
		delete(ps.data, partition)
		store.revokedInternal()
	}
}
