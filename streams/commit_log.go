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
	"sync"
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/google/uuid"
)

type eosCommitLog struct {
	pendingSyncs  map[string]*sync.WaitGroup
	watermarks    map[int32]int64
	mux           sync.Mutex
	syncMux       sync.Mutex
	numPartitions int32
	topic         string
	changeLog     GlobalChangeLog[*eosCommitLog]
}

const intByteSize = int(unsafe.Sizeof(uintptr(1)))

func writeTopicPartitionToBytes(tp TopicPartition, b *bytes.Buffer) {
	var arr [intByteSize]byte
	*(*int64)(unsafe.Pointer(&arr[0])) = int64(tp.Partition)
	b.Write(arr[:])
	b.WriteString(tp.Topic)
}

func writeSignedIntToByteArray[T sak.Signed](i T, b *bytes.Buffer) {
	var arr [intByteSize]byte
	*(*int64)(unsafe.Pointer(&arr[0])) = int64(i)
	b.Write(arr[:])
}

func readIntegerFromByteArray[T sak.Signed](b []byte) T {
	return T(*(*int64)(unsafe.Pointer(&b[0])))
}

func topicPartitionFromBytes(b []byte) (tp TopicPartition) {
	tp.Partition = readIntegerFromByteArray[int32](b)
	tp.Topic = string(b[intByteSize:])
	return
}

func newEosCommitLog(source *Source, numPartitions int) *eosCommitLog {
	cl := &eosCommitLog{
		watermarks:    make(map[int32]int64),
		pendingSyncs:  make(map[string]*sync.WaitGroup),
		numPartitions: int32(numPartitions),
		topic:         source.CommitLogTopicNameForGroupId(),
	}
	cl.changeLog = NewGlobalChangeLog(source.stateCluster(), cl, numPartitions, cl.topic, CompactCleanupPolicy)
	return cl
}

func (cl *eosCommitLog) commitRecordPartition(tp TopicPartition) int32 {
	return tp.Partition % cl.numPartitions
}

func (cl *eosCommitLog) commitRecord(tp TopicPartition, offset int64) *Record {
	record := NewRecord().WithTopic(cl.topic).WithPartition(cl.commitRecordPartition(tp))
	writeTopicPartitionToBytes(tp, record.KeyWriter())
	// increment so we start consuming at the next offset
	writeSignedIntToByteArray(offset+1, record.ValueWriter())
	return record
}

func (cl *eosCommitLog) ReceiveChange(record IncomingRecord) error {
	if record.isMarkerRecord() {
		cl.closeSyncRequest(string(record.Value()))
	} else {
		tp := topicPartitionFromBytes(record.Key())
		offset := readIntegerFromByteArray[int64](record.Value())
		cl.mux.Lock()
		cl.watermarks[tp.Partition] = offset
		cl.mux.Unlock()
	}
	return nil
}

func (cl *eosCommitLog) Revoked() {}

func (cl *eosCommitLog) closeSyncRequest(mark string) {
	cl.syncMux.Lock()
	if wg, ok := cl.pendingSyncs[mark]; ok {
		delete(cl.pendingSyncs, mark)
		wg.Done()
	}
	cl.syncMux.Unlock()
}

func (cl *eosCommitLog) syncAll() {
	wg := &sync.WaitGroup{}
	for i := int32(0); i < cl.numPartitions; i++ {
		tp := ntp(i, cl.topic)
		wg.Add(1)
		go func() {
			cl.syncCommitLogPartition(tp)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (cl *eosCommitLog) lastProcessed(tp TopicPartition) int64 {
	cl.syncCommitLogPartition(ntp(cl.commitRecordPartition(tp), cl.topic))
	return cl.Watermark(tp)
}

func (cl *eosCommitLog) syncCommitLogPartition(tp TopicPartition) {
	cl.syncMux.Lock()
	mark := uuid.NewString()
	markWaiter := &sync.WaitGroup{}
	markWaiter.Add(1)
	cl.pendingSyncs[mark] = markWaiter
	cl.syncMux.Unlock()
	sendMarkerMessage(cl.changeLog.client, tp, []byte(mark))
	markWaiter.Wait()
}

func (cl *eosCommitLog) Watermark(tp TopicPartition) int64 {
	cl.mux.Lock()
	defer cl.mux.Unlock()
	if offset, ok := cl.watermarks[tp.Partition]; ok {
		return offset
	}
	return -1
}

func (cl *eosCommitLog) Stop() {
	cl.changeLog.Stop()
	cl.changeLog.client.Close()
}

func (cl *eosCommitLog) Start() {
	cl.changeLog.Start()
}
