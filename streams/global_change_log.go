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

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/twmb/franz-go/pkg/kgo"
)

type CleanupPolicy int

const (
	CompactCleanupPolicy CleanupPolicy = iota
	DeleteCleanupPolicy
)

type ChangeLogReceiver interface {
	ReceiveChange(IncomingRecord) error
}

// A GlobalChangeLog is simply a consumer which continously consumes all partitions within the given topic and
// forwards all records to it's StateStore. GlobalChangeLogs can be useful for sharing small amounts of data between
// a group of hosts. For example, GKES uses a global change log to keep track of consumer group offsets.
type GlobalChangeLog[T ChangeLogReceiver] struct {
	receiver      T
	client        *kgo.Client
	runStatus     sak.RunStatus
	numPartitions int
	topic         string
	cleanupPolicy CleanupPolicy
}

// Creates a NewGlobalChangeLog consumer and forward all records to `receiver`.
func NewGlobalChangeLog[T ChangeLogReceiver](cluster Cluster, receiver T, numPartitions int, topic string, cleanupPolicy CleanupPolicy) GlobalChangeLog[T] {
	return NewGlobalChangeLogWithRunStatus(sak.NewRunStatus(context.Background()), cluster, receiver, numPartitions, topic, cleanupPolicy)
}

// Creates a NewGlobalChangeLog consumer and forward all records to `receiver`.
func NewGlobalChangeLogWithRunStatus[T ChangeLogReceiver](runStatus sak.RunStatus, cluster Cluster, receiver T, numPartitions int, topic string, cleanupPolicy CleanupPolicy) GlobalChangeLog[T] {
	assignments := make(map[int32]kgo.Offset)
	for i := 0; i < numPartitions; i++ {
		assignments[int32(i)] = kgo.NewOffset().AtStart()
	}
	client, err := NewClient(
		cluster,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: assignments,
		}),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	if err != nil {
		panic(err)
	}

	return GlobalChangeLog[T]{
		runStatus:     runStatus,
		client:        client,
		receiver:      receiver,
		numPartitions: numPartitions,
		topic:         topic,
		cleanupPolicy: cleanupPolicy,
	}
}

// Pauses consumption of all partitions.
func (cl GlobalChangeLog[T]) PauseAllPartitions() {
	allPartitions := make([]int32, int(cl.numPartitions))
	for i := range allPartitions {
		allPartitions[i] = int32(i)
	}
	cl.client.PauseFetchPartitions(map[string][]int32{cl.topic: allPartitions})
}

// Pauses consumption of a partition.
func (cl GlobalChangeLog[T]) Pause(partition int32) {
	cl.client.PauseFetchPartitions(map[string][]int32{cl.topic: {partition}})
}

// Resumes consumption of a partition at offset.
func (cl GlobalChangeLog[T]) ResumePartitionAt(partition int32, offset int64) {
	cl.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		cl.topic: {partition: kgo.EpochOffset{
			Offset: offset,
			Epoch:  -1,
		}},
	})
	cl.client.ResumeFetchPartitions(map[string][]int32{cl.topic: {partition}})
}

func (cl GlobalChangeLog[T]) Stop() {
	cl.runStatus.Halt()
}

func (cl GlobalChangeLog[T]) Start() {
	go cl.consume()
}

func (cl GlobalChangeLog[T]) consume() {
	for cl.runStatus.Running() {
		ctx, f := pollConsumer(cl.client)
		if f.IsClientClosed() {
			log.Debugf("GlobalChangeLog client closed")
			return
		}
		for _, err := range f.Errors() {
			if err.Err != ctx.Err() {
				log.Errorf("%v", err)
			}
		}
		f.EachRecord(cl.forwardChange)
	}
	log.Debugf("GlobalChangeLog halted")
	cl.client.Close()
}

func (cl GlobalChangeLog[T]) forwardChange(r *kgo.Record) {
	ir := newIncomingRecord(r)
	if err := cl.receiver.ReceiveChange(ir); err != nil {
		log.Errorf("GlobalChangeLog error for %+v, offset: %d, recordType: %s, error: %v", ir.TopicPartition(), ir.Offset(), ir.RecordType(), err)
	}
}
