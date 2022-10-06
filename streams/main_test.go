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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/google/btree"
	"github.com/google/uuid"
)

const kafkaProgramScript = "kafka_local/exec-kafka-script.sh"
const kafkaCleanupScript = "kafka_local/cleanup.sh"
const kafkaDownloadScript = "kafka_local/download-kafka.sh"
const kafkaWorkingDir = "kafka_local/kafka"

func TestMain(m *testing.M) {
	flag.Parse()
	InitLogger(SimpleLogger(LogLevelError), LogLevelError)
	if testing.Short() {
		os.Exit(m.Run())
		return
	}
	// cleanup data logs in case we exited abnormally
	if err := exec.Command("sh", kafkaCleanupScript).Run(); err != nil {
		fmt.Println(err)
	}

	// download binaly distribution of kafka if necessary
	if err := exec.Command("sh", kafkaDownloadScript, kafkaWorkingDir).Run(); err != nil {
		fmt.Println(err)
	}

	// start zookeeper and broker asynchronously
	zookeeper := kafkaScriptCommand("zookeeper", "start")
	kafka := kafkaScriptCommand("kafka", "start")
	if err := zookeeper.Start(); err != nil {
		fmt.Println("zookeeper: ", err)
	}
	if err := kafka.Start(); err != nil {
		fmt.Println("broker: ", err)
	}
	time.Sleep(5 * time.Second) // give some time for Kafak to warm up

	// run our tests
	code := m.Run()

	// stop zookeeper and broker
	if err := kafkaScriptCommand("zookeeper", "stop").Run(); err != nil {
		fmt.Println("zookeeper: ", err)
	}
	if err := kafkaScriptCommand("kafka", "stop").Run(); err != nil {
		fmt.Println("kafka: ", err)
	}

	// give it a second then clean up data logs
	time.Sleep(time.Second)
	if err := exec.Command("sh", kafkaCleanupScript).Run(); err != nil {
		fmt.Println(err)
	}
	os.Exit(code)
}

func kafkaScriptCommand(program, command string) *exec.Cmd {
	return exec.Command("sh", kafkaProgramScript, kafkaWorkingDir, program, command)
}

type intStoreItem struct {
	Key, Value int
}

func (isi intStoreItem) encodeKey(cle ChangeLogEntry) {
	IntCodec.Encode(cle.KeyWriter(), isi.Key)
}

func (isi intStoreItem) encodeValue(cle ChangeLogEntry) {
	IntCodec.Encode(cle.ValueWriter(), isi.Key)
}

func intStoreItemLess(a, b intStoreItem) bool {
	return a.Key < b.Key
}

type intStore struct {
	tree *btree.BTreeG[intStoreItem]
}

func NewIntStore(TopicPartition) intStore {
	return intStore{btree.NewG(64, intStoreItemLess)}
}

func (s *intStore) decodeRecord(r IncomingRecord) (item intStoreItem, ok bool) {
	item.Key = sak.Must(IntCodec.Decode(r.Key()))
	if len(r.Value()) > 0 {
		item.Value = sak.Must(IntCodec.Decode(r.Value()))
		ok = true
	}
	return
}

func (s intStore) add(item intStoreItem) {
	s.tree.ReplaceOrInsert(item)
}

func (s intStore) del(item intStoreItem) {
	s.tree.Delete(item)
}

func (s intStore) ReceiveChange(r IncomingRecord) error {
	if item, ok := s.decodeRecord(r); ok {
		s.add(item)
	} else {
		s.del(item)
	}
	return nil
}

func (s intStore) Revoked() {
	s.tree.Clear(false)
}

var testCluster = SimpleCluster([]string{"127.0.0.1:9092"})

func testTopicConfig() EventSourceConfig {
	topicName := uuid.NewString()
	return EventSourceConfig{
		GroupId:           topicName + "_group",
		Topic:             topicName,
		NumPartitions:     10,
		ReplicationFactor: 1,
		MinInSync:         1,
		SourceCluster:     testCluster,
	}
}

func defaultTestHandler(ec *EventContext[intStore], ir IncomingRecord) ExecutionState {
	s := ec.Store()
	cle := NewChangeLogEntry().WithEntryType("defaultHandler")
	if item, ok := s.decodeRecord(ir); ok {
		s.add(item)
		item.encodeKey(cle)
		item.encodeValue(cle)
	} else {
		item.encodeKey(cle)
		s.del(item)
	}
	ec.RecordChange(cle)
	return Complete
}

type testProducer struct {
	producer *Producer
}

func (p testProducer) signal(t *testing.T, v string, partition int32) {
	p.producer.ProduceAsync(context.TODO(),
		NewRecord().WithRecordType("verify").WithValue([]byte(v)).WithPartition(partition),
		func(r *Record, err error) {
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
		})
}

func (p testProducer) produce(t *testing.T, recordType string, k, v int) {
	r := NewRecord().
		WithRecordType(recordType).
		WithPartition(int32(k % 10))
	IntCodec.Encode(r.KeyWriter(), k)
	IntCodec.Encode(r.ValueWriter(), v)
	p.producer.ProduceAsync(context.TODO(),
		r,
		func(r *Record, err error) {
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
		})
}

func (p testProducer) delete(t *testing.T, recordType string, k int) {
	r := NewRecord().
		WithRecordType(recordType).
		WithPartition(int32(k % 10))
	IntCodec.Encode(r.KeyWriter(), k)
	p.producer.ProduceAsync(context.TODO(),
		r,
		func(r *Record, err error) {
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
		})
}

func (p testProducer) produceMany(t *testing.T, recordType string, count int) {
	for i := 0; i < count; i++ {
		p.produce(t, recordType, i, i)
	}
}

func (p testProducer) deleteMany(t *testing.T, recordType string, count int) {
	for i := 0; i < count; i++ {
		p.delete(t, recordType, i)
	}
}

func newTestEventSource() (*EventSource[intStore], testProducer, <-chan string) {
	c := make(chan string)
	cfg := testTopicConfig()

	es := sak.Must(NewEventSource(cfg, NewIntStore, defaultTestHandler))
	producer := NewProducer(es.source.AsDestination())

	RegisterEventType(es, func(ir IncomingRecord) (string, error) {
		return string(ir.Value()), nil
	}, func(ec *EventContext[intStore], v string) ExecutionState {
		c <- v
		return Complete
	}, "verify")
	return es, testProducer{producer}, c
}

const defaultTestTimeout = 30 * time.Second

func waitForVerificationSignal(t *testing.T, c <-chan string, timeout time.Duration) (string, bool) {
	if timeout == 0 {
		timeout = defaultTestTimeout
	}
	timer := time.NewTimer(timeout)
	select {
	case s := <-c:
		return s, true
	case <-timer.C:
		t.Errorf("deadline exceeded")
		t.FailNow()
		return "", false
	}
}

func (p testProducer) waitForAllPartitions(t *testing.T, c <-chan string, timeout time.Duration) bool {
	partitionCount := p.producer.destination.NumPartitions
	for partition := 0; partition < partitionCount; partition++ {
		p.waitForPartition(t, c, timeout, int32(partition))
	}
	return true
}

func (p testProducer) waitForPartition(t *testing.T, c <-chan string, timeout time.Duration, partition int32) (string, bool) {
	p.signal(t, "waitForPartition", int32(partition))
	return waitForVerificationSignal(t, c, timeout)
}
