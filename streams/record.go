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
	"time"
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
	jsoniter "github.com/json-iterator/go"
	"github.com/twmb/franz-go/pkg/kgo"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// The record.Header key that GKES uses to transmit type information about an IncomingRecord or a ChangeLogEntry.
const RecordTypeHeaderKey = "__grt__" // let's keep it small. every byte counts

const AutoAssign = int32(-1)

func recordSize(r kgo.Record) int {
	byteCount := len(r.Key)
	byteCount += len(r.Value)
	for _, h := range r.Headers {
		byteCount += len(h.Key)
		byteCount += len(h.Value)
	}
	return byteCount
}

type Record struct {
	keyBuffer   *bytes.Buffer
	valueBuffer *bytes.Buffer
	kRecord     kgo.Record
	recordType  string
	err         error
}

var recordPool = sak.NewPool(30000,
	func() *Record {
		return &Record{
			kRecord: kgo.Record{
				Partition: AutoAssign,
				Key:       nil,
				Value:     nil,
			},
			keyBuffer:   bytes.NewBuffer(nil),
			valueBuffer: bytes.NewBuffer(nil),
		}
	}, func(r *Record) *Record {
		//reset the record data
		r.kRecord = kgo.Record{
			Partition: AutoAssign,
			Key:       nil,
			Value:     nil,
		}
		if len(r.kRecord.Headers) > 0 {
			r.kRecord.Headers = r.kRecord.Headers[0:0]
		}
		r.keyBuffer.Reset()
		r.valueBuffer.Reset()
		r.recordType = ""
		r.err = nil
		return r
	})

func NewRecord() *Record {
	return recordPool.Borrow()
}

type IncomingRecord struct {
	kRecord    kgo.Record
	recordType string
}

func newIncomingRecord(incoming *kgo.Record) IncomingRecord {
	r := IncomingRecord{
		kRecord: *incoming,
	}
	for _, header := range incoming.Headers {
		if header.Key == RecordTypeHeaderKey {
			r.recordType = string(header.Value)
		}
	}
	return r
}

func (r IncomingRecord) Offset() int64 {
	return r.kRecord.Offset
}

func (r IncomingRecord) TopicPartition() TopicPartition {
	return ntp(r.kRecord.Partition, r.kRecord.Topic)
}

func (r IncomingRecord) LeaderEpoch() int32 {
	return r.kRecord.LeaderEpoch
}

func (r IncomingRecord) Timestamp() time.Time {
	return r.kRecord.Timestamp
}

func (r IncomingRecord) RecordType() string {
	return r.recordType
}

func (r IncomingRecord) Key() []byte {
	return r.kRecord.Key
}

func (r IncomingRecord) Value() []byte {
	return r.kRecord.Value
}

func (r IncomingRecord) Headers() []kgo.RecordHeader {
	return r.kRecord.Headers
}

func (r IncomingRecord) HeaderValue(name string) []byte {
	for _, v := range r.kRecord.Headers {
		if v.Key == name {
			return v.Value
		}
	}
	return nil
}

func (r IncomingRecord) isMarkerRecord() bool {
	return isMarkerRecord(&r.kRecord)
}

func (r *Record) Offset() int64 {
	return r.kRecord.Offset
}

func (r *Record) TopicPartition() TopicPartition {
	return ntp(r.kRecord.Partition, r.kRecord.Topic)
}

func (r *Record) WriteKey(bs ...[]byte) {
	for _, b := range bs {
		r.keyBuffer.Write(b)
	}
}

func (r *Record) WriteKeyString(ss ...string) {
	for _, s := range ss {
		r.keyBuffer.WriteString(s)
	}
}

func (r *Record) KeyWriter() *bytes.Buffer {
	return r.keyBuffer
}

func (r *Record) WriteValue(bs ...[]byte) {
	for _, b := range bs {
		r.valueBuffer.Write(b)
	}
}

func (r *Record) WriteValueString(ss ...string) {
	for _, s := range ss {
		r.valueBuffer.WriteString(s)
	}
}

func (r *Record) ValueWriter() *bytes.Buffer {
	return r.valueBuffer
}

func (r *Record) WithTopic(topic string) *Record {
	r.kRecord.Topic = topic
	return r
}

func (r *Record) WithKey(key ...[]byte) *Record {
	r.WriteKey(key...)
	return r
}

func (r *Record) WithKeyString(key ...string) *Record {
	r.WriteKeyString(key...)
	return r
}

func (r *Record) WithValue(value ...[]byte) *Record {
	r.WriteValue(value...)
	return r
}

func (r *Record) WithHeader(key string, value []byte) *Record {
	r.kRecord.Headers = append(r.kRecord.Headers, kgo.RecordHeader{Key: key, Value: value})
	return r
}

func (r *Record) WithRecordType(recordType string) *Record {
	r.recordType = recordType
	return r
}

func (r *Record) WithPartition(partition int32) *Record {
	r.kRecord.Partition = int32(partition)
	return r
}

func addRecordTypeHeader(recordType string, record *kgo.Record) {
	if len(recordType) == 0 {
		return
	}
	for _, header := range record.Headers {
		if header.Key == RecordTypeHeaderKey {
			return
		}
	}
	record.Headers = append(record.Headers, kgo.RecordHeader{
		Key:   RecordTypeHeaderKey,
		Value: []byte(recordType),
	})
}

// used internally for producing.
func (r *Record) toKafkaRecord() *kgo.Record {

	r.kRecord.Key = r.keyBuffer.Bytes()

	// an empty buffer should be a deletion
	// not sure if nil === empty for these purposes
	// so leaving nil to be sure
	if r.valueBuffer.Len() > 0 {
		r.kRecord.Value = r.valueBuffer.Bytes()
	}
	addRecordTypeHeader(r.recordType, &r.kRecord)

	// this record is already in the heap (it's part of the recordPool)
	// since we know that this pointer is guaranteed to outlive any produce calls
	// in the underlying kgo driver, let's prevent the compiler from escaping this
	// to the heap (again). this will significantly ease GC pressure
	// since we are producing a lot of records
	return (*kgo.Record)(sak.Noescape(unsafe.Pointer(&r.kRecord)))
}

// Creates a newly allocated kgo.Record. The Key and Value fields are freshly allocated bytes, copied from [streams.Record].
func (r *Record) ToKafkaRecord() *kgo.Record {
	record := new(kgo.Record)
	if r.keyBuffer.Len() > 0 {
		record.Key = append(record.Key, r.keyBuffer.Bytes()...)
	}

	if r.valueBuffer.Len() > 0 {
		record.Value = append(record.Value, r.valueBuffer.Bytes()...)
	}
	addRecordTypeHeader(r.recordType, &r.kRecord)
	return record
}

// A convenience function for unit testing. This method should not need to be invoked in a production code.
func (r *Record) AsIncomingRecord() IncomingRecord {
	return IncomingRecord{
		kRecord:    r.kRecord,
		recordType: r.recordType,
	}
}

func (r *Record) Error() error {
	return r.err
}

// A convenience function provided in case you are working with a raw kgo producer
// and want to integrate with streams. This will ensure that the EventSource will route the record to the proper handler
// without falling back to the defaultHandler
func SetRecordType(r *kgo.Record, recordType string) {
	r.Headers = append(r.Headers, kgo.RecordHeader{
		Key:   RecordTypeHeaderKey,
		Value: []byte(recordType),
	})
}

func (r *Record) Release() {
	recordPool.Release(r)
}

// ChangeLogEntry represents a Kafka record which wil be produced to the StateStore for your EventSource.
// Note that you can not set a topic or partition on a ChangeLogEntry. These values are managed by GKES.
type ChangeLogEntry struct {
	record *Record
}

func NewChangeLogEntry() ChangeLogEntry {
	return ChangeLogEntry{NewRecord()}
}

func (cle ChangeLogEntry) KeyWriter() *bytes.Buffer {
	return cle.record.KeyWriter()
}

func (cle ChangeLogEntry) ValueWriter() *bytes.Buffer {
	return cle.record.ValueWriter()
}

func (cle ChangeLogEntry) WriteKey(bs ...[]byte) {
	cle.record.WriteKey(bs...)
}

func (cle ChangeLogEntry) WriteKeyString(ss ...string) {
	cle.record.WriteKeyString(ss...)
}

func (cle ChangeLogEntry) WriteValue(bs ...[]byte) {
	cle.record.WriteValue(bs...)
}

func (cle ChangeLogEntry) WriteValueString(ss ...string) {
	cle.record.WriteValueString(ss...)
}

func (cle ChangeLogEntry) WithKey(key ...[]byte) ChangeLogEntry {
	cle.record.WriteKey(key...)
	return cle
}

func (cle ChangeLogEntry) WithKeyString(key ...string) ChangeLogEntry {
	cle.record.WriteKeyString(key...)
	return cle
}

func (cle ChangeLogEntry) WithValue(value ...[]byte) ChangeLogEntry {
	cle.record.WriteValue(value...)
	return cle
}

func (cle ChangeLogEntry) WithEntryType(entryType string) ChangeLogEntry {
	cle.record.recordType = entryType
	return cle
}

func (cle ChangeLogEntry) WithHeader(key string, value []byte) ChangeLogEntry {
	cle.record = cle.record.WithHeader(key, value)
	return cle
}

type OptionalPartitioner struct {
	manualPartitioner  kgo.Partitioner
	defaultPartitioner kgo.Partitioner
	topicPartitioners  map[string]kgo.Partitioner
}

type optionalTopicPartitioner struct {
	manualTopicPartitioner kgo.TopicPartitioner
	keyTopicPartitioner    kgo.TopicPartitioner
}

// A kgo compatible partitioner which respects Record partitions that are manually assigned.
// If the record partition is [AutoAssign], the provided kgo.Partitioner will be used for partition assignment.
// Note: [NewRecord] will return a record with a partition of [AutoAssign].
func NewOptionalPartitioner(partitioner kgo.Partitioner) OptionalPartitioner {
	return NewOptionalPerTopicPartitioner(partitioner, map[string]kgo.Partitioner{})
}

// A kgo compatible partitioner which respects Record partitions that are manually assigned.
// Allows you to set different partitioner per topic. If a topic is encountered that has not been defined, defaultPartitioner will be used.
func NewOptionalPerTopicPartitioner(defaultPartitioner kgo.Partitioner, topicPartitioners map[string]kgo.Partitioner) OptionalPartitioner {
	return OptionalPartitioner{
		manualPartitioner:  kgo.ManualPartitioner(),
		defaultPartitioner: defaultPartitioner,
		topicPartitioners:  topicPartitioners,
	}
}

func (op OptionalPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	partitioner := op.defaultPartitioner
	if p, ok := op.topicPartitioners[topic]; ok {
		partitioner = p
	}
	return optionalTopicPartitioner{
		manualTopicPartitioner: op.manualPartitioner.ForTopic(topic),
		keyTopicPartitioner:    partitioner.ForTopic(topic),
	}
}

func (otp optionalTopicPartitioner) RequiresConsistency(_ *kgo.Record) bool {
	return true
}

func (otp optionalTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition == AutoAssign {
		return otp.keyTopicPartitioner.Partition(r, n)
	}
	return otp.manualTopicPartitioner.Partition(r, n)
}

// A shortcut method for createing a ChangeLogEntry with a json endcoded value.
//
//	cle := CreateJsonChangeLogEntry(myValue).WithKeyString(myKey).WithEntryType(myType)
//	eventContext.RecordChange(cle)
func CreateJsonChangeLogEntry[T any](item T) (ChangeLogEntry, error) {
	return CreateChangeLogEntry[T](item, JsonCodec[T]{})
}

// A shortcut method for createing a ChangeLogEntry with a value endcoded using the provided codec.
//
//	cle := CreateChangeLogEntry(myValue, myCodec).WithKeyString(myKey).WithEntryType(myType)
//	eventContext.RecordChange(cle)
func CreateChangeLogEntry[T any](item T, codec Codec[T]) (ChangeLogEntry, error) {
	cle := NewChangeLogEntry()
	return cle, codec.Encode(cle.ValueWriter(), item)
}
