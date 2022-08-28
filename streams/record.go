package streams

import (
	"bytes"
	"sync"
	"time"
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
	jsoniter "github.com/json-iterator/go"
	"github.com/twmb/franz-go/pkg/kgo"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// The record.Header key that gstreams uses to transmit type information about an IncomingRecord or a ChangeLogEntry.
const RecordTypeHeaderKey = "__grt__" // let's keep it small. every byte counts

const AutoAssign = int32(-1)

var reusableRecordPool = sync.Pool{
	New: func() any {
		return &Record{
			keyBuffer:   bytes.NewBuffer(nil),
			valueBuffer: bytes.NewBuffer(nil),
		}
	},
}

type Record struct {
	keyBuffer   *bytes.Buffer
	valueBuffer *bytes.Buffer
	kRecord     kgo.Record
	recordType  string
}

func NewRecord() *Record {
	return reusableRecordPool.Get().(*Record)
}

func newIncomingRecord(incoming *kgo.Record) IncomingRecord {
	r := IncomingRecord{
		kRecord: incoming,
	}
	for _, header := range incoming.Headers {
		if header.Key == RecordTypeHeaderKey {
			r.recordType = string(header.Value)
		}
	}
	return r
}

type IncomingRecord struct {
	kRecord    *kgo.Record
	recordType string
}

func (r IncomingRecord) Offset() int64 {
	return r.kRecord.Offset
}

func (r IncomingRecord) TopicPartition() TopicPartition {
	return TopicPartition{
		Partition: r.kRecord.Partition,
		Topic:     r.kRecord.Topic,
	}
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

func (r IncomingRecord) isMarkerRecord() bool {
	return len(r.kRecord.Headers) == 1 && r.kRecord.Headers[0].Key == "__gstreams__mark"
}

func (r *Record) Offset() int64 {
	return r.kRecord.Offset
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

func (r *Record) WithRecordType(recordType string) *Record {
	r.recordType = recordType
	return r
}

func (r *Record) WithPartition(partition int32) *Record {
	r.kRecord.Partition = int32(partition)
	return r
}

func (r *Record) ToKafkaRecord() *kgo.Record {
	r.kRecord.Key = r.keyBuffer.Bytes()

	// an empty buffer should be a deletion
	// not sure if nil === empty for these purposes
	// so leaving nil to be sure
	if valueBytes := r.valueBuffer.Bytes(); len(valueBytes) != 0 {
		r.kRecord.Value = valueBytes
	}

	if len(r.recordType) > 0 {
		r.kRecord.Headers = append(r.kRecord.Headers, kgo.RecordHeader{
			Key:   RecordTypeHeaderKey,
			Value: []byte(r.recordType),
		})
	}
	// this record is already in the heap (it's part of the recordPool)
	// since we know that this pointer is guaranteed to outlive any produce calls
	// in the underlying kgo driver, let's prevent the compiler from escaping this
	// to the heap (again). this will significantly ease GC pressure
	// since we are producing a lot of records
	return (*kgo.Record)(sak.Noescape(unsafe.Pointer(&r.kRecord)))
}

// A convenince method provided in case you are working with a raw kgo producer
// and want to integrate with streams. This will ensure that the EventSource will route the record to the proper handler
// without falling back to the defaultHandler
func SetRecordType(r *kgo.Record, recordType string) {
	r.Headers = append(r.Headers, kgo.RecordHeader{
		Key:   RecordTypeHeaderKey,
		Value: []byte(recordType),
	})
}

func (r *Record) release() {
	r.kRecord = kgo.Record{
		Partition: AutoAssign,
		Key:       nil,
		Value:     nil,
	}
	if len(r.kRecord.Headers) > 0 {
		r.kRecord.Headers = r.kRecord.Headers[0:0]
	}
	//reset the record data
	r.keyBuffer.Reset()
	r.valueBuffer.Reset()
	r.recordType = ""
	reusableRecordPool.Put(r)
}

type ChangeLogEntry struct {
	record *Record
}

func NewChangeLogEntry() ChangeLogEntry {
	return ChangeLogEntry{NewRecord()}
}

func (cle ChangeLogEntry) WriteKey(bs ...[]byte) {
	cle.record.WriteKey(bs...)
}

func (cle ChangeLogEntry) WriteKeyString(ss ...string) {
	cle.record.WriteKeyString(ss...)
}

func (cle ChangeLogEntry) KeyWriter() *bytes.Buffer {
	return cle.record.KeyWriter()
}

func (cle ChangeLogEntry) WriteValue(bs ...[]byte) {
	cle.record.WriteValue(bs...)
}

func (cle ChangeLogEntry) WriteValueString(ss ...string) {
	cle.record.WriteValueString(ss...)
}

func (cle ChangeLogEntry) ValueWriter() *bytes.Buffer {
	return cle.record.ValueWriter()
}

func (cle ChangeLogEntry) WithEntryType(entryType string) ChangeLogEntry {
	cle.record.recordType = entryType
	return cle
}

type OptionalPartitioner struct {
	manualPartitioner kgo.Partitioner
	keyPartitioner    kgo.Partitioner
}

type optionalTopicPartitioner struct {
	manualTopicPartitioner kgo.TopicPartitioner
	keyTopicPartitioner    kgo.TopicPartitioner
}

func NewOptionalPartitioner(p kgo.Partitioner) OptionalPartitioner {
	return OptionalPartitioner{
		manualPartitioner: kgo.ManualPartitioner(),
		keyPartitioner:    p,
	}
}

func (op OptionalPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return optionalTopicPartitioner{
		manualTopicPartitioner: op.manualPartitioner.ForTopic(topic),
		keyTopicPartitioner:    op.keyPartitioner.ForTopic(topic),
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
