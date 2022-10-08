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
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
	jsoniter "github.com/json-iterator/go"
)

type Codec[T any] interface {
	Encode(*bytes.Buffer, T) error
	Decode([]byte) (T, error)
}

var defaultJson = jsoniter.ConfigCompatibleWithStandardLibrary

// A convenience function for decoding an IncomingRecord.
// Conforms to streams.IncomingRecordDecoder interface needed for streams.RegisterEventType
//
//	streams.RegisterEventType(myEventSource, codec.JsonItemDecoder[myType], myHandler, "myType")
//	// or standalone
//	myDecoder := codec.JsonItemDecoder[myType]
//	myItem := myDecoder(incomingRecord)
func JsonItemDecoder[T any](record IncomingRecord) (T, error) {
	var codec JsonCodec[T]
	return codec.Decode(record.Value())
}

// A convenience function for encoding an item into a Record suitable for sending to a producer
// Please not that the Key on the record will be left uninitialized. Usage:
//
//	record := codec.JsonItemEncoder("myType", myItem)
//	record.WriteKeyString(myItem.Key)
func JsonItemEncoder[T any](recordType string, item T) *Record {
	var codec JsonCodec[T]
	record := NewRecord().WithRecordType(recordType)
	codec.Encode(record.ValueWriter(), item)
	return record
}

// A convenience function for encoding an item into a ChangeLogEntry suitable writing to a StateStore
// Please not that the Key on the entry will be left uninitialized. Usage:
//
//	entry := codec.EncodeJsonChangeLogEntryValue("myType", myItem)
//	entry.WriteKeyString(myItem.Key)
func EncodeJsonChangeLogEntryValue[T any](entryType string, item T) ChangeLogEntry {
	var codec JsonCodec[T]
	cle := NewChangeLogEntry().WithEntryType(entryType)
	codec.Encode(cle.ValueWriter(), item)
	return cle
}

type intCodec[T sak.Signed] struct{}

func (intCodec[T]) Encode(b *bytes.Buffer, i T) error {
	writeSignedIntToByteArray(i, b)
	return nil
}

func (intCodec[T]) Decode(b []byte) (T, error) {
	return readIntegerFromByteArray[T](b), nil
}

// Convenience codec for working with int types.
// Will never induce an error unless there is an OOM condition, so they are safe to ignore on Encode/Decode
var IntCodec = intCodec[int]{}

// Convenience codec for working with int64 types
// Will never induce an error unless there is an OOM condition, so they are safe to ignore on Encode/Decode
var Int64Codec = intCodec[int64]{}

// Convenience codec for working with int32 types
// Will never induce an error unless there is an OOM condition, so they are safe to ignore on Encode/Decode
var Int32Codec = intCodec[int32]{}

// A convenience Codec for integers where the encoded value is suitable for sorting in data structure which use
// []byte as keys (such as an LSM based db like BadgerDB or RocksDB). Useful if you need to persist items in order by timestamp
// or some other integer value.
// Decode will generate an error if the input []byte size is not [LexInt64Size].
var LexoInt64Codec = lexoInt64Codec{}

type lexoInt64Codec struct{}

const LexInt64Size = int(unsafe.Sizeof(uint64(1))) + 1

// Encodes the provided value. Will never induce an error unless there is an OOM condition, so it should be safe to ignore.
func (lexoInt64Codec) Encode(buf *bytes.Buffer, i int64) error {
	var b [LexInt64Size]byte
	if i > 0 {
		b[0] = 1
		binary.LittleEndian.PutUint64(b[1:], uint64(i))
	} else {
		binary.LittleEndian.PutUint64(b[1:], uint64(math.MaxInt64+i))
	}
	buf.Write(b[:])
	return nil
}

// Decodes the provided []byte. If len([]byte) is not equal to [LexInt64Size], an error will be generated.
func (lexoInt64Codec) Decode(b []byte) (int64, error) {
	if len(b) != LexInt64Size {
		return 0, fmt.Errorf("invalid lexo integer []byte length. Expected %d, actual: %d", LexInt64Size, len(b))
	}
	sign := b[0]
	val := int64(binary.LittleEndian.Uint64(b[1:]))
	if sign == 1 {
		return val, nil
	}
	return val - math.MaxInt64, nil
}

// A generic JSON en/decoder.
// Uses "github.com/json-iterator/go".ConfigCompatibleWithStandardLibrary for en/decoding JSON in a performant way
type JsonCodec[T any] struct{}

// Encodes the provided value.
func (JsonCodec[T]) Encode(b *bytes.Buffer, t T) error {
	stream := defaultJson.BorrowStream(b)
	defer defaultJson.ReturnStream(stream)
	stream.WriteVal(t)
	return stream.Flush()
}

// Decodes the provided []byte,
func (JsonCodec[T]) Decode(b []byte) (T, error) {
	iter := defaultJson.BorrowIterator(b)
	defer defaultJson.ReturnIterator(iter)

	var t T
	iter.ReadVal(&t)
	return t, iter.Error
}

type stringCodec struct{}

// Encodes the provide value. Will never induce an error unless there is an OOM condition, so it should be safe to ignore.
func (stringCodec) Encode(b *bytes.Buffer, s string) error {
	_, err := b.WriteString(s)
	return err
}

// Decodes the provide value. Will never induce an error so it is safe to ignore.
func (stringCodec) Decode(b []byte) (string, error) {
	return string(b), nil
}

// Convenience codec for working with strings.
var StringCodec Codec[string] = stringCodec{}

type byteCodec struct{}

// Encodes the provide value. Will never induce an error unless there is an OOM condition, so it should be safe to ignore on Encode/Decode
func (byteCodec) Encode(b *bytes.Buffer, v []byte) error {
	_, err := b.Write(v)
	return err
}

// Decodes the provide value. Will never induce an error so it is safe to ignore.
func (byteCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

// Convenience codec for working with raw `[]byte`s
var ByteCodec Codec[[]byte] = byteCodec{}
