// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package codec

import (
	"bytes"

	"github.com/aws/go-kafka-event-source/streams"
	jsoniter "github.com/json-iterator/go"
)

type Encode[T any] func(*bytes.Buffer, T)
type Decode[T any] func([]byte) T

type Codec[T any] interface {
	Encode(*bytes.Buffer, T)
	Decode([]byte) T
}

var defaultJson = jsoniter.ConfigCompatibleWithStandardLibrary

func ItemDecoder[T any, C Codec[T]](record streams.IncomingRecord) T {
	var codec C
	return codec.Decode(record.Value())
}

func ItemEncoder[T any, C Codec[T]](record streams.IncomingRecord) T {
	var codec C
	return codec.Decode(record.Value())
}

// A convenience function for decoding an IncomingRecord.
// Conforms to streams.IncomingRecordDecoder interface needed for streams.RegisterEventType
//  streams.RegisterEventType(myEventSource, codec.JsonItemDecoder[myType], myHandler, "myType")
//  // or standalone
//  myDecoder := codec.JsonItemDecoder[myType]
//  myItem := myDecoder(incomingRecord)
func JsonItemDecoder[T any](record streams.IncomingRecord) T {
	var codec JsonCodec[T]
	return codec.Decode(record.Value())
}

// A convenience function for encoding an item into a Record suitable for sending to a producer
// Please not that the Key on the record will be left uninitialized. Usage:
//  record := codec.JsonItemEncoder("myType", myItem)
//  record.WriteKeyString(myItem.Key)
func JsonItemEncoder[T any](recordType string, item T) *streams.Record {
	var codec JsonCodec[T]
	record := streams.NewRecord().WithRecordType(recordType)
	codec.Encode(record.ValueWriter(), item)
	return record
}

// A convenience function for encoding an item into a ChangeLogEntry suitable writing to a StateStore
// Please not that the Key on the entry will be left uninitialized. Usage:
//  entry := codec.JsonChangeLogEntryEncoder("myType", myItem)
//  entry.WriteKeyString(myItem.Key)
func JsonChangeLogEntryEncoder[T any](entryType string, item T) streams.ChangeLogEntry {
	var codec JsonCodec[T]
	cle := streams.NewChangeLogEntry().WithEntryType(entryType)
	codec.Encode(cle.ValueWriter(), item)
	return cle
}

// A generic JSON en/decoder. =
// Uses "github.com/json-iterator/go".ConfigCompatibleWithStandardLibrary for en/decoding JSON in a perforamnt way
type JsonCodec[T any] struct{}

func (JsonCodec[T]) Encode(b *bytes.Buffer, t T) {
	stream := defaultJson.BorrowStream(b)
	defer defaultJson.ReturnStream(stream)
	stream.WriteVal(t)
	stream.Flush()
}

func (JsonCodec[T]) Decode(b []byte) T {
	iter := defaultJson.BorrowIterator(b)
	defer defaultJson.ReturnIterator(iter)

	var t T
	iter.ReadVal(&t)
	return t
}

type stringCodec struct{}

func (stringCodec) Encode(b *bytes.Buffer, s string) {
	b.WriteString(s)
}

func (stringCodec) Decode(b []byte) string {
	return string(b)
}

var StringCodec Codec[string] = stringCodec{}

type byteCodec struct{}

func (byteCodec) Encode(b *bytes.Buffer, v []byte) {
	b.Write(v)
}

func (byteCodec) Decode(b []byte) []byte {
	return b
}

var ByteCodec Codec[[]byte] = byteCodec{}
