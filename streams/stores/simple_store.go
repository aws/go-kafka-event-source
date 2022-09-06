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

package stores

import (
	"github.com/aws/go-kafka-event-source/streams"
	"github.com/google/btree"
)

type Keyed interface {
	Key() string
}

type keyedValue[T any] struct {
	key   string
	value T
}

func keyedLess[T Keyed](a, b *keyedValue[T]) bool {
	return a.key < b.key
}

type SimpleStore[T Keyed] struct {
	tree           *btree.BTreeG[*keyedValue[T]]
	topicPartition streams.TopicPartition
}

func NewSimpleStore[T Keyed](tp streams.TopicPartition) *SimpleStore[T] {
	return &SimpleStore[T]{
		tree:           btree.NewG(64, keyedLess[T]),
		topicPartition: tp,
	}
}

func (s *SimpleStore[T]) ToChangeLogEntry(item T) streams.ChangeLogEntry {
	var codec streams.JsonCodec[T]
	cle := streams.NewChangeLogEntry()
	cle.WriteKeyString(item.Key())
	codec.Encode(cle.ValueWriter(), item)
	return cle
}

func (s *SimpleStore[T]) Put(item T) streams.ChangeLogEntry {
	s.tree.ReplaceOrInsert(&keyedValue[T]{key: item.Key(), value: item})
	return s.ToChangeLogEntry(item)
}

func (s *SimpleStore[T]) Get(id string) (val T, ok bool) {
	var item *keyedValue[T]
	key := keyedValue[T]{
		key: id,
	}
	if item, ok = s.tree.Get(&key); ok {
		val = item.value
	}
	return
}

func (s *SimpleStore[T]) Delete(item T) (cle streams.ChangeLogEntry, ok bool) {
	keyedValue := keyedValue[T]{
		key: string(item.Key()),
	}
	if _, ok = s.tree.Delete(&keyedValue); ok {
		cle = streams.NewChangeLogEntry()
		cle.WriteKeyString(keyedValue.key)
	}
	return
}

func (s *SimpleStore[T]) ReceiveChange(record streams.IncomingRecord) (err error) {
	var item T
	if len(record.Value()) == 0 {
		keyedValue := keyedValue[T]{
			key: string(record.Key()),
		}
		s.tree.Delete(&keyedValue)
	} else if item, err = streams.JsonItemDecoder[T](record); err != nil {
		s.tree.ReplaceOrInsert(&keyedValue[T]{key: item.Key(), value: item})
	}
	return
}

func (s *SimpleStore[T]) Revoked() {
	s.tree.Clear(false) // not really necessary
}
