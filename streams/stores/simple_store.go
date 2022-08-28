package stores

import (
	"github.com/aws/go-kafka-event-source/streams"
	"github.com/aws/go-kafka-event-source/streams/codec"
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
	var codec codec.JsonCodec[T]
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

func (s *SimpleStore[T]) ReceiveChange(record streams.IncomingRecord) {
	if len(record.Value()) == 0 {
		keyedValue := keyedValue[T]{
			key: string(record.Key()),
		}
		s.tree.Delete(&keyedValue)
	} else {
		item := codec.JsonItemDecoder[T](record)
		s.tree.ReplaceOrInsert(&keyedValue[T]{key: item.Key(), value: item})
	}
}

func (s *SimpleStore[T]) Revoked() {
	s.tree.Clear(false) // not really necessary
}
