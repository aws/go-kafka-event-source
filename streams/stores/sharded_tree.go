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

package stores

import (
	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/cespare/xxhash/v2"
	"github.com/google/btree"
)

type HashFunc[T any] func(T) uint64
type LessFunc[T any] btree.LessFunc[T]

// For string keys, use this for hashFunc argument to NewShardedTree
// Uses "github.com/cespare/xxhash/v2".Sum64String
func StringHash(s string) uint64 {
	return xxhash.Sum64String(s)
}

// For []byte keys, use this for hashFunc argument to NewShardedTree
// Uses "github.com/cespare/xxhash/v2".Sum64
func ByteHash(b []byte) uint64 {
	return xxhash.Sum64(b)
}

func StringLess(a, b string) bool {
	return a < b
}

func NumberLess[T sak.Number](a, b T) bool {
	return a < b
}

// A convenience data structure provided for storing large amounts of data in an in-memory StateStore.
// Simply wraps an array of github.com/google/btree#BTreeG[T].
// This is to help alleviate the O(log(n)) performance degerdation of a single, very large tree.
// If you need to store more than a million items in a single tree, you might consider using a ShardTree.
// There is an upfront O(1) performance hit for calculating the hash of key K when finding a tree.
// If you need ordering across your entire data set, this will not fit the bill.
type ShardedTree[K any, T any] struct {
	trees    []*btree.BTreeG[T]
	hashFunc HashFunc[K]
	mod      uint64
}

/*
Return a ShardedTree. The exponent argument is used to produce the number of shards as follows:

	shards := 2 << exponent

So an exponent of 10 will give you a ShardedTree with 2048 btree.Btree[T] shards.
This is to facilitate quicker shard look up with bitwise AND as opposed to a modulo, necessitating a []tree that has a length in an exponent of 2:

	mod := shards-1
	tree := trees[hashFunc(key)&st.mod]

Your use case wil determine what the proper number of shards is, but it is recommended to start small -> shards counts between 32-512 (exponents of 4-8).
`hashFunc` is used to find a the correct tree for a given key K
The `lessFunc`` argument mirrors that required by the "github.com/google/btree" package.
The trees in the data ShardedTree will share a common btree.FreeListG[T]
*/
func NewShardedTree[K any, T any](exponent int, hashFunc HashFunc[K], lessFunc LessFunc[T]) ShardedTree[K, T] {
	shards := 2 << exponent
	trees := make([]*btree.BTreeG[T], shards)
	freeList := btree.NewFreeListG[T](16)
	for i := 0; i < shards; i++ {
		trees[i] = btree.NewWithFreeListG(64, (btree.LessFunc[T])(lessFunc), freeList)
	}
	return ShardedTree[K, T]{
		trees:    trees,
		hashFunc: hashFunc,
		mod:      uint64(shards - 1),
	}
}

/*
Return the tree for key, invoking the supplied HashFunc[K]. If you're doing
multiple operations on the same tree, it makes sense to declare a variable:

	tree := shardeTree.For(item.key)
	tree.Delete(item)
	updateItem(item)
	tree.ReplaceOrInsert(item)
*/
func (st ShardedTree[K, T]) For(key K) *btree.BTreeG[T] {
	return st.trees[st.hashFunc(key)&st.mod]
}

// Iterates through all trees and sums their lengths. O(n) performance where n = 2 << exponent.
func (st ShardedTree[K, T]) Len() (l int) {
	for _, tree := range st.trees {
		l += tree.Len()
	}
	return
}
