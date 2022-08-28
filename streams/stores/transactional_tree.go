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
	"github.com/aws/go-kafka-event-source/streams/txn"
	"github.com/google/btree"
)

type TransactonalTree[T any] struct {
	committed *btree.BTreeG[T]
	live      *btree.BTreeG[T]
}

func NewTransactionalTree[T any](degree int, lessFunc LessFunc[T], freeList *btree.FreeListG[T]) *TransactonalTree[T] {
	if freeList == nil {
		freeList = btree.NewFreeListG[T](16)
	}
	return &TransactonalTree[T]{
		committed: btree.NewWithFreeListG(degree, (btree.LessFunc[T])(lessFunc), freeList),
	}
}

func (tt *TransactonalTree[T]) ForTxnContext(id uint64) (uint64, txn.Transactable) {
	if tt.live == nil {
		tt.live = tt.committed.Clone()
		id++
	}
	return id, tt
}

func (tt *TransactonalTree[T]) CurrentTxn() *btree.BTreeG[T] {
	return tt.live
}

func (tt *TransactonalTree[T]) Commit() error {
	if tt.live != nil {
		tt.committed.Clear(false)
		tt.committed = tt.live
		tt.live = nil
	}
	return nil
}

func (tt *TransactonalTree[T]) Abort() {
	if tt.live != nil {
		tt.live.Clear(false)
		tt.live = nil
	}
}
