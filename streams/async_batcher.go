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
	"sync"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
)

type BatchItem[S StateStore, K comparable, V any] struct {
	eventContext *EventContext[S]
	key          K
	item         V
}

func (bi BatchItem[S, K, V]) EventContext() *EventContext[S] {
	return bi.eventContext
}

func (bi BatchItem[S, K, V]) Key() K {
	return bi.key
}

func (bi BatchItem[S, K, V]) Item() V {
	return bi.item
}

type BatchExecutor[S StateStore, K comparable, V any] func(batch []BatchItem[S, K, V])

type asyncBatchState int

const (
	ready asyncBatchState = iota
	executing
)

type asyncBatch[S StateStore, K comparable, V any] struct {
	items      []BatchItem[S, K, V]
	state      asyncBatchState
	flushTimer *time.Timer
}

func (b *asyncBatch[S, K, V]) add(item BatchItem[S, K, V]) {
	b.items = append(b.items, item)
}

func (b *asyncBatch[S, K, V]) reset(assignments map[K]*asyncBatch[S, K, V]) {
	var empty BatchItem[S, K, V]
	for i, item := range b.items {
		delete(assignments, item.key)
		b.items[i] = empty
	}
	b.items = b.items[0:0]
	b.state = ready
}

type AsyncBatcher[S StateStore, K comparable, V any] struct {
	batches        []*asyncBatch[S, K, V]
	assignments    map[K]*asyncBatch[S, K, V]
	pendingItems   *sak.List[BatchItem[S, K, V]]
	executor       BatchExecutor[S, K, V]
	executingCount int
	MaxBatchSize   int
	MinBatchSize   int
	BatchDelay     time.Duration
	mux            sync.Mutex
}

func NewAsyncBatcher[S StateStore, K comparable, V any](eventSource *EventSource[S], executor BatchExecutor[S, K, V], minBatchSize, maxBatchSize, maxConcurrentBatches int) *AsyncBatcher[S, K, V] {
	batches := make([]*asyncBatch[S, K, V], maxConcurrentBatches)
	for i := range batches {
		batches[i] = &asyncBatch[S, K, V]{
			items: make([]BatchItem[S, K, V], 0, maxBatchSize),
		}
	}
	return &AsyncBatcher[S, K, V]{
		executor:     executor,
		assignments:  make(map[K]*asyncBatch[S, K, V]),
		pendingItems: sak.NewList[BatchItem[S, K, V]](),
		batches:      batches,
		MaxBatchSize: maxBatchSize,
		MinBatchSize: minBatchSize,
		BatchDelay:   time.Millisecond * 5,
	}
}

func (ab *AsyncBatcher[S, K, V]) Add(ec *EventContext[S], key K, item V) {
	bi := BatchItem[S, K, V]{
		eventContext: ec,
		key:          key,
		item:         item,
	}

	ab.mux.Lock()
	if batch := ab.batchFor(bi); batch != nil {
		ab.addToBatch(bi, batch)
	} else {
		ab.pendingItems.PushBack(bi)
	}
	ab.mux.Unlock()
}

func (ab *AsyncBatcher[S, K, V]) batchFor(item BatchItem[S, K, V]) *asyncBatch[S, K, V] {
	if batch, ok := ab.assignments[item.key]; ok && batch.state == ready {
		return batch
	} else if ok {
		// this key is currently in an executing batch, so we have to wait for it to finish
		return nil
	}
	for _, batch := range ab.batches {
		if batch.state == ready {
			return batch
		}
	}
	return nil
}

func (ab *AsyncBatcher[S, K, V]) addToBatch(item BatchItem[S, K, V], batch *asyncBatch[S, K, V]) {
	ab.assignments[item.key] = batch
	batch.add(item)

	if len(batch.items) == ab.MaxBatchSize {
		ab.conditionallyExecuteBatch(batch)
	} else if batch.flushTimer == nil {
		batch.flushTimer = time.AfterFunc(ab.BatchDelay, func() {
			// we have a race condition where we could have reached max items
			ab.mux.Lock()
			ab.conditionallyExecuteBatch(batch)
			ab.mux.Unlock()
		})
	}
}

func (ab *AsyncBatcher[S, K, V]) conditionallyExecuteBatch(batch *asyncBatch[S, K, V]) {
	if batch.state == ready {
		batch.state = executing
		ab.executingCount++
		if batch.flushTimer != nil {
			batch.flushTimer.Stop()
			batch.flushTimer = nil
		}
		go ab.executeBatch(batch)
	}
}

func (ab *AsyncBatcher[S, K, V]) executeBatch(batch *asyncBatch[S, K, V]) {
	ab.executor(batch.items)
	ab.mux.Lock()
	ab.executingCount--
	// TODO: handle errors right here as this may effect other batches
	batch.reset(ab.assignments)
	ab.flushPendingItems()
	ab.mux.Unlock()
}

func (ab *AsyncBatcher[S, K, V]) flushPendingItems() {
	el := ab.pendingItems.Front()
	for el != nil {
		if batch := ab.batchFor(el.Value); batch != nil {
			ab.addToBatch(el.Value, batch)
			if ab.executingCount == len(ab.batches) {
				// there are no available batches, no need to continue in this loop
				return
			}
			tmp := el.Next()
			ab.pendingItems.Remove(el)
			el = tmp
		} else {
			el = el.Next()
		}
	}
}
