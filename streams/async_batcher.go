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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/aws/go-kafka-event-source/streams/sak"
)

type batchItemType int

const (
	normal batchItemType = iota
	placeholder
)

type BatchItem[K comparable, V any] struct {
	batch    unsafe.Pointer
	itemType batchItemType
	Key      K
	Value    V
	Err      error
	UserData any
}

func batchFor[S any, K comparable, V any](ptr unsafe.Pointer) *Batch[S, K, V] {
	return (*Batch[S, K, V])(ptr)
}

type Batch[S any, K comparable, V any] struct {
	eventContext *EventContext[S]
	Items        []BatchItem[K, V]
	UserData     any
	callback     BatchCallback[S, K, V]
	completed    int64
}

type BatchCallback[S any, K comparable, V any] func(*EventContext[S], *Batch[S, K, V]) ExecutionState

func NewBatch[S any, K comparable, V any](ec *EventContext[S], cb BatchCallback[S, K, V]) *Batch[S, K, V] {
	return &Batch[S, K, V]{
		eventContext: ec,
		callback:     cb,
	}
}

func (b *Batch[S, K, V]) executeCallback() ExecutionState {
	if b.callback != nil {
		return b.callback(b.eventContext, b)
	}
	return Complete
}

func (b *Batch[S, K, V]) completeItem() {
	if atomic.AddInt64(&b.completed, 1) == int64(len(b.Items)) {
		b.eventContext.AsyncJobComplete(b.executeCallback)
	}
}

func (b *Batch[S, K, V]) Add(items ...BatchItem[K, V]) *Batch[S, K, V] {
	for i := range items {
		items[i].itemType = normal
		items[i].batch = unsafe.Pointer(b)
	}
	b.Items = append(b.Items, items...)
	return b
}

func (b *Batch[S, K, V]) AddKeyValue(key K, value V) *Batch[S, K, V] {
	b.Items = append(b.Items, BatchItem[K, V]{batch: unsafe.Pointer(b), Key: key, Value: value})
	return b
}

type BatchExecutor[K comparable, V any] func(batch []*BatchItem[K, V])

type asyncBatchState int

const (
	batcherReady asyncBatchState = iota
	batcherExecuting
)

type asyncBatchExecutor[S any, K comparable, V any] struct {
	items      []*BatchItem[K, V]
	noops      []*Batch[S, K, V]
	state      asyncBatchState
	flushTimer *time.Timer
}

func (b *asyncBatchExecutor[S, K, V]) add(item *BatchItem[K, V]) {
	if item.itemType == placeholder {
		b.noops = append(b.noops, (*Batch[S, K, V])(item.batch))
	} else {
		b.items = append(b.items, item)
	}
}

func (b *asyncBatchExecutor[S, K, V]) reset(assignments map[K]*asyncBatchExecutor[S, K, V]) {
	for i, item := range b.items {
		delete(assignments, item.Key)
		b.items[i] = nil
	}
	for i := range b.noops {
		b.noops[i] = nil
	}
	b.items = b.items[0:0]
	b.noops = b.noops[0:0]
	b.state = batcherReady
}

type AsyncBatcher[S any, K comparable, V any] struct {
	executors      []*asyncBatchExecutor[S, K, V]
	assignments    map[K]*asyncBatchExecutor[S, K, V]
	pendingItems   *sak.List[*BatchItem[K, V]]
	executor       BatchExecutor[K, V]
	executingCount int
	maxBatchSize   int
	batchDelay     time.Duration
	mux            sync.Mutex
}

func NewAsyncBatcher[S StateStore, K comparable, V any](eventSource *EventSource[S], executor BatchExecutor[K, V], maxBatchSize, maxConcurrentBatches int, delay time.Duration) *AsyncBatcher[S, K, V] {
	executors := make([]*asyncBatchExecutor[S, K, V], maxConcurrentBatches)
	for i := range executors {
		executors[i] = &asyncBatchExecutor[S, K, V]{
			items: make([]*BatchItem[K, V], 0, maxBatchSize),
		}
	}

	if delay == 0 {
		delay = time.Millisecond * 5
	}
	return &AsyncBatcher[S, K, V]{
		executor:     executor,
		assignments:  make(map[K]*asyncBatchExecutor[S, K, V]),
		pendingItems: sak.NewList[*BatchItem[K, V]](),
		executors:    executors,
		maxBatchSize: maxBatchSize,
		batchDelay:   sak.Abs(delay),
	}
}

func (ab *AsyncBatcher[S, K, V]) Add(batch *Batch[S, K, V]) ExecutionState {
	if len(batch.Items) == 0 {
		/*
			since all events for a given key *must* travel through the async processor
			we need to add a placeholder in the cases where there are no items to process
			if we bypass the async scheduler in these situations, we could end up processing out of
			order if there are other async processes after this one. Additionally, if this is the last processor
			for a given event, we may deadlock because the original event may not be marked as complete
			if we add a batch with no items.

			Add a placeholder to ensure this doesn't happen.
		*/
		ab.add(&BatchItem[K, V]{batch: unsafe.Pointer(batch), itemType: placeholder})
	}
	for i := range batch.Items {
		ab.add(
			// ensure we don't escape to the heap
			(*BatchItem[K, V])(sak.Noescape(unsafe.Pointer(&batch.Items[i]))),
		)
	}
	return Incomplete
}

func (ab *AsyncBatcher[S, K, V]) add(bi *BatchItem[K, V]) {
	ab.mux.Lock()
	if asyncBatch := ab.asyncExecutorFor(bi); asyncBatch != nil {
		ab.addToExecutor(bi, asyncBatch)
	} else {
		ab.pendingItems.PushBack(bi)
	}
	ab.mux.Unlock()
}

func (ab *AsyncBatcher[S, K, V]) asyncExecutorFor(item *BatchItem[K, V]) *asyncBatchExecutor[S, K, V] {
	if batch, ok := ab.assignments[item.Key]; ok && batch.state == batcherReady {
		return batch
	} else if ok {
		// this key is currently in an executing batch, so we have to wait for it to finish
		return nil
	}
	for _, batch := range ab.executors {
		if batch.state == batcherReady {
			return batch
		}
	}
	return nil
}

func (ab *AsyncBatcher[S, K, V]) addToExecutor(item *BatchItem[K, V], executor *asyncBatchExecutor[S, K, V]) {
	ab.assignments[item.Key] = executor
	executor.add(item)

	if len(executor.items)+len(executor.noops) == ab.maxBatchSize {
		ab.conditionallyExecuteBatch(executor)
	} else if executor.flushTimer == nil {
		executor.flushTimer = time.AfterFunc(ab.batchDelay, func() {
			// we have a race condition where we could have reached max items
			ab.mux.Lock()
			ab.conditionallyExecuteBatch(executor)
			ab.mux.Unlock()
		})
	}
}

func (ab *AsyncBatcher[S, K, V]) conditionallyExecuteBatch(executor *asyncBatchExecutor[S, K, V]) {
	if executor.state == batcherReady {
		executor.state = batcherExecuting
		ab.executingCount++
		if executor.flushTimer != nil {
			executor.flushTimer.Stop()
			executor.flushTimer = nil
		}
		go ab.executeBatch(executor)
	}
}

func (ab *AsyncBatcher[S, K, V]) completeBatchItems(items []*BatchItem[K, V]) {
	for _, item := range items {
		batchFor[S, K, V](item.batch).completeItem()
	}
}

func (ab *AsyncBatcher[S, K, V]) executeBatch(executor *asyncBatchExecutor[S, K, V]) {
	if len(executor.items) > 0 {
		ab.executor(executor.items)
		ab.completeBatchItems(executor.items)
	}
	for _, b := range executor.noops {
		b.eventContext.AsyncJobComplete(b.executeCallback)
	}
	ab.mux.Lock()
	ab.executingCount--
	executor.reset(ab.assignments)
	ab.flushPendingItems()
	ab.mux.Unlock()
}

func (ab *AsyncBatcher[S, K, V]) flushPendingItems() {
	if ab.executingCount == len(ab.executors) {
		// there are no available batches, no need to continue in this loop
		return
	}
	for el := ab.pendingItems.Front(); el != nil; {
		if executor := ab.asyncExecutorFor(el.Value); executor != nil {
			ab.addToExecutor(el.Value, executor)
			tmp := el.Next()
			ab.pendingItems.Remove(el)
			el = tmp
			if ab.executingCount == len(ab.executors) {
				// there are no available batches, no need to continue in this loop
				return
			}
		} else {
			el = el.Next()
		}
	}
}
