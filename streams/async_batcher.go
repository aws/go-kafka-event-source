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
	key      K
	Value    V
	Err      error
	UserData any
}

func (bi BatchItem[K, V]) Key() K {
	return bi.key
}

func batchFor[S any, K comparable, V any](ptr unsafe.Pointer) *BatchItems[S, K, V] {
	return (*BatchItems[S, K, V])(ptr)
}

type BatchItems[S any, K comparable, V any] struct {
	eventContext *EventContext[S]
	key          K
	items        []BatchItem[K, V]
	UserData     any
	callback     BatchCallback[S, K, V]
	completed    int64
}

type BatchCallback[S any, K comparable, V any] func(*EventContext[S], *BatchItems[S, K, V]) ExecutionState

// Creates a container for BatchItems and ties them to an EventContext. Once all items in BatchItems.Items() have been executed,
// the provided BatchCallback will be executed.
func NewBatchItems[S any, K comparable, V any](ec *EventContext[S], key K, cb BatchCallback[S, K, V]) *BatchItems[S, K, V] {
	return &BatchItems[S, K, V]{
		eventContext: ec,
		key:          key,
		callback:     cb,
	}
}

func (b *BatchItems[S, K, V]) Key() K {
	return b.key
}

func (b *BatchItems[S, K, V]) Items() []BatchItem[K, V] {
	return b.items
}

func (b *BatchItems[S, K, V]) executeCallback() ExecutionState {
	if b.callback != nil {
		return b.callback(b.eventContext, b)
	}
	return Complete
}

func (b *BatchItems[S, K, V]) completeItem() {
	if atomic.AddInt64(&b.completed, 1) == int64(len(b.items)) {
		b.eventContext.AsyncJobComplete(b.executeCallback)
	}
}

// Adds items to BatchItems container. Values added in this method will inherit their key from the BatchItems container.
func (b *BatchItems[S, K, V]) Add(values ...V) *BatchItems[S, K, V] {
	for _, value := range values {
		b.items = append(b.items, BatchItem[K, V]{
			key:      b.key,
			Value:    value,
			itemType: normal,
			batch:    unsafe.Pointer(b),
		})
	}
	return b
}

// AddWithKey() is similar to Add(), but the items added do not inherit their key from the BatchItems.
// Useful for interjectors that may need to batch items that belong to multiple keys.
func (b *BatchItems[S, K, V]) AddWithKey(key K, values ...V) *BatchItems[S, K, V] {
	for _, value := range values {
		b.items = append(b.items, BatchItem[K, V]{
			key:      key,
			Value:    value,
			itemType: normal,
			batch:    unsafe.Pointer(b),
		})
	}
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
	noops      []*BatchItems[S, K, V]
	state      asyncBatchState
	flushTimer *time.Timer
}

func (b *asyncBatchExecutor[S, K, V]) add(item *BatchItem[K, V]) {
	if item.itemType == placeholder {
		b.noops = append(b.noops, (*BatchItems[S, K, V])(item.batch))
	} else {
		b.items = append(b.items, item)
	}
}

func (b *asyncBatchExecutor[S, K, V]) reset(assignments map[K]*asyncBatchExecutor[S, K, V]) {
	for i, item := range b.items {
		delete(assignments, item.key)
		b.items[i] = nil
	}
	for i := range b.noops {
		b.noops[i] = nil
	}
	b.items = b.items[0:0]
	b.noops = b.noops[0:0]
	b.state = batcherReady
}

/*
AsyncBatcher performs a similar function to the [AsyncJobScheduler],
but is intended for performing actions for multiple events at a time.
This is particularly useful when interacting with systems which provide a batch API.

For detailed examples, see https://github.com/aws/go-kafka-event-source/docs/asynprocessing.md
*/
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

// Create a new AsynBatcher. Each invocation of `executor` will have a maximum of `maxBatchSize` items.
// No more than `maxConcurrentBatches` will be executing at any given time. AsynBatcher will accumulate items until `delay` has elapsed,
// or `maxBatchSize` items have been received.
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

// Schedules items in BatchItems to be executed when capoacity is available.
func (ab *AsyncBatcher[S, K, V]) Add(batch *BatchItems[S, K, V]) ExecutionState {
	if len(batch.items) == 0 {
		/*
			since all events for a given key *must* travel through the async processor
			we need to add a placeholder in the cases where there are no items to process
			if we bypass the async scheduler in these situations, we could end up processing out of
			order if there are other async processes after this one. Additionally, if this is the last processor
			for a given event, we may deadlock because the original event may not be marked as complete
			if we add a batch with no items.

			Add a placeholder to ensure this doesn't happen.
		*/
		ab.add(&BatchItem[K, V]{batch: unsafe.Pointer(batch), key: batch.key, itemType: placeholder})
	}
	for i := range batch.items {
		ab.add(
			// ensure we don't escape to the heap
			(*BatchItem[K, V])(sak.Noescape(unsafe.Pointer(&batch.items[i]))),
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
	if batch, ok := ab.assignments[item.key]; ok && batch.state == batcherReady {
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
	ab.assignments[item.key] = executor
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
