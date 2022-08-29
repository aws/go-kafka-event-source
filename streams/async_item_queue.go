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
)

const minWorkItemQueueSize = 2

/*
	This is a thread safe fifo queue implementation, implemented via a peakable buffer.
	It will actually report a size of maxSize + 1 items when there is a penging put.

	We're adding this to eliminate the FifoQueueFullError condition
	which currently results in a thread.Sleep().

	Instead, we'll provide a blocking fixed size buffer to provide backpressure.
	We're going to use a channel as this will decrease the number of allocations and type conversions
	that are required for linked list. Queue operations make no memory allocations.

	Obviously, in a multi-sender context, the separate go-routines will be racing for order, which may be OK depending on use case

	`done()` reports true if head and channel are empty and there are no pending writes to the channel

	see async_scheduler.go notes as to why we have a tryEnqueue and resumeEnqueue call
*/
type asyncItemQueue[T any] struct {
	size     int32
	queue    chan T
	headLock sync.Mutex
	head     T
	// we don't know if T is a pointer or a struct, so we may not be able to just return nil
	// use emptyItem to return frome peek()/dequeue() when queue is empty
	emptyItem T
	peekable  bool
}

func newAsyncItemQueue[T any](maxSize int) *asyncItemQueue[T] {
	if maxSize < minWorkItemQueueSize {
		maxSize = minWorkItemQueueSize
	}
	return &asyncItemQueue[T]{
		size:     0,
		peekable: false,
		queue:    make(chan T, maxSize-1),
	}
}

func (aiq *asyncItemQueue[T]) backfill() bool {
	select {
	case aiq.head = <-aiq.queue:
		aiq.peekable = true
	default:
		aiq.head = aiq.emptyItem
		aiq.peekable = false
	}
	return aiq.peekable
}

func (aiq *asyncItemQueue[T]) enqueueChannel() chan T {
	return aiq.queue
}

func (aiq *asyncItemQueue[T]) tryEnqueue(item T) bool {
	aiq.headLock.Lock()
	defer aiq.headLock.Unlock()
	aiq.size++
	if !aiq.peekable && !aiq.backfill() {
		aiq.head = item
		aiq.peekable = true
		return true
	}
	select {
	case aiq.queue <- item:
		return true
	default:
		return false
	}
}

func (aiq *asyncItemQueue[T]) dequeue() (T, bool) {
	aiq.headLock.Lock()
	defer aiq.headLock.Unlock()
	if !aiq.peekable && !aiq.backfill() {
		return aiq.emptyItem, false
	}
	aiq.size--
	item, res := aiq.head, aiq.peekable
	aiq.backfill()
	return item, res
}

func (aiq *asyncItemQueue[T]) peek() (T, bool) {
	aiq.headLock.Lock()
	defer aiq.headLock.Unlock()
	return aiq.head, aiq.peekable
}

func (aiq *asyncItemQueue[T]) done() bool {
	aiq.headLock.Lock()
	defer aiq.headLock.Unlock()
	return aiq.size <= 0
}
