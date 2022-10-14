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
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/aws/go-kafka-event-source/streams/sak"
)

type asyncJobContainer[S any, K comparable, V any] struct {
	eventContext *EventContext[S]
	finalizer    AsyncJobFinalizer[S, K, V]
	key          K
	value        V
	err          error
}

func (ajc asyncJobContainer[S, K, V]) invokeFinalizer() ExecutionState {
	return ajc.finalizer(ajc.eventContext, ajc.key, ajc.value, ajc.err)
}

// A handler invoked when a previously scheduled AsyncJob should be performed.
type AsyncJobProcessor[K comparable, V any] func(K, V) error

// A callback invoked when a previously scheduled AsyncJob has been completed.
type AsyncJobFinalizer[T any, K comparable, V any] func(*EventContext[T], K, V, error) ExecutionState

type worker[S any, K comparable, V any] struct {
	capacity  int
	workQueue *asyncItemQueue[asyncJobContainer[S, K, V]]
	processor AsyncJobProcessor[K, V]
	depth     int64
	ctx       context.Context
	key       K
	no_key    K
}

func (w *worker[S, K, V]) reset() {
	w.key = w.no_key
}

func (w *worker[S, K, V]) tryAddItem(item asyncJobContainer[S, K, V]) bool {
	if w.workQueue.tryEnqueue(item) {
		atomic.AddInt64(&w.depth, 1)
		return true
	}
	return false
}

func (w *worker[S, K, V]) blockingAddItem(item asyncJobContainer[S, K, V]) {
	select {
	case w.workQueue.enqueueChannel() <- item:
		atomic.AddInt64(&w.depth, 1)
	case <-w.ctx.Done():
	}
}

func (w *worker[S, K, V]) dequeue() {
	w.workQueue.dequeue()
	atomic.AddInt64(&w.depth, -1)
}

func (w *worker[S, K, V]) advance() {
	w.dequeue()
}

func (w *worker[S, K, V]) process() {

	if w.ctx.Err() != nil {
		// worker is cancelled
		return
	}
	item, ok := w.workQueue.peek()
	if !ok {
		return
	}
	item.err = w.processor(item.key, item.value)
	w.advance()
	item.eventContext.AsyncJobComplete(item.invokeFinalizer)
}

/*
The AsyncJobScheduler provides a generic work scheduler/job serializer which takes a key/value as input via Schedule.
All work is organized into queues by 'key'. So for a given key, all work is serial allowing the use of
the single writer principle in an asynchronous fashion. In practice, it divides a stream partition into
it's individual keys and processes the keys in parallel.

After the the scheduling is complete for a key/value,
Scheduler will call the `processor` callback defined at initialization.
The output of this call will be passed to the `finalizer` callback.
If `finalizer` is nil, the event is marked as `Complete`, once the job is finished, ignoring any errors.

For detailed examples, see https://github.com/aws/go-kafka-event-source/docs/asynprocessing.md
*/
type AsyncJobScheduler[S StateStore, K comparable, V any] struct {
	runStatus         sak.RunStatus
	processor         AsyncJobProcessor[K, V]
	finalizer         AsyncJobFinalizer[S, K, V]
	workerFreeSignal  chan struct{}
	workerMap         map[K]*worker[S, K, V]
	workerChannel     chan *worker[S, K, V] // functions as a blocking queue
	workerQueueDepth  int64
	maxConcurrentKeys int
	mux               sync.Mutex
	updateRWLock      sync.RWMutex
	workerPool        sync.Pool
}

type SchedulerConfig struct {
	Concurrency, WorkerQueueDepth, MaxConcurrentKeys int
}

/* it does not make an sense to have less concurrent keys than max number of processors */
func (c SchedulerConfig) concurrentKeys() int {
	if c.Concurrency > c.MaxConcurrentKeys {
		return c.Concurrency
	}
	return c.MaxConcurrentKeys
}

var DefaultConfig = SchedulerConfig{
	Concurrency:       runtime.NumCPU(),
	WorkerQueueDepth:  1000,
	MaxConcurrentKeys: 10000,
}

var ComputeConfig = SchedulerConfig{
	Concurrency:       runtime.NumCPU(),
	WorkerQueueDepth:  1000,
	MaxConcurrentKeys: 10000,
}

var FastNetworkConfig = SchedulerConfig{
	Concurrency:       runtime.NumCPU() * 4,
	WorkerQueueDepth:  100,
	MaxConcurrentKeys: 10000,
}

var SlowNetworkConfig = SchedulerConfig{
	Concurrency:       runtime.NumCPU() * 16,
	WorkerQueueDepth:  100,
	MaxConcurrentKeys: 10000,
}

var WideNetworkConfig = SchedulerConfig{
	Concurrency:       runtime.NumCPU() * 32,
	WorkerQueueDepth:  1000,
	MaxConcurrentKeys: 10000,
}

// Creates an AsyncJobScheduler which is tied to the RunStatus of EventSource.
func CreateAsyncJobScheduler[S StateStore, K comparable, V any](
	eventSource *EventSource[S],
	processor AsyncJobProcessor[K, V],
	finalizer AsyncJobFinalizer[S, K, V],
	config SchedulerConfig) (*AsyncJobScheduler[S, K, V], error) {
	return NewAsyncJobScheduler(eventSource.runStatus.Fork(), processor, finalizer, config)
}

// Creates an AsyncJobScheduler which will continue to run while runStatus.Running()
func NewAsyncJobScheduler[S StateStore, K comparable, V any](
	runStatus sak.RunStatus,
	processor AsyncJobProcessor[K, V],
	finalizer AsyncJobFinalizer[S, K, V],
	config SchedulerConfig) (*AsyncJobScheduler[S, K, V], error) {

	if config.WorkerQueueDepth < 0 {
		return nil, errors.New("workerQueueDepth must be >= 0")
	}
	if config.Concurrency < 1 {
		return nil, errors.New("concurrency must be > 0")
	}
	if finalizer == nil {
		finalizer = func(ec *EventContext[S], k K, v V, err error) ExecutionState {
			return Complete
		}
	}
	maxConcurrentKeys := config.concurrentKeys()
	ap := &AsyncJobScheduler[S, K, V]{
		runStatus:         runStatus,
		processor:         processor,
		finalizer:         finalizer,
		workerQueueDepth:  int64(config.WorkerQueueDepth),
		workerFreeSignal:  make(chan struct{}, 1),
		workerMap:         make(map[K]*worker[S, K, V], maxConcurrentKeys),
		workerChannel:     make(chan *worker[S, K, V], maxConcurrentKeys+1),
		maxConcurrentKeys: maxConcurrentKeys,
	}

	ap.workerPool = sync.Pool{
		New: func() interface{} { return ap.newQueue() },
	}
	ap.warmup()

	for i := 0; i < config.Concurrency; i++ {
		go ap.work()
	}
	return ap, nil
}

func (ap *AsyncJobScheduler[S, K, V]) isClosed() bool {
	return !ap.runStatus.Running()
}

func (ap *AsyncJobScheduler[S, K, V]) queueDepth() int64 {
	return atomic.LoadInt64(&ap.workerQueueDepth)
}

func (ap *AsyncJobScheduler[S, K, V]) newQueue() interface{} {
	qd := int(ap.queueDepth())
	return &worker[S, K, V]{
		capacity:  qd,
		workQueue: newAsyncItemQueue[asyncJobContainer[S, K, V]](qd),
		processor: ap.processor,
		ctx:       ap.runStatus.Ctx(),
	}
}

// Schedules the value for processing in order by key. The finalizer will be invoked once processing is complete.
func (ap *AsyncJobScheduler[S, K, V]) Schedule(ec *EventContext[S], key K, value V) ExecutionState {
	if ap.isClosed() {
		return Complete
	}
	ap.scheduleItem(asyncJobContainer[S, K, V]{
		eventContext: ec,
		finalizer:    ap.finalizer,
		key:          key,
		value:        value,
		err:          nil,
	})
	return Incomplete
}

func (ap *AsyncJobScheduler[S, K, V]) scheduleItem(item asyncJobContainer[S, K, V]) {
	var w *worker[S, K, V] = nil
	var created bool
	added := false
	for w == nil {
		ap.mux.Lock()
		w, created = ap.grabWorker(item.key)
		if w == nil {
			ap.mux.Unlock()
			// wait until a worker thread finshes processing and try again
			ap.waitForWorker()
			if ap.isClosed() {
				return
			}
		}
	}
	/*
		we're in a bit of a pickle here.
		We need to record that we're adding an item to this key before we unlock the list of keys,
		otherwise we may end up orphaning a key's queue (adding an item while it's being released)

		but a blocking operation here will cause a deadlock

		so tell the worker to stay alive until an item has been added

	*/
	added = w.tryAddItem(item)
	ap.mux.Unlock()

	if !added {
		w.blockingAddItem(item)
	} else if created {
		ap.enqueueWorker(w)
	}
}

func (ap *AsyncJobScheduler[S, K, V]) work() {
	for {
		select {
		case wq := <-ap.workerChannel:
			if wq != nil {
				wq.process()
				ap.releaseWorker(wq)
			}
		case <-ap.runStatus.Done():
			// there may be routines publishing to or receiving from
			// ap.workerFreeSignal. If we close it, those that are publishing will cause a panic
			// so try to publish to close out any receivers.
			// if we can't, it means the channel is already full and we've done our job and any receivers will close
			select {
			case ap.workerFreeSignal <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (ap *AsyncJobScheduler[S, K, V]) SetWorkerQueueDepth(size int) {
	atomic.StoreInt64(&ap.workerQueueDepth, int64(size))
}

/*
Dynamically update the MaxConcurrentKeys for the current scheduler.
*/
func (ap *AsyncJobScheduler[S, K, V]) SetMaxConcurrentKeys(size int) {
	// prevent any action on workerChannel until this operation is complete
	ap.updateRWLock.Lock()
	defer ap.updateRWLock.Unlock()
	// this does not actually increase the number of workers, just makes room in the pending worker channel
	prevMaxKeys := ap.maxConcurrentKeys

	// lock here as grabWorker() uses this value, locked in release/scheduleWorker
	ap.mux.Lock()
	ap.maxConcurrentKeys = SchedulerConfig{MaxConcurrentKeys: size}.concurrentKeys()
	ap.mux.Unlock()
	ap.ensureWorkerChannelCapacity(size, prevMaxKeys)
}

func (ap *AsyncJobScheduler[S, K, V]) ensureWorkerChannelCapacity(newSize, oldSize int) {
	if newSize > oldSize {
		/*
			we need to make sure s.workerChannel capacity is > s.maxConcurrentKeys
			to avoid a deadlock. worker routines pull from this channel and may post back in the same thread
			if there is more work for a given key.

			In short, this channel needs to be able to fold at least s.maxConcurrentKeys at any given time
		*/
		wc := make(chan *worker[S, K, V], newSize+1)
		// transfer any pending workers to the new channel
		oldChan := ap.workerChannel
	pending:
		for i := 0; i < newSize; i++ {
			select {
			case w := <-oldChan:
				wc <- w
			default:
				break pending
			}
		}

		ap.workerChannel = wc
		close(oldChan)
	}
}

func (ap *AsyncJobScheduler[S, K, V]) waitForWorker() {
	<-ap.workerFreeSignal
}

func (ap *AsyncJobScheduler[S, K, V]) workerAvailable() {
	if ap.isClosed() {
		return
	}
	select {
	case ap.workerFreeSignal <- struct{}{}:
	default:
	}
}

func (ap *AsyncJobScheduler[S, K, V]) enqueueWorker(w *worker[S, K, V]) {
	// if ap.isClosed() {
	// 	return
	// }
	ap.updateRWLock.RLock()
	ap.workerChannel <- w
	ap.updateRWLock.RUnlock()
}

func (ap *AsyncJobScheduler[S, K, V]) warmup() {
	for i := 0; i < ap.maxConcurrentKeys; i++ {
		w := ap.workerPool.Get().(*worker[S, K, V])
		w.reset()
		ap.workerPool.Put(w)
	}
}

func (ap *AsyncJobScheduler[S, K, V]) releaseWorker(w *worker[S, K, V]) {
	// if ap.isClosed() {
	// 	return
	// }
	ap.mux.Lock()
	if w.workQueue.done() {
		delete(ap.workerMap, w.key)
		w.reset()
		ap.workerPool.Put(w)
		ap.mux.Unlock()
		ap.workerAvailable()
		return
	}
	ap.mux.Unlock()
	ap.enqueueWorker(w)
}

func (ap *AsyncJobScheduler[S, K, V]) grabWorker(key K) (*worker[S, K, V], bool) {
	var w *worker[S, K, V]
	var ok bool
	if w, ok = ap.workerMap[key]; !ok {
		if len(ap.workerMap) >= ap.maxConcurrentKeys {
			return nil, false
		}
		for w = ap.workerPool.Get().(*worker[S, K, V]); w.capacity != int(ap.queueDepth()); {
			// we've updated the workerQueueDepth, exhaust the pool until we create a new one
			w = ap.workerPool.Get().(*worker[S, K, V])
		}
		w.key = key
		ap.workerMap[key] = w
	}
	return w, !ok
}
