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
	"sync"
	"testing"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
)

func TestAsyncJobSchedulerOrdering(t *testing.T) {

	runStatus := sak.NewRunStatus(context.Background())
	defer runStatus.Halt()
	done := make(chan struct{}, 100)

	ordered := make(map[int]int)
	mapLock := &sync.Mutex{}
	scheduler, err := NewAsyncJobScheduler(runStatus, func(int, int) error {
		time.Sleep(time.Millisecond)
		return nil
	}, func(ec *EventContext[intStore], key int, value int, err error) ExecutionState {
		mapLock.Lock()
		defer mapLock.Unlock()
		var lastValue int
		var ok bool
		if lastValue, ok = ordered[key]; ok {
			if value <= lastValue {
				t.Errorf("out of sequence events for key: %d, lastValue: %d, newValue: %d", key, lastValue, value)
			}
		}
		ordered[key] = lastValue
		return Complete
	}, WideNetworkConfig)

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	timer := time.NewTimer(defaultTestTimeout)
	defer timer.Stop()
	executionCount := 0
	testSize := 100000
	store := NewIntStore(TopicPartition{})
	go func() {
		for i := 0; i < testSize; i++ {
			event := MockEventContext[intStore](runStatus.Ctx(), NewRecord(), "", store, mockAsyncCompleter{
				done:          done,
				expectedState: Complete,
				t:             t,
			}, nil)
			key := i % 1000
			value := 1
			scheduler.Schedule(event, key, value)
		}
	}()
	for {
		select {
		case <-done:
			executionCount++
			if executionCount == testSize {
				return
			}
		case <-timer.C:
			t.Errorf("execution timed out")
			return
		}
	}

}

// We need to avoid deadlocks between the async process and the event source.
// An async processor should never accept more items than eosProducerPool.maxPendingItems(),
// otherwise we will deadlock as the async process signals the partionWorker that processing is complete
// (partionWorker may be blocked on eosProducerPool.addEventContext).
// This test ensures we do not make any code changes that break this rule.
//
// In the current implementation, this is enforced by the partitionWorker.maxPending channel, but we're adding the test
// as part of the async testing, as this problem only exists because of async processing
func TestAsyncJobScheduler_CapacityGreaterThanEOSProducer(t *testing.T) {
	if testing.Short() {
		t.Skip()
		return
	}

	itemCount := 100000 // must be greater than the default EOS Producer capacity of 30k
	processed := 0
	done := make(chan struct{}, 1000)
	es, p, _ := newTestEventSource(nil)
	p.produceMany(t, "int", itemCount)
	scheduler, _ := NewAsyncJobScheduler(es.ForkRunStatus(), func(key, value int) error {
		time.Sleep(time.Millisecond)
		return nil
	}, func(ec *EventContext[intStore], key, value int, err error) ExecutionState {
		done <- struct{}{}
		return Complete
	}, SchedulerConfig{
		Concurrency:       100,
		MaxConcurrentKeys: itemCount,
		WorkerQueueDepth:  10,
	})
	RegisterEventType(es, decodeIntStoreItem, func(ec *EventContext[intStore], event intStoreItem) ExecutionState {
		return scheduler.Schedule(ec, event.Key, event.Value)
	}, "int")
	es.ConsumeEvents()
	defer es.StopNow()
	timer := time.NewTimer(time.Minute)
	for {
		select {
		case <-done:
			processed++
			if processed == itemCount {
				return
			}
		case <-timer.C:
			t.Error("async processing deadlock")
			t.FailNow()
			return
		}

	}
}
