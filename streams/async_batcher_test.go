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
	"sync/atomic"
	"testing"
	"time"
)

type mockAsyncCompleter struct {
	expectedState ExecutionState
	done          chan struct{}
	t             *testing.T
}

func (m mockAsyncCompleter) AsyncComplete(job AsyncJob[intStore]) {
	if state := job.Finalize(); state != m.expectedState {
		m.t.Errorf("incorrect ExecutionState. actual %v, expected: %v", state, m.expectedState)
	}
	if m.done != nil {
		m.done <- struct{}{}
	}
}

func TestAsyncBatching(t *testing.T) {
	var items [20]int64

	for i := range items {
		items[i] = int64(i)
	}
	done := make(chan struct{})
	ec := MockEventContext[intStore](context.TODO(), nil, "", NewIntStore(ntp(0, "")), mockAsyncCompleter{
		expectedState: Incomplete,
		done:          done,
		t:             t,
	}, nil)

	batch := NewBatchItems(ec, 0,
		func(_ *EventContext[intStore], b *BatchItems[intStore, int, int64]) ExecutionState {
			if len(b.items) != 20 {
				t.Errorf("incorrect number of items. actual: %d, expected: %d", len(b.items), 20)
			}
			return Incomplete
		},
	).Add(items[:]...)

	executionCount := int64(0)
	lastProcessed := int64(-1)
	executor := func(batch []*BatchItem[int, int64]) {
		if atomic.AddInt64(&executionCount, 1) == 1 {
			time.Sleep(100 * time.Millisecond)
		}
		if len(batch) != 10 {
			t.Errorf("incorrect batch size. actual %d, exepected %d", len(batch), 10)
		}
		for _, batchItem := range batch {
			value := batchItem.Value
			oldValue := atomic.SwapInt64(&lastProcessed, value)
			batchItem.UserData = -value
			if value-1 != oldValue {
				t.Errorf("incorrect ordering of async batcher. actual %d, exepected %d", value, oldValue+1)
			}

		}
	}
	batcher := NewAsyncBatcher[intStore](executor, 10, 10, 0)
	batcher.Add(batch)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Errorf("execution timed out")
	}

	if executionCount != 2 {
		t.Errorf("incorrect execution count. actual %d, expected: %d", executionCount, 2)
	}

	for _, item := range batch.items {
		userData := item.UserData.(int64)
		if item.Value+userData != 0 {
			t.Errorf("invalid userdata: %v, %v", userData, item.Value)
		}
	}
}

func TestAsyncNoopBatching(t *testing.T) {
	done := make(chan struct{})
	ec := MockEventContext[intStore](context.TODO(), nil, "", NewIntStore(ntp(0, "")), mockAsyncCompleter{
		expectedState: Complete,
		done:          done,
		t:             t,
	}, nil)
	batch := NewBatchItems(ec, 0,
		func(_ *EventContext[intStore], b *BatchItems[intStore, int, int64]) ExecutionState {
			if len(b.items) != 0 {
				t.Errorf("incorrect number of items. actual: %d, expected: %d", len(b.items), 0)
			}
			return Complete
		},
	)

	executor := func(batch []*BatchItem[int, int64]) {
		t.Errorf("executor should not have been executed")
	}
	batcher := NewAsyncBatcher[intStore](executor, 10, 10, 0)
	batcher.Add(batch)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Errorf("execution timed out")
	}
}
