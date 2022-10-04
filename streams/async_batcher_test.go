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
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncBatchOrdering(t *testing.T) {
	var items [20]BatchItem[int, int64]

	for i := range items {
		items[i] = BatchItem[int, int64]{
			Key:   0, // all items have the same key
			Value: int64(i),
		}
	}
	done := make(chan struct{})
	batch := NewBatch(&EventContext[*IntStore]{}, func(_ *Batch[*IntStore, int, int64]) {
		close(done)
	}).Add(items[:]...)

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
	batcher := NewAsyncBatcher[*IntStore](nil, executor, 10, 10, 0)
	batcher.Add(batch)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}

	if executionCount != 2 {
		t.Errorf("all batches did not execute")
	}

	for _, item := range batch.Items {
		userData := item.UserData.(int64)
		if item.Value+userData != 0 {
			t.Errorf("invalid userdata: %v, %v", userData, item.Value)
		}
	}
}
