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
	"testing"
	"time"

	"github.com/google/btree"
)

func TestEventSourceInsert(t *testing.T) {
	if testing.Short() {
		t.Skip()
		return
	}

	itemCount := 10000 //must be multiple of 10 for this to work

	es, p, c := newTestEventSource()
	p.produceMany(t, "int", itemCount)

	es.ConsumeEvents()
	defer es.StopNow()
	p.waitForAllPartitions(t, c, defaultTestTimeout)

	trees := []*btree.BTreeG[intStoreItem]{}
	es.InterjectAllSync(func(ec *EventContext[intStore], _ time.Time) ExecutionState {
		tree := ec.Store().tree
		trees = append(trees, tree.Clone())
		if tree.Len() != itemCount/10 {
			t.Errorf("incorrect number of items in partition. actual: %d, expected: %d", tree.Len(), itemCount/10)
		}
		return Complete
	})
	if len(trees) != es.consumer.source.Config().NumPartitions {
		t.Errorf("incorrect number of stores. actual: %d, expected: %d", len(trees), es.consumer.source.Config().NumPartitions)
	}

	data := make(map[int]int)
	for _, tree := range trees {
		tree.Ascend(func(item intStoreItem) bool {
			if item.Key != item.Value {
				t.Errorf("incorrect item value. actual: %d, expected: %d", item.Value, item.Key)
			}
			if _, ok := data[item.Key]; ok {
				t.Errorf("duplicate key: %d", item.Key)
			}
			data[item.Key] = item.Value
			return true
		})
	}

	if len(data) != itemCount {
		t.Errorf("incorrect item count. actual: %d, expected: %d", len(data), itemCount)
	}
	for i := 0; i < itemCount; i++ {
		if _, ok := data[i]; !ok {
			t.Errorf("missing key: %d", i)
		}
	}
}

func TestEventSourceDelete(t *testing.T) {
	if testing.Short() {
		t.Skip()
		return
	}

	itemCount := 10000 //must be multiple of 10 for this to work

	es, p, c := newTestEventSource()
	p.produceMany(t, "int", itemCount)
	p.delete(t, "int", 0)

	es.ConsumeEvents()
	defer es.StopNow()

	p.waitForAllPartitions(t, c, defaultTestTimeout)
	ij := func(ec *EventContext[intStore], _ time.Time) ExecutionState {
		targetCount := (itemCount / 10) - 1
		if ec.Store().tree.Len() != targetCount {
			t.Errorf("incorrect item count. actual: %d, expected: %d", ec.Store().tree.Len(), targetCount)
		}
		return Complete
	}
	if err := <-es.Interject(0, ij); err != nil {
		t.Error(err)
	}
	p.deleteMany(t, "int", itemCount)
	p.waitForAllPartitions(t, c, defaultTestTimeout)

	trees := []*btree.BTreeG[intStoreItem]{}
	es.InterjectAllSync(func(ec *EventContext[intStore], _ time.Time) ExecutionState {
		tree := ec.Store().tree
		trees = append(trees, tree.Clone())
		if tree.Len() != 0 {
			t.Errorf("incorrect number of items in partition. actual: %d, expected: %d", tree.Len(), 0)
		}
		return Complete
	})
	if len(trees) != es.consumer.source.Config().NumPartitions {
		t.Errorf("incorrect number of stores. actual: %d, expected: %d", len(trees), es.consumer.source.Config().NumPartitions)
	}

}
