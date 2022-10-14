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

	"github.com/twmb/franz-go/pkg/kgo"
)

type fixedPartitoner struct {
	partition int32
}

func (fp fixedPartitoner) ForTopic(topic string) kgo.TopicPartitioner {
	return fp
}

func (fp fixedPartitoner) Partition(r *kgo.Record, n int) int {
	return int(fp.partition)
}

func (fp fixedPartitoner) RequiresConsistency(*kgo.Record) bool {
	return true
}

func TestOptionalPartitioner(t *testing.T) {
	partitioner := NewOptionalPerTopicPartitioner(fixedPartitoner{10}, map[string]kgo.Partitioner{
		"A": fixedPartitoner{1},
		"B": fixedPartitoner{2},
	})

	assigned := NewRecord().WithPartition(11)
	unassigned := NewRecord()

	partitionerA := partitioner.ForTopic("A")
	partitionerB := partitioner.ForTopic("B")
	defaultPartitioner := partitioner.ForTopic("UNKNOWN")

	p := partitionerA.Partition(assigned.ToKafkaRecord(), 100)
	if p != 11 {
		t.Errorf("Incorrect partition. actual: %d, expected: %d", p, 11)
	}

	p = partitionerA.Partition(unassigned.ToKafkaRecord(), 100)
	if p != 1 {
		t.Errorf("Incorrect partition. actual: %d, expected: %d", p, 1)
	}

	p = partitionerB.Partition(unassigned.ToKafkaRecord(), 100)
	if p != 2 {
		t.Errorf("Incorrect partition. actual: %d, expected: %d", p, 2)
	}

	p = defaultPartitioner.Partition(unassigned.ToKafkaRecord(), 100)
	if p != 10 {
		t.Errorf("Incorrect partition. actual: %d, expected: %d", p, 10)
	}
}
