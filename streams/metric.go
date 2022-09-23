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

import "time"

const TxnCommitOperation = "TxnCommit"
const PartitionPreppedOperation = "PartitionPrepped"

type MetricsHandler func(Metric)
type Metric struct {
	StartTime      time.Time
	ExecuteTime    time.Time
	EndTime        time.Time
	Count          int
	Bytes          int
	PartitionCount int
	Partition      int32
	Operation      string
	Topic          string
	GroupId        string
}

func (m Metric) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

func (m Metric) Linger() time.Duration {
	return m.ExecuteTime.Sub(m.StartTime)
}

func (m Metric) ExecuteDuration() time.Duration {
	return m.EndTime.Sub(m.ExecuteTime)
}
