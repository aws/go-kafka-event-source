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
	"time"
)

type EosConfig struct {
	// PoolSize is the number of transactional producer clients in the pool.
	PoolSize int
	// The maximum number of pending transactions to be allowed in the pool at any given point in time.
	PendingTxnCount int
	// MinBatchSize is the target number of events or records (whichever is greater) for a transaction before a commit is attempted.
	MinBatchSize int
	// MaxBatchSize is the maximum number of events or records (whichever is greater) for a transaction before it will stop accepting new events.
	// Once a transaction reaches MaxBatchSize, it  ust be commited.
	MaxBatchSize int
	// The maximum amount of time to wait before commiting a transaction. Once this time has elapsed, the transaction will commit
	// even if MinBatchSize has not been achieved. This number will be the tail latency of the consume/produce cycle during periods of low activity.
	// Under high load, this setting has little effect. If this value is too low, there is a great loss in efficiency. The recommnded value is 10ms and the minimum allowed value is 1ms.
	BatchDelay time.Duration
}

func (cfg EosConfig) IsZero() bool {
	if cfg.PoolSize != 0 {
		return false
	}
	if cfg.PendingTxnCount != 0 {
		return false
	}
	if cfg.MinBatchSize != 0 {
		return false
	}
	if cfg.MaxBatchSize != 0 {
		return false
	}
	if cfg.BatchDelay != 0 {
		return false
	}
	return true
}

func (cfg EosConfig) validate() {
	if cfg.PoolSize < 1 {
		panic("EosConfig.PoolSize is less than 1")
	}
	if cfg.PendingTxnCount < 1 {
		panic("EosConfig.PendingTxnCount is less than 1")
	}
	if cfg.MinBatchSize < 1 {
		panic("EosConfig.MinBatchSize is less than 1")
	}
	if cfg.MaxBatchSize < 1 {
		panic("EosConfig.MaxBatchSize is less than 1")
	}
	if cfg.BatchDelay < time.Millisecond {
		panic("EosConfig.BatchDelay is less than 1ms")
	}
	if cfg.MinBatchSize >= cfg.MaxBatchSize {
		panic("EosConfig.MinBatchSize >= EosConfig.MaxBatchSize")
	}
}

var DefaultEosConfig = EosConfig{
	PoolSize:        3,
	PendingTxnCount: 1,
	MinBatchSize:    100,
	MaxBatchSize:    10000,
	BatchDelay:      10 * time.Millisecond,
}
