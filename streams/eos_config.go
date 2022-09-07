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

/*
EosDiagarm

	 On-Deck Txn            Pending Txn Channel          Commit Go-Routine
	┌───────────┐           ┌─────────────────┐          ┌─────────────────────────────────────┐
	│ EventCtx  │           │  Pending Txn(s) │          │  Committing Txn                     │
	│ Offset: 7 │           │  ┌───────────┐  │          │  ┌───────────┐                      │
	├───────────┤           │  │ EventCtx  │  │          │  │ EventCtx  │  1: Receive Txn      │
	│ EventCtx  │           │  │ Offset: 4 │  │          │  │ Offset: 1 │                      │
	│ Offset: 8 │           │  ├───────────┤  │          │  ├───────────┤  2: EventCtx(s).Wait │
	├───────────┼──────────►│  │ EventCtx  │  ├─────────►│  │ EventCtx  │                      │
	│ EventCtx  │           │  │ Offset: 5 │  │          │  │ Offset: 2 │  3: Flush Records    │
	│ Offset: 9 │           │  ├───────────┤  │          │  ├───────────┤                      │
	└───────────┘           │  │ EventCtx  │  │          │  │ EventCtx  │  4: Commit           │
	      ▲                 │  │ Offset: 6 │  │          │  │ Offset: 3 │                      │
	      │                 │  └───────────┘  │          │  └───────────┘                      │
	      │                 └─────────────────┘          └─────────────────────────────────────┘
	Incoming EventCtx
*/
type EosConfig struct {
	// PoolSize is the number of transactional producer clients in the pool.
	PoolSize int
	// The maximum number of pending transactions to be allowed in the pool at any given point in time.
	PendingTxnCount int
	// TargetBatchSize is the target number of events or records (whichever is greater) for a transaction before a commit is attempted.
	TargetBatchSize int
	// MaxBatchSize is the maximum number of events or records (whichever is greater) for a transaction before it will stop accepting new events.
	// Once a transaction reaches MaxBatchSize, it  ust be commited.
	MaxBatchSize int
	// The maximum amount of time to wait before committing a transaction. Once this time has elapsed, the transaction will commit
	// even if TargetBatchSize has not been achieved. This number will be the tail latency of the consume/produce cycle during periods of low activity.
	// Under high load, this setting has little impact unless set too low. If this value is too low, produce batch sizes will be extremely small a
	// and Kafka will need to manage an excessive number of transactions.
	// The recommnded value is 10ms and the minimum allowed value is 1ms.
	BatchDelay time.Duration
}

// IsZero returns true if EosConfig is uninitialized, or all values equal zero. Used to determine whether the EventSource should fall back to [DefaultEosConfig].
func (cfg EosConfig) IsZero() bool {
	if cfg.PoolSize != 0 {
		return false
	}
	if cfg.PendingTxnCount != 0 {
		return false
	}
	if cfg.TargetBatchSize != 0 {
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
	if cfg.TargetBatchSize < 1 {
		panic("EosConfig.MinBatchSize is less than 1")
	}
	if cfg.MaxBatchSize < 1 {
		panic("EosConfig.MaxBatchSize is less than 1")
	}
	if cfg.BatchDelay < time.Millisecond {
		panic("EosConfig.BatchDelay is less than 1ms")
	}
	if cfg.TargetBatchSize > cfg.MaxBatchSize {
		panic("EosConfig.MinBatchSize >= EosConfig.MaxBatchSize")
	}
}

const DefaultPoolSize = 3
const DefaultPendingTxnCount = 1
const DefaultTargetBatchSize = 100
const DefaultMaxBatchSize = 10000
const DefaultBatchDelay = 10 * time.Millisecond

var DefaultEosConfig = EosConfig{
	PoolSize:        DefaultPoolSize,
	PendingTxnCount: DefaultPendingTxnCount,
	TargetBatchSize: DefaultTargetBatchSize,
	MaxBatchSize:    DefaultMaxBatchSize,
	BatchDelay:      DefaultBatchDelay,
}
