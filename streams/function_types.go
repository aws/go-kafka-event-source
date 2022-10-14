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

// Defines a method which accepts a TopiCPartition argument and returns T
type TopicPartitionCallback[T any] func(TopicPartition) T

// A callback invoked when a new TopicPartition has been assigned to a EventSource. Your callback should return an empty StateStore.
type StateStoreFactory[T StateStore] TopicPartitionCallback[T]

// A callback invoked when a new record has been received from the EventSource.
type IncomingRecordDecoder[V any] func(IncomingRecord) (V, error)

// A callback invoked when a new record has been received from the EventSource, after it has been transformed via IncomingRecordTransformer.
type EventProcessor[T any, V any] func(*EventContext[T], V) ExecutionState

type SourcePartitionEventHandler func(*Source, int32)

type MetricsHandler func(Metric)

type DeserializationErrorHandler func(ec ErrorContext, eventType string, err error) ErrorResponse

type TxnErrorHandler func(err error) ErrorResponse

// A handler invoked when a previously scheduled AsyncJob should be performed.
type AsyncJobProcessor[K comparable, V any] func(K, V) error

// A callback invoked when a previously scheduled AsyncJob has been completed.
type AsyncJobFinalizer[T any, K comparable, V any] func(*EventContext[T], K, V, error) ExecutionState

type BatchCallback[S any, K comparable, V any] func(*EventContext[S], *BatchItems[S, K, V]) ExecutionState

type BatchExecutor[K comparable, V any] func(batch []*BatchItem[K, V])

type BatchProducerCallback[S any] func(eventContext *EventContext[S], records []*Record, userData any) ExecutionState

// Defines the method signature needed by the EventSource to perform a stream interjection. See EventSource.Interject.
type Interjector[T any] func(*EventContext[T], time.Time) ExecutionState
