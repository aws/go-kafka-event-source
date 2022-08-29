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

// A handler invoked when a previously scheduled AsyncJob should be performed.
type AsyncProcessor[K comparable, V any] func(K, V) error

// A callback invoked when a previously scheduled AsyncJob has been completed.
type AsyncJobFinalizer[T StateStore, K comparable, V any] func(*EventContext[T], K, V, error) (ExecutionState, error)

// An interface that defines the entry point for scheduling processing outside the context of a TopicPartition event loop
type AsyncJobScheduler[T StateStore, K comparable, V any] interface {
	Schedule(*EventContext[T], K, V) (ExecutionState, error)
}

// Creates an async scheduler using the default GKES implementation (suitable for most use cases).
func CreateAsyncJobScheduler[T StateStore, K comparable, V any](es *EventSource[T], processor AsyncProcessor[K, V], finalizer AsyncJobFinalizer[T, K, V]) AsyncJobScheduler[T, K, V] {
	return nil
}
