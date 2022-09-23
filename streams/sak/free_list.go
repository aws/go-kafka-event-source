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

package sak

import "sync"

type FreeList[T any] struct {
	mu       sync.Mutex
	freelist []T
	factory  func() T
}

// NewFreeList creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeList[T any](size int, factory func() T) *FreeList[T] {
	return &FreeList[T]{freelist: make([]T, 0, size), factory: factory}
}

func (f *FreeList[T]) Make() (n T) {
	f.mu.Lock()
	index := len(f.freelist) - 1
	if index < 0 {
		f.mu.Unlock()
		return f.factory()
	}
	var empty T
	n = f.freelist[index]
	f.freelist[index] = empty
	f.freelist = f.freelist[:index]
	f.mu.Unlock()
	return
}

func (f *FreeList[T]) Free(n T) (out bool) {
	f.mu.Lock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	f.mu.Unlock()
	return
}
