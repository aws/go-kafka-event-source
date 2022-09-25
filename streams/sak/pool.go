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

type Pool[T any] struct {
	mu       sync.Mutex
	freelist []T
	factory  func() T
	reset    func(T) T
}

// NewPool creates a new free list.
// size is the maximum size of the returned free list.
func NewPool[T any](size int, factory func() T, resetter func(T) T) *Pool[T] {
	if resetter == nil {
		resetter = func(v T) T { return v }
	}
	return &Pool[T]{freelist: make([]T, 0, size), factory: factory, reset: resetter}
}

func (p *Pool[T]) Borrow() (n T) {
	p.mu.Lock()
	index := len(p.freelist) - 1
	if index < 0 {
		p.mu.Unlock()
		return p.factory()
	}
	var empty T
	n = p.freelist[index]
	p.freelist[index] = empty
	p.freelist = p.freelist[:index]
	p.mu.Unlock()
	return
}

func (p *Pool[T]) Release(n T) (out bool) {
	p.reset(n)
	p.mu.Lock()
	if len(p.freelist) < cap(p.freelist) {
		p.freelist = append(p.freelist, n)
		out = true
	}
	p.mu.Unlock()
	return
}
