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

// package "sak" (Swiss Army knife) provides some basic util functions
package sak

import (
	"unsafe"
)

// Simple utilty for swapping struct T to a ptr T
// Wether or not this creates a heap escape is up to the compiler.
// This method simply return &v
func Ptr[T any](v T) *T {
	return &v
}

type Signed interface {
	~int | ~int16 | ~int32 | ~int64 | ~int8
}

type Unsigned interface {
	~uint | ~uint16 | ~uint32 | ~uint64 | uint8
}

type Float interface {
	~float32 | ~float64
}

type Number interface {
	Signed | Unsigned | Float
}

// A generic version of math.Min.
func Min[T Number](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// A generic version of math.Min with the added bonus of accepting more than 2 arguments.
func MinN[T Number](vals ...T) (min T) {
	if len(vals) == 0 {
		return
	}
	min = vals[0]
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		if v < min {
			min = v
		}
	}
	return
}

// A generic version of math.Max.
func Max[T Number](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// A generic version of math.Max with the added bonus of accepting more than 2 arguments.
func MaxN[T Number](vals ...T) (max T) {
	if len(vals) == 0 {
		return
	}
	max = vals[0]
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		if v > max {
			max = v
		}
	}
	return
}

// A utility function that converts a slice of T to a slice of *T.
// Useful when you don't want consumer of your package to be able to mutate an argument passed to you,
// but you need to mutate it internally (accept a slice of structs, then swap them to pointers).
// This methos forces a heap escape via
//
// ptr := new(T)
//
// Used internally but exposed for your consumption.
func ToPtrSlice[T any](structs []T) []*T {
	pointers := make([]*T, len(structs))
	for i, v := range structs {
		ptr := new(T)
		*ptr = v
		pointers[i] = ptr
	}
	return pointers
}

// The inverse of ToPtrSlice.
// Useful when you're doing some type gymnastics.
// Not used internally but, seems only correct to supply the inverse.
func ToStructSlice[T any](ptrs []*T) []T {
	pointers := make([]T, len(ptrs))
	for i, v := range ptrs {
		pointers[i] = *v
	}
	return pointers
}

// A utility function that extracts all values from a map[K]T.
// Useful when you need to iterate over items in a map that is synchronized buy a Mutex.
// Used internally but exposed for your consumption.
func MapValuesToSlice[K comparable, T any](m map[K]T) []T {
	slice := make([]T, 0, len(m))
	for _, v := range m {
		slice = append(slice, v)
	}
	return slice
}

// A utility function that extracts all keys from a map[K]T.
// Useful when you need to iterate over keys in a map that is synchronized buy a Mutex.
// Used internally but exposed for your consumption.
func MapKeysToSlice[K comparable, T any](m map[K]T) []K {
	slice := make([]K, 0, len(m))
	for k := range m {
		slice = append(slice, k)
	}
	return slice
}

// Noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.
// USE CAREFULLY!
//
//go:nosplit
func Noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(unsafe.Pointer(p))
	return unsafe.Pointer(x ^ 0)
}

// A convenience method for panicking on errors. Useful for simplifying code when calling methods that should never error,
// or when thre is no way to recover from the error.
func Must[T any](item T, err error) T {
	if err != nil {
		panic(err)
	}
	return item
}
