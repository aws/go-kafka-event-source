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
package sak_test

import (
	"fmt"

	"github.com/aws/go-kafka-event-source/streams/sak"
)

type Encoder[T any] func(T) ([]byte, error)

func encodeString(s string) ([]byte, error) {
	return []byte(s), nil
}

func ExampleMust() {
	var encode Encoder[string] = encodeString
	b := sak.Must(encode("Hello World!"))
	fmt.Println(len(b))
	// Output: 12
}

func ExampleMinN() {
	vals := []int{1, 9, 4, 10, -1, 25}
	min := sak.MinN(vals...)
	fmt.Println(min)
	// Output: -1
}

func ExampleMaxN() {
	vals := []int{1, 9, 4, 10, -1, 25}
	max := sak.MaxN(vals...)
	fmt.Println(max)
	// Output: 25
}

func ExampleMin() {
	a := uint16(1)
	b := uint16(2)
	min := sak.Min(a, b)
	fmt.Println(min)
	// Output: 1
}

func ExampleMax() {
	a := uint16(1)
	b := uint16(2)
	max := sak.Max(a, b)
	fmt.Println(max)
	// Output: 2
}

func newIntArray() []int {
	return make([]int, 0)
}

func releaseIntArray(a []int) []int {
	return a[0:0]
}

func ExamplePool() {
	intArrayPool := sak.NewPool(100, newIntArray, releaseIntArray)
	a := intArrayPool.Borrow()
	for i := 0; i < 10; i++ {
		a = append(a, i)
	}
	fmt.Println(len(a))
	intArrayPool.Release(a)

	b := intArrayPool.Borrow()
	fmt.Println(len(b))
	// Output: 10
	// 0
}
