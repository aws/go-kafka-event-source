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
	"math/rand"
	"sync"
	"time"
)

// Defines the method signature needed by the EventSource to perform a stream interjection. See EventSource.Interject.
type Interjector[T any] func(*EventContext[T], time.Time) ExecutionState

// Container for an Interjector.
type interjection[T any] struct {
	interjector      Interjector[T]
	every            time.Duration
	jitter           time.Duration
	cancelSignal     chan struct{}
	interjectChannel chan *interjection[T]
	topicPartition   TopicPartition
	timer            *time.Timer
	callback         func()
	cancelled        bool
	isOneOff         bool
	initOnce         sync.Once
	cancelOnce       sync.Once
}

func (ij *interjection[T]) interject(ec *EventContext[T]) ExecutionState {
	return ij.interjector(ec, time.Now())
}

func (ij *interjection[T]) init(tp TopicPartition, c chan *interjection[T]) {
	ij.initOnce.Do(func() {
		ij.topicPartition = tp
		ij.cancelSignal = make(chan struct{}, 1)
		ij.interjectChannel = c
		log.Infof("Interjection initialized for %+v", ij.topicPartition)
	})
}

// calculates the timerDuration with jitter for the next interjection interval
func (ij *interjection[T]) timerDuration() time.Duration {
	if ij.jitter == 0 {
		return ij.every
	}
	jitter := time.Duration(rand.Float64() * float64(ij.jitter))
	if rand.Intn(10) < 5 {
		return ij.every + jitter
	}
	return ij.every - jitter
}

// schedules the next interjection
func (ij *interjection[T]) tick() {
	if ij.isOneOff {
		return
	}
	delay := ij.timerDuration()
	log.Tracef("Scheduling interjection for %+v in %v", ij.topicPartition, delay)
	ij.timer = time.AfterFunc(delay, func() {
		for {
			select {
			case ij.interjectChannel <- ij:
				log.Tracef("Interjected into %+v", ij.topicPartition)
				return
			case <-ij.cancelSignal:
				return
			}
		}
	})
}

func (ij *interjection[T]) cancel() {
	ij.cancelOnce.Do(func() {
		ij.cancelled = true
		ij.cancelSignal <- struct{}{}
		ij.timer.Stop()
		log.Infof("Interjection stopped for %+v", ij.topicPartition)
	})

}
