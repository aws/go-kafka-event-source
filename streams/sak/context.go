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

import "context"

// RunStatus encapsulates a cancellable Context for the purposes
// of determining whether a sub-process is running or to instruct it halt.
type RunStatus struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Creates a RunStatus. If `parent` == nil, context.Background() is used.
func NewRunStatus(parent context.Context) RunStatus {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return RunStatus{ctx, cancel}
}

func (rs RunStatus) Ctx() context.Context {
	return rs.ctx
}

// Creates a new RunStatus by adding key/value to the underlying Context. This is semantically different than Fork as the RunStatus
// returned here does not get a new context.CancelFunc, so calling Halt on the returned RunStatus will also halt the receiver.
func (rs RunStatus) WithValue(key, value any) RunStatus {
	return RunStatus{
		ctx:    context.WithValue(rs.ctx, key, value),
		cancel: rs.cancel,
	}
}

func (rs RunStatus) Err() error {
	return rs.ctx.Err()
}

// Returns the RunStatus.Ctx().Done()
func (rs RunStatus) Done() <-chan struct{} {
	return rs.ctx.Done()
}

// Returns true if the underlying Context has neither timed out or has been canceled.
func (rs RunStatus) Running() bool {
	return rs.ctx.Err() == nil
}

func (rs RunStatus) Halt() {
	rs.cancel()
}

// Creates a new child RunStatus, using the current RunStatus.Ctx() as a parent.
// The newly created RunStatus get's a new context.CancelFunc. Calling Halt on the returned RunStatus will not Halt the parent.
// The equivalent of calling:
//
//	NewRunStatus(rs.Ctx())
func (rs RunStatus) Fork() RunStatus {
	return NewRunStatus(rs.Ctx())
}
