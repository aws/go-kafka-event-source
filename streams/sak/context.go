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
// of determining whether a sub-process should is running or to instruct it halt.
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

func (rs RunStatus) Err() error {
	return rs.ctx.Err()
}

func (rs RunStatus) Done() <-chan struct{} {
	return rs.ctx.Done()
}

func (rs RunStatus) Running() bool {
	return rs.ctx.Err() == nil
}

func (rs RunStatus) Halt() {
	rs.cancel()
}

// Creates a new child RunStatus, using the current RunStatus.Ctx() as a parent.
// The equivalent of calling:
//
//	NewRunStatus(rs.Ctx())
func (rs RunStatus) Fork() RunStatus {
	return NewRunStatus(rs.Ctx())
}
