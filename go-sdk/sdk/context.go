// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sdk

import (
	"context"
	"time"
)

// TIRunContext is the execution context handed to a task. It behaves as the
// standard context.Context (cancellation, deadline, request-scoped values) and
// additionally exposes the identifiers and scheduling timestamps of the task
// instance that is executing, along with the Dag run it belongs to. It is the
// Go equivalent of the execution context the Python and Java SDKs expose to
// task authors.
//
// The runtime injects it into a task function by parameter type, so declare it
// as the task's context argument:
//
//	func myTask(ctx sdk.TIRunContext, log *slog.Logger) error {
//		log.Info("running",
//			"task_id", ctx.TaskInstance().TaskID,
//			"run_id", ctx.DagRun().RunID,
//		)
//		return nil
//	}
//
// Because it embeds context.Context it is usable wherever one is expected:
// pass it straight to client calls, select on ctx.Done(), or hand it to
// downstream helpers that take a context.Context.
//
// It is an interface rather than a struct holding a context.Context, which
// the context package advises against (https://pkg.go.dev/context#hdr-Contexts_and_structs):
// the runtime constructs a fresh value around the live task context for each
// invocation, and task code cannot end up with a half-initialised value. Build
// one in tests with NewTIRunContext.
type TIRunContext interface {
	context.Context

	// TaskInstance identifies the task instance that is executing.
	TaskInstance() TaskInstance
	// DagRun identifies the Dag run the task instance belongs to.
	DagRun() DagRun
}

// NewTIRunContext returns a TIRunContext that delegates context behaviour to
// ctx and exposes ti and dagRun. It panics on a nil ctx, mirroring the context
// package's own constructors. The runtime calls it when binding a task's
// TIRunContext parameter; in unit tests, use it to hand-build the argument:
//
//	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{TaskID: "t1"}, sdk.DagRun{})
func NewTIRunContext(ctx context.Context, ti TaskInstance, dagRun DagRun) TIRunContext {
	if ctx == nil {
		// This cannot happen from the runtime: taskFunction.Execute always
		// binds the live task context. A nil ctx is a programming error in
		// the caller, so fail loudly instead of masking it.
		panic("sdk.NewTIRunContext: cannot create TIRunContext from nil context.Context")
	}
	return tiRunContext{Context: ctx, ti: ti, dagRun: dagRun}
}

// tiRunContext is the runtime implementation of TIRunContext.
type tiRunContext struct {
	context.Context

	ti     TaskInstance
	dagRun DagRun
}

func (c tiRunContext) TaskInstance() TaskInstance { return c.ti }

func (c tiRunContext) DagRun() DagRun { return c.dagRun }

// TaskInstance identifies the currently executing task instance.
type TaskInstance struct {
	DagID  string
	RunID  string
	TaskID string
	// MapIndex is the index within a dynamically mapped task, or nil for an
	// unmapped (regular) task instance.
	MapIndex  *int
	TryNumber int
}

// DagRun identifies the Dag run the current task instance belongs to and
// carries its scheduling timestamps. The *time.Time fields are nil when the
// supervisor did not provide a value (for example, a manually triggered run
// without a logical date).
type DagRun struct {
	DagID             string
	RunID             string
	LogicalDate       *time.Time
	DataIntervalStart *time.Time
	DataIntervalEnd   *time.Time
}
