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

// TIRunContext is the execution context handed to a task. It embeds the
// standard context.Context (cancellation, deadline, request-scoped values) and
// additionally carries the identifiers and scheduling timestamps of the task
// instance that is executing, along with the Dag run it belongs to. It is the
// Go equivalent of the execution context the Python and Java SDKs expose to
// task authors.
//
// The runtime injects it into a task function by parameter type, so declare it
// as the task's context argument and read the fields directly:
//
//	func myTask(ctx sdk.TIRunContext, log *slog.Logger) error {
//		log.Info("running", "task_id", ctx.TI.TaskID, "run_id", ctx.DagRun.RunID)
//		return nil
//	}
//
// Because it embeds context.Context it is itself a context.Context: pass it
// straight to client calls, select on ctx.Done(), or hand it to downstream
// helpers that take a context.Context.
//
// Embedding a context.Context in a struct is normally discouraged, but it is a
// deliberate exception here: the runtime needs a concrete type to bind by
// parameter, and the value lives only for the duration of a single task
// execution. The runtime always sets the embedded Context before invoking the
// task. When constructing one yourself (for example in a unit test) set the
// Context field, otherwise the context.Context methods dereference a nil
// interface and panic:
//
//	ctx := sdk.TIRunContext{Context: context.Background()}
type TIRunContext struct {
	context.Context

	// TI identifies the task instance that is executing.
	TI TaskInstance
	// DagRun identifies the Dag run the task instance belongs to.
	DagRun DagRun
}

// TIRunContext is always usable wherever a context.Context is expected.
var _ context.Context = TIRunContext{}

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
