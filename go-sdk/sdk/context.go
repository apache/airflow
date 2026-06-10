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

	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

// RuntimeContext carries the identifiers and scheduling timestamps of the task
// instance that is currently executing, along with the Dag run it belongs to.
// It is the Go equivalent of the execution context the Python and Java SDKs
// expose to task authors.
//
// Retrieve it inside a task function with CurrentContext:
//
//	func myTask(ctx context.Context, log *slog.Logger) error {
//		rc := sdk.CurrentContext(ctx)
//		log.Info("running", "task_id", rc.TI.TaskID, "run_id", rc.DagRun.RunID)
//		return nil
//	}
type RuntimeContext struct {
	// TI identifies the task instance that is executing.
	TI TaskInstance
	// DagRun identifies the Dag run the task instance belongs to.
	DagRun DagRun
}

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

// CurrentContext returns the RuntimeContext the runtime stored on ctx for the
// executing task. When ctx carries no RuntimeContext (for example when called
// outside of a running task) it returns the zero value.
//
// It takes ctx explicitly rather than reading task-local state like Python's
// get_current_context, because Go has no goroutine-local storage and the
// worker path runs multiple tasks concurrently in one process.
func CurrentContext(ctx context.Context) RuntimeContext {
	rc, _ := ctx.Value(sdkcontext.RuntimeContextKey).(RuntimeContext)
	return rc
}
