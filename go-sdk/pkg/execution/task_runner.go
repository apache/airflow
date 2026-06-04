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

package execution

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// RunTask executes a task based on StartupDetails received from the supervisor.
//
// It looks up the task in the bundle, creates a CoordinatorClient for SDK
// calls, executes the task, and returns a terminal message body
// (SucceedTaskMsg or TaskStateMsg) ready to ship as the final response frame.
//
// The supervisor owns the Execution-API state transitions in coordinator
// mode, so we deliberately bypass worker.ExecuteTaskWorkload (which drives
// Run / UpdateState itself) and only invoke the user's task function.
//
// ctx is the task's root context; Serve derives it from SIGINT/SIGTERM, so a
// cooperative task that honors ctx returns promptly on a supervisor shutdown.
func RunTask(
	ctx context.Context,
	bundle bundlev1.Bundle,
	details *StartupDetails,
	comm *CoordinatorComm,
	logger *slog.Logger,
) map[string]any {
	task, exists := bundle.LookupTask(details.TI.DagID, details.TI.TaskID)
	if !exists {
		logger.Error("Task not registered",
			"dag_id", details.TI.DagID,
			"task_id", details.TI.TaskID,
		)
		return TaskStateMsg{State: TaskStateRemoved, EndDate: time.Now().UTC()}.toMap()
	}

	client := NewCoordinatorClient(comm)

	// taskFunction.sendXcom reads the workload from context to get the task
	// instance ids; populate it the same shape the gRPC path uses.
	tiUUID, err := uuid.Parse(details.TI.ID)
	if err != nil {
		logger.Error("Invalid task instance UUID from supervisor",
			"dag_id", details.TI.DagID,
			"task_id", details.TI.TaskID,
			"ti_id", details.TI.ID,
			"error", err,
		)
		return TaskStateMsg{State: TaskStateFailed, EndDate: time.Now().UTC()}.toMap()
	}
	mapIndex := details.TI.MapIndex
	workload := api.ExecuteTaskWorkload{
		TI: api.TaskInstance{
			Id:        tiUUID,
			DagId:     details.TI.DagID,
			RunId:     details.TI.RunID,
			TaskId:    details.TI.TaskID,
			TryNumber: details.TI.TryNumber,
			MapIndex:  &mapIndex,
		},
		BundleInfo: api.BundleInfo{
			Name:    details.BundleInfo.Name,
			Version: &details.BundleInfo.Version,
		},
	}

	ctx = context.WithValue(ctx, sdkcontext.WorkloadContextKey, workload)
	ctx = context.WithValue(ctx, sdkcontext.SdkClientContextKey, sdk.Client(client))

	return executeTask(ctx, task, logger)
}

// executeTask runs the task and handles success, failure, and panics.
func executeTask(
	ctx context.Context,
	task bundlev1.Task,
	logger *slog.Logger,
) (result map[string]any) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Recovered panic in task",
				"error", r,
				"stack", string(debug.Stack()),
			)
			result = TaskStateMsg{
				State:   TaskStateFailed,
				EndDate: time.Now().UTC(),
			}.toMap()
		}
	}()

	if err := task.Execute(ctx, logger); err != nil {
		logger.ErrorContext(ctx, "Task failed", "error", err)
		// TODO(https://github.com/apache/airflow/issues/67797): emit RetryTask
		// (UP_FOR_RETRY) when ti_context.should_retry is set. Today every
		// failure maps to terminal FAILED because the supervisor honors this
		// frame on exit 0 and we never send RetryTask, so retries are lost.
		return TaskStateMsg{
			State:   TaskStateFailed,
			EndDate: time.Now().UTC(),
		}.toMap()
	}

	return SucceedTaskMsg{
		EndDate: time.Now().UTC(),
	}.toMap()
}
