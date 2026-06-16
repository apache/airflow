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
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
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
	details *genmodels.StartupDetails,
	comm *CoordinatorComm,
	logger *slog.Logger,
) any {
	task, exists := bundle.LookupTask(details.TI.DagID, details.TI.TaskID)
	if !exists {
		logger.Error("Task not registered",
			"dag_id", details.TI.DagID,
			"task_id", details.TI.TaskID,
		)
		return genmodels.TaskState{
			Type:    genmodels.TypeTaskState,
			State:   genmodels.TaskStateStateRemoved,
			EndDate: time.Now().UTC(),
		}
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
		return genmodels.TaskState{
			Type:    genmodels.TypeTaskState,
			State:   genmodels.TaskStateStateFailed,
			EndDate: time.Now().UTC(),
		}
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
			Version: ifaceStringPtr(details.BundleInfo.Version),
		},
	}

	// Carries the task runtime context for sdk.TIRunContext injection. The
	// scheduling timestamps live on the nested dag_run object in the
	// supervisor's TIRunContext schema. The base context is a placeholder;
	// bundlev1.Execute rebuilds the value around the live task context when
	// binding the parameter.
	dagRun := details.TIContext.DagRun
	runtimeContext := sdk.NewTIRunContext(
		context.Background(),
		sdk.TaskInstance{
			DagID:     details.TI.DagID,
			RunID:     details.TI.RunID,
			TaskID:    details.TI.TaskID,
			MapIndex:  mapIndexPtr(details.TI.MapIndex),
			TryNumber: details.TI.TryNumber,
		},
		sdk.DagRun{
			DagID:             details.TI.DagID,
			RunID:             details.TI.RunID,
			LogicalDate:       ifaceTimePtr(dagRun.LogicalDate),
			DataIntervalStart: ifaceTimePtr(dagRun.DataIntervalStart),
			DataIntervalEnd:   ifaceTimePtr(dagRun.DataIntervalEnd),
		},
	)

	ctx = context.WithValue(ctx, sdkcontext.WorkloadContextKey, workload)
	ctx = context.WithValue(ctx, sdkcontext.SdkClientContextKey, sdk.Client(client))
	ctx = context.WithValue(ctx, sdkcontext.RuntimeContextKey, runtimeContext)

	return executeTask(ctx, task, details.TIContext.ShouldRetry, logger)
}

// mapIndexPtr converts the supervisor's map_index (which uses -1 as the
// sentinel for an unmapped task) into the optional form exposed on
// sdk.TaskInstance: nil for an unmapped task, otherwise a pointer to the index.
func mapIndexPtr(mapIndex int) *int {
	if mapIndex < 0 {
		return nil
	}
	return &mapIndex
}

// executeTask runs the task and handles success, failure, and panics. It
// returns the terminal message body (genmodels.SucceedTask or
// genmodels.TaskState) ready to ship as the final response frame.
func executeTask(
	ctx context.Context,
	task bundlev1.Task,
	shouldRetry bool,
	logger *slog.Logger,
) (result any) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Recovered panic in task",
				"error", r,
				"stack", string(debug.Stack()),
			)
			if shouldRetry {
				result = genmodels.RetryTask{
					Type:        genmodels.TypeRetryTask,
					EndDate:     time.Now().UTC(),
					RetryReason: fmt.Sprintf("panic: %v", r),
				}
			} else {
				result = genmodels.TaskState{
					Type:    genmodels.TypeTaskState,
					State:   genmodels.TaskStateStateFailed,
					EndDate: time.Now().UTC(),
				}
			}
		}
	}()

	if err := task.Execute(ctx, logger); err != nil {
		logger.ErrorContext(ctx, "Task failed", "error", err)
		// A task that fails when ti_context.should_retry is set is reported as
		// UP_FOR_RETRY via RetryTask; otherwise it terminates as FAILED.
		if shouldRetry {
			return genmodels.RetryTask{
				Type:        genmodels.TypeRetryTask,
				EndDate:     time.Now().UTC(),
				RetryReason: err.Error(),
			}
		}
		return genmodels.TaskState{
			Type:    genmodels.TypeTaskState,
			State:   genmodels.TaskStateStateFailed,
			EndDate: time.Now().UTC(),
		}
	}

	// task_outlets / outlet_events must be sent as empty lists, not omitted:
	// the supervisor's SucceedTask validation rejects a null/absent value
	// ("Input should be a valid list"). A task that emits no asset events
	// reports empty collections rather than None.
	return genmodels.SucceedTask{
		Type:         genmodels.TypeSucceedTask,
		EndDate:      time.Now().UTC(),
		TaskOutlets:  &genmodels.TaskOutlets{},
		OutletEvents: &genmodels.OutletEvents{},
	}
}
