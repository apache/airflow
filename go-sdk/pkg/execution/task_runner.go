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
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// RunTask executes a task based on StartupDetails received from the supervisor.
//
// It looks up the task in the bundle, creates a CoordinatorClient for SDK
// calls, executes the task, and returns the terminal body to ship as the final
// response frame: one of genmodels.SucceedTask, TaskState, or RetryTask.
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
			State:   genmodels.TaskStateStateFailed,
			EndDate: time.Now().UTC(),
		}
	}
	workload := api.ExecuteTaskWorkload{
		TI: api.TaskInstance{
			Id:        tiUUID,
			DagId:     details.TI.DagID,
			RunId:     details.TI.RunID,
			TaskId:    details.TI.TaskID,
			TryNumber: details.TI.TryNumber,
			MapIndex:  mapIndexPtr(details.TI.MapIndex),
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

	args := convertStubArgs(details.TIContext.StubArgs)

	return executeTask(ctx, task, args, details.TIContext.ShouldRetry, logger)
}

// convertStubArgs maps the wire-model positional-argument spec (captured from
// the Python stub Dag's TaskFlow call) onto the runtime-neutral binding form.
func convertStubArgs(specsPtr *genmodels.StubArgs) []binding.Arg {
	if specsPtr == nil || len(*specsPtr) == 0 {
		return nil
	}
	specs := *specsPtr
	args := make([]binding.Arg, len(specs))
	for i, spec := range specs {
		taskID := ""
		if s, ok := spec.TaskID.(string); ok {
			taskID = s
		}
		args[i] = binding.Arg{
			Kind:     binding.ArgKind(spec.Kind),
			TaskID:   taskID,
			Key:      spec.Key,
			Value:    spec.Value,
			DataType: binding.DataType(spec.DataType),
		}
	}
	return args
}

// mapIndexPtr normalizes the supervisor's map_index into the optional form
// exposed on api.TaskInstance / sdk.TaskInstance: nil for an unmapped task,
// otherwise a pointer to the index. The wire field is itself optional now, so an
// unmapped task arrives as either nil (key absent) or a pointer to the -1
// sentinel; both collapse to nil here.
func mapIndexPtr(mapIndex *int) *int {
	if mapIndex == nil || *mapIndex < 0 {
		return nil
	}
	idx := *mapIndex
	return &idx
}

// executeTask runs the task, handling success, failure, and panics, and returns
// the terminal body: genmodels.SucceedTask, TaskState, or RetryTask.
//
// args carries the positional-argument spec from the stub Dag's TaskFlow call;
// tasks that implement bundlev1.TaskWithArgs bind it (an empty spec still runs
// the arity check), while a custom Task implementation that receives a
// non-empty spec fails loudly rather than silently dropping the arguments.
func executeTask(
	ctx context.Context,
	task bundlev1.Task,
	args []binding.Arg,
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
					EndDate:     time.Now().UTC(),
					RetryReason: fmt.Sprintf("panic: %v", r),
				}
			} else {
				result = genmodels.TaskState{
					State:   genmodels.TaskStateStateFailed,
					EndDate: time.Now().UTC(),
				}
			}
		}
	}()

	var err error
	if tw, ok := task.(bundlev1.TaskWithArgs); ok {
		err = tw.ExecuteArgs(ctx, logger, args)
	} else if len(args) > 0 {
		err = fmt.Errorf(
			"task received %d positional argument(s) from the Dag but its implementation "+
				"does not support argument binding (does not implement TaskWithArgs)",
			len(args),
		)
	} else {
		err = task.Execute(ctx, logger)
	}
	if err != nil {
		logger.ErrorContext(ctx, "Task failed", "error", err)
		// A task that fails when ti_context.should_retry is set is reported as
		// UP_FOR_RETRY via RetryTask; otherwise it terminates as FAILED.
		if shouldRetry {
			return genmodels.RetryTask{
				EndDate:     time.Now().UTC(),
				RetryReason: err.Error(),
			}
		}
		return genmodels.TaskState{
			State:   genmodels.TaskStateStateFailed,
			EndDate: time.Now().UTC(),
		}
	}

	// task_outlets / outlet_events must be sent as empty lists, not omitted:
	// the supervisor's SucceedTask validation rejects a null/absent value
	// ("Input should be a valid list"). A task that emits no asset events
	// reports empty collections rather than None.
	return genmodels.SucceedTask{
		EndDate:      time.Now().UTC(),
		TaskOutlets:  &genmodels.TaskOutlets{},
		OutletEvents: &genmodels.OutletEvents{},
	}
}
