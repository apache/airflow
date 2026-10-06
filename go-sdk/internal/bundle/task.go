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

package bundle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"

	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Task is one registered task that the coordinator runtime can execute. Bundle
// authors do not implement this directly. airflow.TaskHandler, airflow.DagRef.Task
// and airflow.DagRef.If wrap a plain Go function into a Task.
type Task interface {
	Execute(ctx context.Context, logger *slog.Logger, args []binding.Arg) error
}

// Bundle looks up a registered task by dag_id and task_id. The coordinator
// runtime uses Bundle to find the task the supervisor asked for.
type Bundle interface {
	LookupTask(dagID, taskID string) (Task, bool)
}

// TaskHandlerInfo identifies a registered task handler by its dag_id and task_id.
type TaskHandlerInfo struct {
	DagID  string
	TaskID string
}

// EnumerableBundle lists the registered task handlers in registration order.
// DumpAirflowMetadata in pkg/execution builds the --airflow-metadata manifest
// from that list, which is how airflow-go-pack reads a bundle's Dag and task ids
// without running a task.
type EnumerableBundle interface {
	ListTaskHandlers() []TaskHandlerInfo
}

type taskFunction struct {
	fn       reflect.Value
	fullName string
	plan     *binding.Plan
	// findSkipped is nil unless the task comes from NewPositionalBranchFunction.
	findSkipped func(result any) []string
}

var _ Task = (*taskFunction)(nil)

// NewTaskFunction validates and wraps a Go function as a Task.
func NewTaskFunction(fn any) (Task, error) { return newTaskFunction(fn, binding.Analyze, nil) }

// NewPositionalTaskFunction is like NewTaskFunction, but the Task binds each argument to one
// parameter, in order, as binding.AnalyzePositional describes.
func NewPositionalTaskFunction(fn any) (Task, error) {
	return newTaskFunction(fn, binding.AnalyzePositional, nil)
}

// NewPositionalBranchFunction is like NewPositionalTaskFunction, but the Task also skips tasks
// that are downstream of it. fn must return a result and an error. Before fn runs, Execute checks
// that the runtime can skip tasks. When fn returns a nil error, Execute passes the result to
// findSkipped. If findSkipped returns task_ids, Execute records them in the skipmixin_key XCom of
// the task and skips those tasks.
func NewPositionalBranchFunction(fn any, findSkipped func(result any) []string) (Task, error) {
	return newTaskFunction(fn, binding.AnalyzePositional, findSkipped)
}

func newTaskFunction(
	fn any,
	analyze func(fnType reflect.Type, fnName string) (*binding.Plan, error),
	findSkipped func(result any) []string,
) (Task, error) {
	// The kind comes first: Value.Pointer panics on an int, and Value.Type on an untyped nil.
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return nil, fmt.Errorf("expected a func as input but was %s", v.Kind())
	}
	f := &taskFunction{
		fn:          v,
		fullName:    runtime.FuncForPC(v.Pointer()).Name(),
		findSkipped: findSkipped,
	}
	if err := f.validateFn(v.Type(), analyze); err != nil {
		return nil, err
	}
	return f, nil
}

// Execute binds the supplied TaskFlow arguments and runs the task.
func (f *taskFunction) Execute(
	ctx context.Context,
	logger *slog.Logger,
	args []binding.Arg,
) error {
	sdkClient, err := clientFrom(ctx)
	if err != nil {
		return err
	}
	var branch *branchRun
	if f.findSkipped != nil {
		if branch, err = startBranch(ctx); err != nil {
			return err
		}
	}
	reflectArgs, err := f.plan.Resolve(ctx, logger, sdkClient, args)
	if err != nil {
		return err
	}
	return f.call(ctx, sdkClient, reflectArgs, logger, branch)
}

func clientFrom(ctx context.Context) (sdk.Client, error) {
	client, ok := ctx.Value(sdkcontext.SdkClientContextKey).(sdk.Client)
	if !ok {
		return nil, errors.New("coordinator SDK client is missing from task context")
	}
	return client, nil
}

func (f *taskFunction) call(
	ctx context.Context,
	sdkClient sdk.Client,
	reflectArgs []reflect.Value,
	logger *slog.Logger,
	branch *branchRun,
) error {
	slog.Debug("Attempting to call fn", "fn", f.fn, "args", reflectArgs)
	retValues := f.fn.Call(reflectArgs)

	var err error
	if errResult := retValues[len(retValues)-1].Interface(); errResult != nil {
		var ok bool
		if err, ok = errResult.(error); !ok {
			return fmt.Errorf(
				"failed to extract task error result as it is not of error interface: %v",
				errResult,
			)
		}
	}
	// If there are two results, convert the first only if it's not a nil pointer
	if len(retValues) > 1 && (retValues[0].Kind() != reflect.Ptr || !retValues[0].IsNil()) {
		res := retValues[0].Interface()
		f.sendXcom(ctx, res, sdkClient, logger)
	}
	if err == nil && branch != nil {
		return branch.skipDownstream(
			ctx,
			sdkClient,
			f.findSkipped(retValues[0].Interface()),
			logger,
		)
	}
	return err
}

// skipMixinXComKey is the key of the XCom that lists the tasks a task skipped. When one of those
// tasks is cleared, NotPreviouslySkippedDep in Airflow core reads the XCom and skips the cleared
// task again instead of running it. SkipMixin in the standard provider writes the same key.
const skipMixinXComKey = "skipmixin_key"

type skipDownstreamTasksKey struct{}

// WithSkipDownstreamTasks returns a copy of ctx that carries skip, the function that a task from
// NewPositionalBranchFunction calls to skip tasks downstream of it. The runtime passes skip in the
// context and not in sdk.Client, so that task functions cannot call it. Airflow core reads the
// skipmixin_key XCom only from a task that has _can_skip_downstream set in the serialized Dag. A
// task skipped by a task without that flag would run when someone clears it.
func WithSkipDownstreamTasks(
	ctx context.Context,
	skip func(ctx context.Context, taskIDs []string) error,
) context.Context {
	return context.WithValue(ctx, skipDownstreamTasksKey{}, skip)
}

// branchRun holds the skip function and the task instance that a run of a task from
// NewPositionalBranchFunction uses to skip tasks after fn returns.
type branchRun struct {
	skip func(ctx context.Context, taskIDs []string) error
	ti   sdk.TaskInstance
}

// startBranch runs before fn, so that a runtime that cannot skip tasks fails the task before fn
// has any effect.
func startBranch(ctx context.Context) (*branchRun, error) {
	skip, ok := ctx.Value(skipDownstreamTasksKey{}).(func(context.Context, []string) error)
	if !ok {
		return nil, errors.New("the task runtime cannot skip downstream tasks")
	}
	runtimeContext, ok := ctx.Value(sdkcontext.RuntimeContextKey).(sdk.TIRunContext)
	if !ok {
		return nil, errors.New("task runtime context is missing")
	}
	return &branchRun{skip: skip, ti: runtimeContext.TaskInstance()}, nil
}

// skipDownstream records taskIDs in the skipmixin_key XCom, and then skips those tasks.
func (b *branchRun) skipDownstream(
	ctx context.Context,
	client sdk.Client,
	taskIDs []string,
	logger *slog.Logger,
) error {
	if len(taskIDs) == 0 {
		return nil
	}
	err := client.PushXCom(ctx, b.ti, skipMixinXComKey, map[string][]string{"skipped": taskIDs})
	if err != nil {
		return fmt.Errorf("recording the skipped tasks in the %s XCom: %w", skipMixinXComKey, err)
	}
	logger.InfoContext(ctx, "Skipping downstream tasks", "task_ids", taskIDs)
	if err := b.skip(ctx, taskIDs); err != nil {
		return fmt.Errorf("skipping the downstream tasks %q: %w", taskIDs, err)
	}
	return nil
}

func (f *taskFunction) sendXcom(
	ctx context.Context,
	value any,
	c sdk.XComClient,
	logger *slog.Logger,
) {
	runtimeContext, ok := ctx.Value(sdkcontext.RuntimeContextKey).(sdk.TIRunContext)
	if !ok {
		logger.ErrorContext(ctx, "Unable to set XCom", "err", "task runtime context is missing")
		return
	}
	ti := runtimeContext.TaskInstance()
	err := c.PushXCom(ctx, sdk.TaskInstance{
		DagID:    ti.DagID,
		RunID:    ti.RunID,
		TaskID:   ti.TaskID,
		MapIndex: ti.MapIndex,
	}, sdk.XComReturnValueKey, value)
	if err != nil {
		logger.ErrorContext(ctx, "Unable to set XCom", "err", err)
	}
}

func (f *taskFunction) validateFn(
	fnType reflect.Type,
	analyze func(fnType reflect.Type, fnName string) (*binding.Plan, error),
) error {
	// analyze turns a ... tail into one []T parameter, so Execute would have to call
	// the function with CallSlice rather than Call to fill it. That is only worth doing for a
	// signature []T cannot already express, and ...T is not one: both take a single argument.
	if fnType.IsVariadic() {
		return fmt.Errorf(
			"task function %s is variadic; declare the last parameter as []T instead of ...T",
			f.fullName,
		)
	}

	// Return values
	//     `<result>, error`,  or just `error`
	if fnType.NumOut() < 1 || fnType.NumOut() > 2 {
		return fmt.Errorf(
			"task function %s has %d return values, must be `<result>, error` or just `error`",
			f.fullName,
			fnType.NumOut(),
		)
	}
	if fnType.NumOut() > 1 && !isValidResultType(fnType.Out(0)) {
		return fmt.Errorf(
			"expected task function %s first return value to return valid type but found: %v",
			f.fullName,
			fnType.Out(0).Kind(),
		)
	}
	last := fnType.Out(fnType.NumOut() - 1)
	if !isError(last) {
		return fmt.Errorf(
			"expected task function %s last return value to return error but found %v",
			f.fullName,
			last.Kind(),
		)
	}
	// The last result must be error itself, not just a type that implements error. A nil *MyErr,
	// for example, becomes a non-nil error when call converts it. The task would then fail even
	// though the function returned nil.
	if last != errorType {
		return fmt.Errorf(
			"task function %s must declare its last result as error, not %s",
			f.fullName,
			last,
		)
	}

	plan, err := analyze(fnType, f.fullName)
	if err != nil {
		return err
	}
	f.plan = plan
	return nil
}

func isValidResultType(inType reflect.Type) bool {
	// https://golang.org/pkg/reflect/#Kind
	switch inType.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	}

	return true
}

var errorType = reflect.TypeFor[error]()

func isError(inType reflect.Type) bool {
	return inType != nil && inType.Implements(errorType)
}
