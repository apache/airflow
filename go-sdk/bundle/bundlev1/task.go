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

package bundlev1

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
// authors do not implement this directly; Dag.AddTask wraps a plain Go
// function into a Task.
type Task interface {
	Execute(ctx context.Context, logger *slog.Logger, args []binding.Arg) error
}

// Bundle is the execution-time view of a registry. It looks up a task by
// dag_id and task_id.
type Bundle interface {
	LookupTask(dagId, taskId string) (Task, bool)
}

// InputBinding names the upstream task whose return-value XCom fills one data
// parameter.
type InputBinding struct {
	TaskID string
}

type taskFunction struct {
	fn         reflect.Value
	fullName   string
	plan       *binding.Plan
	inputs     []binding.Arg
	doXComPush bool
}

var _ Task = (*taskFunction)(nil)

// NewTaskFunction validates and wraps a Go function as a Task.
func NewTaskFunction(fn any, inputs ...InputBinding) (Task, error) {
	return newTaskFunction(fn, true, inputs)
}

func newTaskFunction(fn any, doXComPush bool, inputs []InputBinding) (Task, error) {
	v := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(v.Pointer()).Name()
	f := &taskFunction{fn: v, fullName: fullName, doXComPush: doXComPush}
	if err := f.validateFn(v.Type()); err != nil {
		return nil, err
	}
	if err := f.bindInputs(v.Type(), inputs); err != nil {
		return nil, err
	}
	return f, nil
}

// dataParamTypes returns non-injected parameter types in declaration order.
func dataParamTypes(fnType reflect.Type) []reflect.Type {
	if fnType.Kind() != reflect.Func {
		return nil
	}
	var out []reflect.Type
	for i := range fnType.NumIn() {
		if in := fnType.In(i); !isInjectable(in) {
			out = append(out, in)
		}
	}
	return out
}

func isInjectable(in reflect.Type) bool {
	return isTIRunContext(in) || isContext(in) || isLogger(in) || isClient(in)
}

func (f *taskFunction) bindInputs(fnType reflect.Type, inputs []InputBinding) error {
	if len(inputs) == 0 {
		return nil
	}
	dataParams := dataParamTypes(fnType)
	if len(dataParams) != len(inputs) {
		return fmt.Errorf(
			"task function %s declares %d data parameter(s) but %d input binding(s) were supplied",
			f.fullName, len(dataParams), len(inputs),
		)
	}
	f.inputs = make([]binding.Arg, len(inputs))
	for i, in := range inputs {
		if in.TaskID == "" {
			return fmt.Errorf(
				"task function %s: input binding %d has an empty task id", f.fullName, i,
			)
		}
		f.inputs[i] = binding.XComArg{Kind: "xcom", TaskID: in.TaskID}
	}
	return nil
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
	if len(args) == 0 && len(f.inputs) > 0 {
		args = f.inputs
	}
	reflectArgs, err := f.plan.Resolve(ctx, logger, sdkClient, args)
	if err != nil {
		return err
	}
	return f.call(ctx, sdkClient, reflectArgs, logger)
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
	if len(retValues) > 1 && f.doXComPush {
		var res any
		value := retValues[0]
		switch value.Kind() {
		case reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if !value.IsNil() {
				res = value.Interface()
			}
		default:
			res = value.Interface()
		}
		f.sendXcom(ctx, res, sdkClient, logger)
	}
	return err
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

func (f *taskFunction) validateFn(fnType reflect.Type) error {
	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("expected a func as input but was %s", fnType.Kind())
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
	if !isError(fnType.Out(fnType.NumOut() - 1)) {
		return fmt.Errorf(
			"expected task function %s last return value to return error but found %v",
			f.fullName,
			fnType.Out(fnType.NumOut()-1).Kind(),
		)
	}

	plan, err := binding.Analyze(fnType, f.fullName)
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

var (
	errorType        = reflect.TypeFor[error]()
	contextType      = reflect.TypeFor[context.Context]()
	tiRunContextType = reflect.TypeFor[sdk.TIRunContext]()
	slogLoggerType   = reflect.TypeFor[*slog.Logger]()
	clientType       = reflect.TypeFor[sdk.Client]()
)

func isError(inType reflect.Type) bool {
	return inType != nil && inType.Implements(errorType)
}

func isContext(inType reflect.Type) bool {
	return inType != nil && inType.Implements(contextType)
}

func isTIRunContext(inType reflect.Type) bool {
	return inType == tiRunContextType
}

func isLogger(inType reflect.Type) bool {
	return inType != nil && inType.AssignableTo(slogLoggerType)
}

func isClient(inType reflect.Type) bool {
	return inType != nil && inType.Kind() == reflect.Interface &&
		inType.NumMethod() > 0 && clientType.Implements(inType)
}
