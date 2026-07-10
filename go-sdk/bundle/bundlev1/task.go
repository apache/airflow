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
	"fmt"
	"log/slog"
	"reflect"
	"runtime"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// TaskWithArgs is implemented by tasks that can bind positional arguments
// captured from the Dag's TaskFlow call (delivered per execution in
// coordinator mode). Execute(ctx, logger) is equivalent to
// ExecuteArgs(ctx, logger, nil).
type TaskWithArgs interface {
	Task
	ExecuteArgs(ctx context.Context, logger *slog.Logger, args []binding.Arg) error
}

type taskFunction struct {
	fn       reflect.Value
	fullName string
	plan     *binding.Plan
}

var _ TaskWithArgs = (*taskFunction)(nil)

// NewTaskFunction wraps a plain Go function as a Task, validating its signature
// (injectable parameters, data parameters that task arguments can decode into,
// and a return of error or (result, error)). Bundle authors normally use
// Dag.AddTask, which calls this for them; use it directly only when building a
// Task outside the registry.
func NewTaskFunction(fn any) (Task, error) {
	v := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(v.Pointer()).Name()
	f := &taskFunction{fn: v, fullName: fullName}
	return f, f.validateFn(v.Type())
}

func (f *taskFunction) Execute(ctx context.Context, logger *slog.Logger) error {
	return f.ExecuteArgs(ctx, logger, nil)
}

// ExecuteArgs resolves the function's parameters — injectables from the
// context and args onto the data parameters — and invokes it. A resolution
// error (arity or type mismatch, xcom pull/decode failure) fails the task
// before its body runs.
func (f *taskFunction) ExecuteArgs(
	ctx context.Context,
	logger *slog.Logger,
	args []binding.Arg,
) error {
	var sdkClient sdk.Client
	if injected, ok := ctx.Value(sdkcontext.SdkClientContextKey).(sdk.Client); ok {
		sdkClient = injected
	} else {
		sdkClient = sdk.NewClient()
	}

	reflectArgs, err := f.plan.Resolve(ctx, logger, sdkClient, args)
	if err != nil {
		return err
	}
	slog.Debug("Attempting to call fn", "fn", f.fn, "args", reflectArgs)
	retValues := f.fn.Call(reflectArgs)

	err = nil
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
	return err
}

func (f *taskFunction) sendXcom(
	ctx context.Context,
	value any,
	c sdk.XComClient,
	logger *slog.Logger,
) {
	workload := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
	err := c.PushXCom(ctx, workload.TI, api.XComReturnValueKey, value)
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

var errorType = reflect.TypeFor[error]()

func isError(inType reflect.Type) bool {
	return inType != nil && inType.Implements(errorType)
}
