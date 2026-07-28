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
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type taskFunction struct {
	fn       reflect.Value
	fullName string
}

var _ Task = (*taskFunction)(nil)

// NewTaskFunction wraps a plain Go function as a Task, validating its signature
// (injectable parameters, and a return of error or (result, error)). Bundle
// authors normally use Dag.AddTask, which calls this for them; use it directly
// only when building a Task outside the registry.
func NewTaskFunction(fn any) (Task, error) {
	v := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(v.Pointer()).Name()
	f := &taskFunction{v, fullName}
	return f, f.validateFn(v.Type())
}

func (f *taskFunction) Execute(ctx context.Context, logger *slog.Logger) error {
	fnType := f.fn.Type()
	var sdkClient sdk.Client
	if injected, ok := ctx.Value(sdkcontext.SdkClientContextKey).(sdk.Client); ok {
		sdkClient = injected
	} else {
		sdkClient = sdk.NewClient()
	}

	reflectArgs := make([]reflect.Value, fnType.NumIn())
	for i := range reflectArgs {
		in := fnType.In(i)

		switch {
		case isTIRunContext(in):
			// sdk.TIRunContext embeds context.Context, so it also satisfies
			// isContext - this case must come first. The runtime stores the
			// identifiers/timestamps under RuntimeContextKey; rebuild the
			// value around the live task context here.
			var ti sdk.TaskInstance
			var dagRun sdk.DagRun
			if stored, ok := ctx.Value(sdkcontext.RuntimeContextKey).(sdk.TIRunContext); ok {
				ti, dagRun = stored.TaskInstance(), stored.DagRun()
			}
			reflectArgs[i] = reflect.ValueOf(sdk.NewTIRunContext(ctx, ti, dagRun))
		case isContext(in):
			// Plain context.Context injection is retained for the Edge Worker
			// runtime path, which does not populate the task runtime context
			// (TI/DagRun) that sdk.TIRunContext carries. New tasks should
			// declare sdk.TIRunContext instead.
			reflectArgs[i] = reflect.ValueOf(ctx)
		case isLogger(in):
			reflectArgs[i] = reflect.ValueOf(logger)
		case isClient(in):
			reflectArgs[i] = reflect.ValueOf(sdkClient)
		default:
			// TODO: deal with other value types. For now they will all be Zero values unless it's a context
			reflectArgs[i] = reflect.Zero(in)
		}
	}
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

	for i := range fnType.NumIn() {
		if err := validateParam(fnType.In(i)); err != nil {
			return fmt.Errorf("task function %s parameter %d: %w", f.fullName, i, err)
		}
	}
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

	clientType = reflect.TypeFor[sdk.Client]()
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

// isClient reports whether inType's method set is a subset of sdk.Client's,
// keeping new client capabilities injectable without a hand-kept list.
func isClient(inType reflect.Type) bool {
	return inType != nil && inType.Kind() == reflect.Interface &&
		inType.NumMethod() > 0 && clientType.Implements(inType)
}

// validateParam rejects interface parameters Execute cannot inject; they
// would be bound to nil and panic on first use.
func validateParam(in reflect.Type) error {
	if in.Kind() != reflect.Interface || isTIRunContext(in) || isClient(in) {
		return nil
	}
	if isContext(in) {
		// The plain task context injected here cannot satisfy extra methods.
		if contextType.Implements(in) {
			return nil
		}
		return fmt.Errorf(
			"interface %s adds methods on top of context.Context; declare sdk.TIRunContext or a separate parameter instead",
			in,
		)
	}
	return fmt.Errorf(
		"interface %s is not injectable (want context.Context, sdk.TIRunContext, or a subset of sdk.Client): %s",
		in,
		explainClientMismatch(in),
	)
}

// explainClientMismatch returns why in is not a subset of sdk.Client.
func explainClientMismatch(in reflect.Type) string {
	if in.NumMethod() == 0 {
		return "empty interfaces cannot be injected"
	}
	for i := range in.NumMethod() {
		m := in.Method(i)
		cm, ok := clientType.MethodByName(m.Name)
		if !ok {
			return fmt.Sprintf("sdk.Client has no method %s", m.Name)
		}
		if cm.Type != m.Type {
			return fmt.Sprintf("method %s is %s on sdk.Client, not %s", m.Name, cm.Type, m.Type)
		}
	}
	return "its method set is not a subset of sdk.Client"
}
