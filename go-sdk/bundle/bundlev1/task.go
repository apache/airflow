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

func NewTaskFunction(fn any) (Task, error) {
	v := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(v.Pointer()).Name()
	f := &taskFunction{v, fullName}
	return f, f.validateFn(v.Type())
}

func (f *taskFunction) Execute(ctx context.Context, logger *slog.Logger) error {
	fnType := f.fn.Type()
	sdkClient := sdk.NewClient()

	reflectArgs := make([]reflect.Value, fnType.NumIn())
	for i := range reflectArgs {
		in := fnType.In(i)

		switch {
		case isContext(in):
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
	errorType      = reflect.TypeFor[error]()
	contextType    = reflect.TypeFor[context.Context]()
	slogLoggerType = reflect.TypeFor[*slog.Logger]()

	connClientType = reflect.TypeFor[sdk.ConnectionClient]()
	varClientType  = reflect.TypeFor[sdk.VariableClient]()
	clientType     = reflect.TypeFor[sdk.Client]()
)

func isError(inType reflect.Type) bool {
	return inType != nil && inType.Implements(errorType)
}

func isContext(inType reflect.Type) bool {
	return inType != nil && inType.Implements(contextType)
}

func isLogger(inType reflect.Type) bool {
	return inType != nil && inType.AssignableTo(slogLoggerType)
}

func isClient(inType reflect.Type) bool {
	return inType != nil && (inType.AssignableTo(clientType) ||
		inType.AssignableTo(connClientType) ||
		inType.AssignableTo(varClientType))
}
