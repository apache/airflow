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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// InputBinding names the upstream task whose return-value XCom fills one data
// parameter of a task function. Bindings are positional: the i-th binding
// fills the i-th data parameter.
type InputBinding struct {
	TaskID string
}

// boundInput is one resolved data parameter: which argument slot to fill,
// from which upstream task, decoding into which type.
type boundInput struct {
	argIndex int
	taskID   string
	typ      reflect.Type
}

type taskFunction struct {
	fn       reflect.Value
	fullName string
	inputs   []boundInput
}

var _ Task = (*taskFunction)(nil)

// NewTaskFunction wraps a plain Go function as a Task, validating its
// signature (injectable parameters, data parameters matching inputs, and a
// return of error or (result, error)). Bundle authors normally use Dag.Task,
// which calls this for them; use it directly only when building a Task
// outside the registry.
//
// inputs bind the function's data parameters (every parameter that is not an
// injectable runtime type) positionally to upstream tasks: at execution each
// data parameter receives the named task's return-value XCom, decoded into
// the parameter's type. The number of inputs must equal the number of data
// parameters.
func NewTaskFunction(fn any, inputs ...InputBinding) (Task, error) {
	v := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(v.Pointer()).Name()
	f := &taskFunction{fn: v, fullName: fullName}
	if err := f.validateFn(v.Type()); err != nil {
		return nil, err
	}
	if err := f.bindInputs(v.Type(), inputs); err != nil {
		return nil, err
	}
	return f, nil
}

// dataParamTypes returns the types of fnType's data parameters — every
// parameter Execute does not inject — in declaration order.
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

// isInjectable reports whether Execute fills a parameter of this type from
// the runtime (rather than from an upstream task's XCom).
func isInjectable(in reflect.Type) bool {
	return isTIRunContext(in) || isContext(in) || isLogger(in) || isClient(in)
}

// bindInputs pairs the fn's data parameters with the supplied input bindings
// and records the pull-and-decode plan Execute runs before calling fn.
func (f *taskFunction) bindInputs(fnType reflect.Type, inputs []InputBinding) error {
	var dataIdx []int
	for i := range fnType.NumIn() {
		if !isInjectable(fnType.In(i)) {
			dataIdx = append(dataIdx, i)
		}
	}
	if len(dataIdx) != len(inputs) {
		return fmt.Errorf(
			"task function %s declares %d data parameter(s) but %d input binding(s) were supplied",
			f.fullName, len(dataIdx), len(inputs),
		)
	}
	f.inputs = make([]boundInput, len(inputs))
	for i, in := range inputs {
		typ := fnType.In(dataIdx[i])
		if in.TaskID == "" {
			return fmt.Errorf(
				"task function %s: input binding %d has an empty task id", f.fullName, i,
			)
		}
		if !isDecodableType(typ) {
			return fmt.Errorf(
				"task function %s: data parameter %d has type %s, which cannot hold an XCom value",
				f.fullName, dataIdx[i], typ,
			)
		}
		f.inputs[i] = boundInput{argIndex: dataIdx[i], taskID: in.TaskID, typ: typ}
	}
	return nil
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
			// A data parameter: placeholder until resolveInputs overwrites it
			// below (validation guarantees each has an input binding).
			reflectArgs[i] = reflect.Zero(in)
		}
	}

	if err := f.resolveInputs(ctx, sdkClient, logger, reflectArgs); err != nil {
		return err
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

// resolveInputs fills the data-parameter argument slots by pulling each bound
// upstream task's return-value XCom for the current dag run and decoding it
// into the parameter's type. Any failure fails the task before its body runs.
func (f *taskFunction) resolveInputs(
	ctx context.Context,
	c sdk.XComClient,
	logger *slog.Logger,
	args []reflect.Value,
) error {
	if len(f.inputs) == 0 {
		return nil
	}
	workload, ok := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
	if !ok {
		return fmt.Errorf(
			"task function %s: no workload in context; cannot resolve task inputs", f.fullName,
		)
	}
	ti := workload.TI
	for _, in := range f.inputs {
		// Pull from the upstream's unmapped instance; mapped upstream fan-in
		// is out of scope for now.
		raw, err := c.GetXCom(ctx, ti.DagId, ti.RunId, in.taskID, nil, api.XComReturnValueKey, nil)
		if err != nil {
			return fmt.Errorf(
				"task function %s: pulling input from task %q: %w", f.fullName, in.taskID, err,
			)
		}
		decoded, err := decodeXCom(raw, in.typ)
		if err != nil {
			return fmt.Errorf(
				"task function %s: decoding input from task %q into %s: %w",
				f.fullName, in.taskID, in.typ, err,
			)
		}
		logger.DebugContext(
			ctx,
			"Resolved task input",
			"from_task",
			in.taskID,
			"type",
			in.typ.String(),
		)
		args[in.argIndex] = decoded
	}
	return nil
}

// decodeXCom decodes a raw (generically deserialised) XCom value into target.
// Decoding into a struct is strict: unknown/renamed keys fail rather than
// silently leaving fields zero. Decoding into a map / interface accepts any
// shape, so authors opt into loose decoding by typing the parameter
// map[string]any or any. A null value is allowed only for a nilable target.
func decodeXCom(raw any, target reflect.Type) (reflect.Value, error) {
	out := reflect.New(target)

	if raw == nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			return out.Elem(), nil
		default:
			return reflect.Value{}, fmt.Errorf(
				"xcom value is null but parameter type %s is not nilable", target,
			)
		}
	}

	blob, err := json.Marshal(raw)
	if err != nil {
		return reflect.Value{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(blob))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return out.Elem(), nil
}

// isDecodableType reports whether a value can be JSON-decoded into inType. It
// rejects kinds json cannot target (func, chan, unsafe pointer) and non-empty
// interfaces (only the empty interface `any` is a valid decode target).
func isDecodableType(inType reflect.Type) bool {
	switch inType.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	case reflect.Interface:
		return inType.NumMethod() == 0
	}
	return true
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

// validateParam rejects interface parameters Execute cannot inject and that
// cannot serve as data parameters either; they would be bound to nil and
// panic on first use. Non-interface parameters are data parameters, filled
// from upstream XComs via the Inputs bindings.
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
	if in.NumMethod() == 0 {
		// The empty interface is a valid (loose) data-parameter target.
		return nil
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
