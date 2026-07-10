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

// Package binding turns a task function's parameter list into the concrete
// argument values it is called with at execution time.
//
// Two kinds of parameter are supported:
//
//   - Injectable runtime values: context.Context, sdk.TIRunContext,
//     *slog.Logger, and any interface whose method set is a subset of
//     sdk.Client. These are filled by type, in any position.
//   - Data parameters: everything else, in declaration order. They receive the
//     positional arguments the Python stub Dag captured at parse time from the
//     TaskFlow call (“transform("uk", extract())“) and delivered in
//     StartupDetails. A literal argument decodes directly; an XCom argument is
//     pulled from the named upstream task in the current dag run first.
//
// Analyze inspects a function once at registration and returns a Plan; Resolve
// builds the call arguments for each execution from that Plan and the
// per-execution argument spec, failing loudly on arity or type mismatches. A
// declared type of "any" (or a Go parameter typed any) opts that argument out
// of the type check; the decode step still fails loudly on unusable values.
//
// Mapped upstream fan-in is out of scope: XCom arguments always pull the
// unmapped upstream instance (map_index is never sent).
package binding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// ArgKind discriminates how one positional argument is sourced.
type ArgKind string

const (
	ArgKindXCom    ArgKind = "xcom"
	ArgKindLiteral ArgKind = "literal"
)

// DataType is the language-neutral value type the Dag declared for an
// argument (from the stub function's annotation on the Python side).
type DataType string

const (
	DataTypeString  DataType = "string"
	DataTypeInteger DataType = "integer"
	DataTypeNumber  DataType = "number"
	DataTypeBoolean DataType = "boolean"
	DataTypeObject  DataType = "object"
	DataTypeArray   DataType = "array"
	DataTypeAny     DataType = "any"
)

// Arg is one positional argument for a task function's data parameters, in
// declaration order. It is a runtime-neutral mirror of the wire model so this
// package stays decoupled from the generated coordinator schema types.
type Arg struct {
	Kind ArgKind
	// TaskID is the upstream task to pull from. Set only for ArgKindXCom.
	TaskID string
	// Key is the XCom key to pull; empty means the return-value key. Set only
	// for ArgKindXCom.
	Key string
	// Value is the literal value from the Dag file. Set only for ArgKindLiteral.
	Value any
	// DataType is the declared type to check the Go parameter against; empty
	// is treated as DataTypeAny.
	DataType DataType
}

// paramKind classifies how a task-function parameter is filled at execution.
type paramKind int

const (
	paramTIRunContext paramKind = iota
	paramContext
	paramLogger
	paramClient
	paramData
)

// paramPlan describes how Resolve fills a single task-function parameter.
type paramPlan struct {
	kind paramKind
	// typ is the declared Go type of a data parameter (kind == paramData).
	typ reflect.Type
	// index is the parameter's position in the function signature, for error
	// messages.
	index int
}

// Plan is the precomputed recipe for filling a task function's parameters. It
// is built once by Analyze and reused for every execution of that function.
type Plan struct {
	fnName  string
	params  []paramPlan
	numData int
}

// NumData returns how many data parameters the analyzed function declares.
func (p *Plan) NumData() int { return p.numData }

// Analyze inspects the parameters of a task function type and builds a Plan.
// fnName appears in error messages only. Every parameter must be an injectable
// runtime type or a type that can receive a task argument (JSON-decodable);
// anything else is a registration error.
func Analyze(fnType reflect.Type, fnName string) (*Plan, error) {
	p := &Plan{fnName: fnName, params: make([]paramPlan, fnType.NumIn())}
	for i := range fnType.NumIn() {
		plan, err := classifyParam(fnName, fnType.In(i), i)
		if err != nil {
			return nil, err
		}
		if plan.kind == paramData {
			p.numData++
		}
		p.params[i] = plan
	}
	return p, nil
}

// Resolve builds the ordered argument values for one call. Injectable
// parameters receive values derived from ctx, logger, or client; data
// parameters consume args in declaration order. An error fails the task
// before its body runs.
func (p *Plan) Resolve(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	args []Arg,
) ([]reflect.Value, error) {
	if len(args) != p.numData {
		return nil, fmt.Errorf(
			"task function %s: argument count mismatch: the Dag passes %d positional argument(s) "+
				"but the Go function declares %d data parameter(s)",
			p.fnName, len(args), p.numData,
		)
	}
	out := make([]reflect.Value, len(p.params))
	argIdx := 0
	for i, plan := range p.params {
		switch plan.kind {
		case paramTIRunContext:
			// The runtime stores the identifiers/timestamps under
			// RuntimeContextKey; rebuild the value around the live task context.
			var ti sdk.TaskInstance
			var dagRun sdk.DagRun
			if stored, ok := ctx.Value(sdkcontext.RuntimeContextKey).(sdk.TIRunContext); ok {
				ti, dagRun = stored.TaskInstance(), stored.DagRun()
			}
			out[i] = reflect.ValueOf(sdk.NewTIRunContext(ctx, ti, dagRun))
		case paramContext:
			out[i] = reflect.ValueOf(ctx)
		case paramLogger:
			out[i] = reflect.ValueOf(logger)
		case paramClient:
			out[i] = reflect.ValueOf(client)
		case paramData:
			arg := args[argIdx]
			v, err := p.resolveData(ctx, client, plan, arg, argIdx)
			if err != nil {
				return nil, err
			}
			out[i] = v
			argIdx++
		}
	}
	return out, nil
}

// resolveData produces the value for one data parameter from its argument
// spec: type-check against the declared Dag type, then decode a literal or
// pull-and-decode an XCom.
func (p *Plan) resolveData(
	ctx context.Context,
	c sdk.XComClient,
	plan paramPlan,
	arg Arg,
	argIdx int,
) (reflect.Value, error) {
	if err := checkDataType(arg.DataType, plan.typ); err != nil {
		return reflect.Value{}, fmt.Errorf(
			"task function %s: argument %d (parameter %d): %w", p.fnName, argIdx, plan.index, err,
		)
	}
	switch arg.Kind {
	case ArgKindLiteral:
		v, err := decodeValue(arg.Value, plan.typ)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: argument %d: decoding literal value into %s: %w",
				p.fnName, argIdx, plan.typ, err,
			)
		}
		return v, nil
	case ArgKindXCom:
		workload, ok := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
		if !ok {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: no workload in context, cannot resolve xcom argument %d",
				p.fnName, argIdx,
			)
		}
		key := arg.Key
		if key == "" {
			key = api.XComReturnValueKey
		}
		// Pull from the upstream's unmapped instance (map_index nil); mapped
		// upstream fan-in is out of scope for now.
		raw, err := c.GetXCom(ctx, workload.TI.DagId, workload.TI.RunId, arg.TaskID, nil, key, nil)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: argument %d: pulling xcom from task %q (key %q): %w",
				p.fnName, argIdx, arg.TaskID, key, err,
			)
		}
		v, err := decodeValue(raw, plan.typ)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: argument %d: decoding xcom from task %q into %s: %w",
				p.fnName, argIdx, arg.TaskID, plan.typ, err,
			)
		}
		return v, nil
	default:
		return reflect.Value{}, fmt.Errorf(
			"task function %s: argument %d: unknown argument kind %q", p.fnName, argIdx, arg.Kind,
		)
	}
}

// classifyParam decides how a single parameter is filled. Injectable runtime
// types map to their paramKind; anything else is a data parameter and must be
// a type a task argument can decode into.
func classifyParam(fnName string, in reflect.Type, index int) (paramPlan, error) {
	switch {
	case isTIRunContext(in):
		// sdk.TIRunContext embeds context.Context, so it also satisfies
		// isContext - this case must come first.
		return paramPlan{kind: paramTIRunContext, index: index}, nil
	case isContext(in):
		// The plain task context injected here cannot satisfy extra methods.
		if !contextType.Implements(in) {
			return paramPlan{}, fmt.Errorf(
				"task function %s: parameter %d: interface %s adds methods on top of "+
					"context.Context; declare sdk.TIRunContext or a separate parameter instead",
				fnName, index, in,
			)
		}
		return paramPlan{kind: paramContext, index: index}, nil
	case isLogger(in):
		return paramPlan{kind: paramLogger, index: index}, nil
	case isClient(in):
		return paramPlan{kind: paramClient, index: index}, nil
	}
	if in.Kind() == reflect.Interface && in.NumMethod() > 0 {
		return paramPlan{}, fmt.Errorf(
			"task function %s: parameter %d: interface %s is not injectable "+
				"(want context.Context, sdk.TIRunContext, or a subset of sdk.Client): %s",
			fnName, index, in, explainClientMismatch(in),
		)
	}
	if !isDecodableType(in) {
		return paramPlan{}, fmt.Errorf(
			"task function %s: parameter %d: type %s cannot receive a task argument "+
				"(func/chan/unsafe-pointer values cannot be decoded)",
			fnName, index, in,
		)
	}
	return paramPlan{kind: paramData, typ: in, index: index}, nil
}

// checkDataType verifies the Dag-declared type can bind to the Go parameter
// type. One pointer level is dereferenced first; DataTypeAny (or an empty
// declaration) skips the check, as does an `any` parameter.
func checkDataType(dt DataType, target reflect.Type) error {
	if dt == "" || dt == DataTypeAny {
		return nil
	}
	t := target
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Interface {
		return nil
	}
	ok := false
	switch dt {
	case DataTypeString:
		ok = t.Kind() == reflect.String
	case DataTypeInteger:
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			ok = true
		}
	case DataTypeNumber:
		ok = t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64
	case DataTypeBoolean:
		ok = t.Kind() == reflect.Bool
	case DataTypeObject:
		ok = t.Kind() == reflect.Struct || t.Kind() == reflect.Map
	case DataTypeArray:
		ok = t.Kind() == reflect.Slice || t.Kind() == reflect.Array
	default:
		return fmt.Errorf("unknown declared type %q in the argument spec", dt)
	}
	if !ok {
		return fmt.Errorf(
			"the Dag declares type %q which cannot bind to Go parameter type %s", dt, target,
		)
	}
	return nil
}

// decodeValue decodes a raw (generically deserialised) value into target.
// Decoding into a struct is strict: unknown/renamed keys fail rather than
// silently leaving fields zero. Decoding into a map / interface accepts any
// shape, so authors opt into loose decoding by typing the parameter
// map[string]any or any. A null value is allowed only for a nilable target.
func decodeValue(raw any, target reflect.Type) (reflect.Value, error) {
	out := reflect.New(target)

	if raw == nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			return out.Elem(), nil
		default:
			return reflect.Value{}, fmt.Errorf(
				"value is null but the parameter type %s is not nilable", target,
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

var (
	contextType      = reflect.TypeFor[context.Context]()
	tiRunContextType = reflect.TypeFor[sdk.TIRunContext]()
	slogLoggerType   = reflect.TypeFor[*slog.Logger]()
	clientType       = reflect.TypeFor[sdk.Client]()
)

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
