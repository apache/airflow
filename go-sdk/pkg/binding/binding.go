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
// argument values it is called with at execution time. It is shared by both
// execution paths (the coordinator runtime and the Edge worker), so it depends
// only on the SDK surface and not on the bundle registry.
//
// Two kinds of parameter are supported:
//
//   - Injectable runtime values: context.Context, *slog.Logger, and
//     sdk.Client (or the narrower sdk.VariableClient / sdk.ConnectionClient).
//   - XCom-input structs: a struct (or pointer to one) whose exported fields
//     each carry an `xcom:"<task_id>[,key=<key>]"` tag. Each field is pulled
//     from the named upstream task in the current dag run and decoded into the
//     field's type, so an author receives an upstream's return value as a typed
//     parameter without calling the client explicitly.
//
// Analyze inspects a function once at registration and returns a Plan; Resolve
// builds the call arguments for each execution from that Plan.
package binding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// paramKind classifies how a task-function parameter is filled at execution.
type paramKind int

const (
	paramContext paramKind = iota
	paramLogger
	paramClient
	paramXComInput
)

// xcomField records how to pull and decode one field of an XCom-input struct.
type xcomField struct {
	fieldIndex int
	taskID     string
	key        string
	fieldType  reflect.Type
}

// paramPlan describes how Resolve fills a single task-function parameter. For
// an XCom-input struct (kind == paramXComInput) the remaining fields describe
// the struct to build and the per-field pulls to perform.
type paramPlan struct {
	kind       paramKind
	isPtr      bool
	structType reflect.Type
	fields     []xcomField
}

// Plan is the precomputed recipe for filling a task function's parameters. It
// is built once by Analyze and reused for every execution of that function.
type Plan struct {
	fnName string
	params []paramPlan
}

// Analyze inspects the parameters of a task function type and builds a Plan.
// fnName appears in error messages only. Every parameter must be an injectable
// runtime type (context.Context, *slog.Logger, sdk.Client / VariableClient /
// ConnectionClient) or an XCom-input struct whose exported fields each carry an
// `xcom:"<task_id>[,key=<key>]"` tag; anything else is a registration error.
func Analyze(fnType reflect.Type, fnName string) (*Plan, error) {
	params := make([]paramPlan, fnType.NumIn())
	for i := range fnType.NumIn() {
		p, err := classifyParam(fnName, fnType.In(i))
		if err != nil {
			return nil, err
		}
		params[i] = p
	}
	return &Plan{fnName: fnName, params: params}, nil
}

// Resolve builds the ordered argument values for one call: injectable
// parameters receive ctx, logger, or client, and XCom-input structs are pulled
// from their upstream tasks (over client) and decoded. An error fails the task
// before its body runs.
func (p *Plan) Resolve(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
) ([]reflect.Value, error) {
	args := make([]reflect.Value, len(p.params))
	for i, plan := range p.params {
		switch plan.kind {
		case paramContext:
			args[i] = reflect.ValueOf(ctx)
		case paramLogger:
			args[i] = reflect.ValueOf(logger)
		case paramClient:
			args[i] = reflect.ValueOf(client)
		case paramXComInput:
			arg, err := p.resolveXComInput(ctx, client, plan)
			if err != nil {
				return nil, err
			}
			args[i] = arg
		}
	}
	return args, nil
}

// classifyParam decides how a single parameter is filled. Injectable runtime
// types map to their paramKind; anything else must be an XCom-input struct (or
// pointer to one) whose exported fields are all `xcom`-tagged.
func classifyParam(fnName string, in reflect.Type) (paramPlan, error) {
	switch {
	case isContext(in):
		return paramPlan{kind: paramContext}, nil
	case isLogger(in):
		return paramPlan{kind: paramLogger}, nil
	case isClient(in):
		return paramPlan{kind: paramClient}, nil
	}

	structType := in
	isPtr := false
	if in.Kind() == reflect.Pointer {
		structType = in.Elem()
		isPtr = true
	}
	if structType.Kind() != reflect.Struct {
		return paramPlan{}, fmt.Errorf(
			"task function %s: parameter of type %s is not an injectable type "+
				"(context.Context, *slog.Logger, sdk.Client/VariableClient/ConnectionClient) "+
				"nor an xcom-input struct",
			fnName, in,
		)
	}

	var fields []xcomField
	for fi := range structType.NumField() {
		sf := structType.Field(fi)
		if !sf.IsExported() {
			continue
		}
		tag, ok := sf.Tag.Lookup("xcom")
		if !ok {
			return paramPlan{}, fmt.Errorf(
				"task function %s: field %s.%s has no `xcom` tag; every exported field "+
					"of an xcom-input struct must be tagged `xcom:\"<task_id>\"`",
				fnName, structType.Name(), sf.Name,
			)
		}
		taskID, key, err := parseXComTag(tag)
		if err != nil {
			return paramPlan{}, fmt.Errorf(
				"task function %s: field %s.%s: %w", fnName, structType.Name(), sf.Name, err,
			)
		}
		if !isDecodableType(sf.Type) {
			return paramPlan{}, fmt.Errorf(
				"task function %s: field %s.%s has type %s which cannot hold an xcom value",
				fnName, structType.Name(), sf.Name, sf.Type,
			)
		}
		fields = append(fields, xcomField{
			fieldIndex: fi,
			taskID:     taskID,
			key:        key,
			fieldType:  sf.Type,
		})
	}

	if len(fields) == 0 {
		return paramPlan{}, fmt.Errorf(
			"task function %s: xcom-input struct %s has no exported `xcom`-tagged fields",
			fnName, structType.Name(),
		)
	}

	return paramPlan{
		kind:       paramXComInput,
		isPtr:      isPtr,
		structType: structType,
		fields:     fields,
	}, nil
}

// parseXComTag parses an `xcom` struct tag of the form "<task_id>" or
// "<task_id>,key=<key>". The key defaults to the return-value XCom key.
func parseXComTag(tag string) (taskID, key string, err error) {
	parts := strings.Split(tag, ",")
	taskID = strings.TrimSpace(parts[0])
	if taskID == "" {
		return "", "", fmt.Errorf("xcom tag is missing the upstream task id")
	}
	key = api.XComReturnValueKey
	for _, opt := range parts[1:] {
		opt = strings.TrimSpace(opt)
		if opt == "" {
			continue
		}
		val, ok := strings.CutPrefix(opt, "key=")
		if !ok {
			return "", "", fmt.Errorf("unknown xcom tag option %q", opt)
		}
		if val == "" {
			return "", "", fmt.Errorf("xcom tag option key= is empty")
		}
		key = val
	}
	return taskID, key, nil
}

// resolveXComInput builds the XCom-input struct described by plan, pulling each
// tagged field from its upstream task in the current dag run and decoding it
// into the field's type.
func (p *Plan) resolveXComInput(
	ctx context.Context,
	c sdk.XComClient,
	plan paramPlan,
) (reflect.Value, error) {
	workload, ok := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
	if !ok {
		return reflect.Value{}, fmt.Errorf(
			"task function %s: no workload in context, cannot resolve xcom inputs", p.fnName,
		)
	}

	structPtr := reflect.New(plan.structType)
	structVal := structPtr.Elem()
	for _, xf := range plan.fields {
		// Pull from the upstream's unmapped instance (map_index -1); mapped
		// upstream fan-in is out of scope for now.
		raw, err := c.GetXCom(
			ctx,
			workload.TI.DagId,
			workload.TI.RunId,
			xf.taskID,
			nil,
			xf.key,
			nil,
		)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: pulling xcom from %q (key %q): %w",
				p.fnName,
				xf.taskID,
				xf.key,
				err,
			)
		}
		decoded, err := decodeXCom(raw, xf.fieldType)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: decoding xcom from %q into field %s: %w",
				p.fnName, xf.taskID, xf.fieldType, err,
			)
		}
		structVal.Field(xf.fieldIndex).Set(decoded)
	}

	if plan.isPtr {
		return structPtr, nil
	}
	return structVal, nil
}

// decodeXCom decodes a raw (generically deserialised) XCom value into target.
// Decoding into a struct is strict: unknown/renamed keys fail rather than
// silently leaving fields zero. Decoding into a map / interface accepts any
// shape, so authors opt into loose decoding by typing the field map[string]any
// or any. A null value is allowed only for a nilable target.
func decodeXCom(raw any, target reflect.Type) (reflect.Value, error) {
	out := reflect.New(target)

	if raw == nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			return out.Elem(), nil
		default:
			return reflect.Value{}, fmt.Errorf(
				"xcom value is null but field type %s is not nilable", target,
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
	contextType    = reflect.TypeFor[context.Context]()
	slogLoggerType = reflect.TypeFor[*slog.Logger]()

	connClientType = reflect.TypeFor[sdk.ConnectionClient]()
	varClientType  = reflect.TypeFor[sdk.VariableClient]()
	clientType     = reflect.TypeFor[sdk.Client]()
)

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
