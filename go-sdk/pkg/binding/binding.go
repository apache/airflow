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
// Three kinds of parameter are supported:
//
//   - Injectable runtime values: context.Context, sdk.TIRunContext,
//     *slog.Logger, and any interface whose method set is a subset of
//     sdk.Client. These are filled by type, in any position.
//   - Data parameters: everything else (except TaskInput structs, below), in
//     declaration order. They receive the positional arguments the Python
//     stub Dag captured at parse time from the TaskFlow call
//     (“transform("uk", extract())“) and delivered in StartupDetails. A
//     literal argument decodes directly; an XCom argument is pulled from the
//     named upstream task in the current dag run first.
//   - TaskInput structs: a struct that anonymously embeds sdk.TaskInput opts
//     into per-field, name-based binding instead of consuming one positional
//     slot as a whole-value decode target. Each exported field binds by name
//     against the Dag's TaskFlow call arguments: an `arg:"<name>"` tag names
//     the argument to claim, and a field with no tag falls back to its own Go
//     field name, snake_cased. At most one such parameter is allowed per
//     function.
//
// A TaskInput struct's fields are resolved first, by name, claiming entries
// out of the argument spec; the remaining unclaimed entries are then
// distributed, in their original relative order, onto the plain flat data
// parameters in declaration order -- so flat parameters and a TaskInput
// struct can coexist in the same function signature regardless of where each
// sits, and (with no TaskInput struct present) this reduces to exactly
// today's positional-only behaviour.
//
// Conceptually, flat data parameters are positional-argument binding: order
// matters, and every parameter must be filled or Resolve fails the task
// before its body runs. A TaskInput struct is closer to keyword-argument
// binding: fields match by name, and a field whose name has no corresponding
// TaskFlow call argument is simply left at its Go zero value instead of
// failing the task -- the same way an unpassed keyword argument falls back
// to its default in a kwargs-style call.
//
// Analyze inspects a function once at registration and returns a Plan; Resolve
// builds the call arguments for each execution from that Plan and the
// per-execution argument spec, failing loudly on arity or type mismatches of
// flat data parameters. A declared type of "any" (or a Go parameter typed
// any) opts that argument out of the type check; the decode step still fails
// loudly on unusable values.
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
	"strings"
	"unicode"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
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
// declaration order: an XComArg or a LiteralArg. A sealed sum type mirroring
// the wire model's XComArgBinding/LiteralArgBinding split, kept runtime-neutral
// so this package stays decoupled from the generated coordinator schema types.
type Arg interface {
	// ArgName is the stub function's parameter name this binding fills; used
	// to match a TaskInput struct field's `arg:` tag (or its snake_cased
	// field-name fallback).
	ArgName() string
	// DeclaredType is the Dag-declared language-neutral type for the argument;
	// empty is treated as DataTypeAny.
	DeclaredType() DataType
	// sealedArg restricts implementations to this package, keeping
	// resolveOne's type switch the single exhaustive consumer.
	sealedArg()
}

// XComArg sources the argument from an upstream task's XCom.
type XComArg struct {
	// Name is the stub function's parameter name this binding fills.
	Name string
	// TaskID is the upstream task to pull from.
	TaskID string
	// Key is the XCom key to pull; empty means the return-value key.
	Key string
	// DataType is the declared type to check the Go parameter against.
	DataType DataType
}

// LiteralArg carries an inline value from the Dag file.
type LiteralArg struct {
	// Name is the stub function's parameter name this binding fills.
	Name string
	// Value is the literal value from the Dag file.
	Value any
	// DataType is the declared type to check the Go parameter against.
	DataType DataType
}

func (a XComArg) ArgName() string    { return a.Name }
func (a LiteralArg) ArgName() string { return a.Name }

func (a XComArg) DeclaredType() DataType    { return a.DataType }
func (a LiteralArg) DeclaredType() DataType { return a.DataType }

func (XComArg) sealedArg()    {}
func (LiteralArg) sealedArg() {}

// paramKind classifies how a task-function parameter is filled at execution.
type paramKind int

const (
	paramTIRunContext paramKind = iota
	paramContext
	paramLogger
	paramClient
	paramData
	// paramTaskInput is a struct that anonymously embeds sdk.TaskInput,
	// opting into per-field, name-based binding instead of consuming one
	// positional slot as a whole-value decode target.
	paramTaskInput
)

// taskInputField describes how Resolve fills one exported field of a
// TaskInput-embedding struct. Precomputed once by Analyze.
type taskInputField struct {
	// structIndex is the field's index within the struct, for
	// reflect.Value.Field.
	structIndex int
	// goName is the Go field name, for error messages.
	goName    string
	fieldType reflect.Type
	// argName is the name to claim from the argument spec: the field's `arg:`
	// tag, or its snake_cased Go name when the tag is omitted.
	argName string
}

// paramPlan describes how Resolve fills a single task-function parameter.
type paramPlan struct {
	kind paramKind
	// typ is the declared Go type of a data parameter (kind == paramData) or
	// the struct type (kind == paramTaskInput).
	typ reflect.Type
	// index is the parameter's position in the function signature, for error
	// messages.
	index int
	// fields describes each exported field's binding. Set only for
	// kind == paramTaskInput.
	fields []taskInputField
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
	seenTaskInput := -1
	for i := range fnType.NumIn() {
		plan, err := classifyParam(fnName, fnType.In(i), i)
		if err != nil {
			return nil, err
		}
		if plan.kind == paramTaskInput {
			if seenTaskInput >= 0 {
				return nil, fmt.Errorf(
					"task function %s: parameter %d: only one TaskInput struct parameter is allowed "+
						"per function (parameter %d already is one)",
					fnName,
					i,
					seenTaskInput,
				)
			}
			seenTaskInput = i
		}
		if plan.kind == paramData {
			p.numData++
		}
		p.params[i] = plan
	}
	return p, nil
}

// Resolve builds the ordered argument values for one call. Injectable
// parameters receive values derived from ctx, logger, or client. A TaskInput
// struct's fields are resolved first, by name, claiming entries out of args;
// the remaining unclaimed entries are then distributed, in their original
// relative order, onto the plain flat data parameters in declaration order.
// An error fails the task before its body runs.
func (p *Plan) Resolve(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	args []Arg,
) ([]reflect.Value, error) {
	byName := make(map[string]int, len(args))
	for i, a := range args {
		// A nil entry can claim no name; it stays unclaimed and fails loudly in
		// resolveOne when the flat-parameter cursor reaches it.
		if a != nil {
			byName[a.ArgName()] = i
		}
	}
	claimed := make([]bool, len(args))

	out := make([]reflect.Value, len(p.params))
	for i, plan := range p.params {
		if plan.kind != paramTaskInput {
			continue
		}
		v, err := p.resolveTaskInput(ctx, client, plan, args, byName, claimed)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}

	remaining := 0
	for _, c := range claimed {
		if !c {
			remaining++
		}
	}
	if remaining != p.numData {
		return nil, fmt.Errorf(
			"task function %s: argument count mismatch: the Dag passes %d positional argument(s) "+
				"but the Go function declares %d data parameter(s)",
			p.fnName, remaining, p.numData,
		)
	}

	argCursor := 0
	nextUnclaimed := func() Arg {
		for argCursor < len(args) {
			i := argCursor
			argCursor++
			if !claimed[i] {
				return args[i]
			}
		}
		// Unreachable: the remaining/numData check above guarantees enough
		// unclaimed args exist for every flat parameter still to be filled.
		panic("binding: exhausted unclaimed args despite a passing arity check")
	}

	flatIdx := 0
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
		case paramTaskInput:
			// Already resolved above.
		case paramData:
			v, err := p.resolveData(ctx, client, plan, nextUnclaimed(), flatIdx)
			if err != nil {
				return nil, err
			}
			out[i] = v
			flatIdx++
		}
	}
	return out, nil
}

// resolveData produces the value for one flat data parameter from its
// argument spec.
func (p *Plan) resolveData(
	ctx context.Context,
	c sdk.XComClient,
	plan paramPlan,
	arg Arg,
	argIdx int,
) (reflect.Value, error) {
	return p.resolveOne(
		ctx, c, plan.typ, arg,
		fmt.Sprintf("argument %d (parameter %d)", argIdx, plan.index),
		fmt.Sprintf("argument %d", argIdx),
	)
}

// resolveTaskInput builds the struct value for one TaskInput parameter. A
// field claiming an argument spec entry by name marks it claimed so the
// later flat-parameter cursor skips it. A field whose name claims nothing
// (kwarg-style: it was never "passed") is left unset at its Go zero value
// rather than failing the task.
func (p *Plan) resolveTaskInput(
	ctx context.Context,
	client sdk.Client,
	plan paramPlan,
	args []Arg,
	byName map[string]int,
	claimed []bool,
) (reflect.Value, error) {
	structType := plan.typ
	isPtr := structType.Kind() == reflect.Pointer
	if isPtr {
		structType = structType.Elem()
	}
	structVal := reflect.New(structType).Elem()

	for _, tif := range plan.fields {
		idx, ok := byName[tif.argName]
		if !ok {
			// No TaskFlow call argument carries this name -- kwarg-style, an
			// unpassed name leaves the field at its Go zero value rather than
			// failing the task (unlike a flat data parameter, where arity is
			// checked strictly; see the package doc comment).
			continue
		}
		claimed[idx] = true

		v, err := p.resolveOne(
			ctx, client, tif.fieldType, args[idx],
			fmt.Sprintf("TaskInput field %s (parameter %d)", tif.goName, plan.index),
			fmt.Sprintf("TaskInput field %s", tif.goName),
		)
		if err != nil {
			return reflect.Value{}, err
		}
		structVal.Field(tif.structIndex).Set(v)
	}

	if isPtr {
		return structVal.Addr(), nil
	}
	return structVal, nil
}

// resolveOne decodes one argument-spec entry into a value assignable to
// targetType: type-check against the declared Dag type, then decode a
// literal or pull-and-decode an XCom. Shared by resolveData (one flat
// parameter) and resolveTaskInput (one TaskInput struct field) so a literal-
// or xcom-kind entry resolves identically regardless of which parameter
// shape it fills. typeCheckCtx/generalCtx are error-message prefixes: the
// former (used only for the type-check error) additionally names the
// parameter index, matching this package's existing error conventions.
func (p *Plan) resolveOne(
	ctx context.Context,
	c sdk.XComClient,
	targetType reflect.Type,
	arg Arg,
	typeCheckCtx string,
	generalCtx string,
) (reflect.Value, error) {
	if arg == nil {
		return reflect.Value{}, fmt.Errorf(
			"task function %s: %s: nil argument binding", p.fnName, generalCtx,
		)
	}
	if err := checkDataType(arg.DeclaredType(), targetType); err != nil {
		return reflect.Value{}, fmt.Errorf("task function %s: %s: %w", p.fnName, typeCheckCtx, err)
	}
	switch a := arg.(type) {
	case LiteralArg:
		v, err := decodeValue(a.Value, targetType)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: %s: decoding literal value into %s: %w",
				p.fnName, generalCtx, targetType, err,
			)
		}
		return v, nil
	case XComArg:
		workload, ok := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
		if !ok {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: %s: no workload in context, cannot resolve xcom argument",
				p.fnName, generalCtx,
			)
		}
		key := a.Key
		if key == "" {
			key = api.XComReturnValueKey
		}
		// Pull from the upstream's unmapped instance (map_index nil); mapped
		// upstream fan-in is out of scope for now.
		raw, err := c.GetXCom(ctx, workload.TI.DagId, workload.TI.RunId, a.TaskID, nil, key, nil)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: %s: pulling xcom from task %q (key %q): %w",
				p.fnName, generalCtx, a.TaskID, key, err,
			)
		}
		v, err := decodeValue(raw, targetType)
		if err != nil {
			return reflect.Value{}, fmt.Errorf(
				"task function %s: %s: decoding xcom from task %q into %s: %w",
				p.fnName, generalCtx, a.TaskID, targetType, err,
			)
		}
		return v, nil
	default:
		return reflect.Value{}, fmt.Errorf(
			"task function %s: %s: unsupported argument binding %T", p.fnName, generalCtx, arg,
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
	if structType := taskInputStructType(in); structType != nil {
		fields, err := buildTaskInputFields(fnName, structType, index)
		if err != nil {
			return paramPlan{}, err
		}
		return paramPlan{kind: paramTaskInput, typ: in, index: index, fields: fields}, nil
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

// taskInputStructType reports whether in (after dereferencing one pointer
// level, matching checkDataType's convention) is a struct that anonymously
// embeds sdk.TaskInput, returning that struct type or nil.
func taskInputStructType(in reflect.Type) reflect.Type {
	t := in
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous && f.Type == taskInputType {
			return t
		}
	}
	return nil
}

// buildTaskInputFields validates and precomputes the field-binding plan for a
// TaskInput-embedding struct parameter. It runs once at registration time so
// a misconfigured struct fails loudly before any task ever executes.
func buildTaskInputFields(
	fnName string,
	structType reflect.Type,
	paramIndex int,
) ([]taskInputField, error) {
	var fields []taskInputField
	seenArgNames := make(map[string]string) // resolved arg name -> Go field name that claims it

	for i := range structType.NumField() {
		f := structType.Field(i)
		if f.Anonymous && f.Type == taskInputType {
			continue
		}
		if !f.IsExported() {
			continue
		}

		if !isDecodableType(f.Type) {
			return nil, fmt.Errorf(
				"task function %s: parameter %d: TaskInput field %s: type %s cannot receive a task "+
					"argument (func/chan/unsafe-pointer values cannot be decoded)",
				fnName,
				paramIndex,
				f.Name,
				f.Type,
			)
		}

		tif := taskInputField{structIndex: i, goName: f.Name, fieldType: f.Type}
		tif.argName = f.Tag.Get("arg")
		if tif.argName == "" {
			tif.argName = snakeCase(f.Name)
		}
		if existing, ok := seenArgNames[tif.argName]; ok {
			return nil, fmt.Errorf(
				"task function %s: parameter %d: TaskInput fields %s and %s both bind arg name %q",
				fnName, paramIndex, existing, f.Name, tif.argName,
			)
		}
		seenArgNames[tif.argName] = f.Name
		fields = append(fields, tif)
	}
	return fields, nil
}

// snakeCase converts a Go exported field name (UpperCamelCase, acronyms
// preserved as a run) to the wire's snake_case convention, e.g.
// "RatioValue" -> "ratio_value", "TaskID" -> "task_id". Used as the fallback
// arg name for a TaskInput struct field with no explicit `arg:` tag.
func snakeCase(name string) string {
	runes := []rune(name)
	var b strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			prevLower := i > 0 && unicode.IsLower(runes[i-1])
			prevUpper := i > 0 && unicode.IsUpper(runes[i-1])
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if i > 0 && (prevLower || (prevUpper && nextLower)) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
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
	taskInputType    = reflect.TypeFor[sdk.TaskInput]()
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
