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

// Package binding resolves TaskFlow arguments for Go task functions.
//
// Runtime values are injected by type. Other parameters bind positionally,
// except a sole struct whose fields bind by `arg:` tag or folded Go name.
// Captured defaults may go unclaimed, and a sole untagged struct can decode one
// unclaimed argument as a whole value.
//
// Analyze validates a function once. Resolve binds each execution.
package binding

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Arg is a literal or XCom-backed TaskFlow argument.
type Arg interface {
	// ArgName returns the stub parameter name.
	ArgName() string
	// Schema returns the argument schema, if declared.
	Schema() *genmodels.ArgValueSchema
	sealedArg()
}

// XComArg reads an upstream task's return-value XCom.
type XComArg genmodels.XComArgBinding

// LiteralArg carries an inline value from the Dag file.
type LiteralArg genmodels.LiteralArgBinding

func (a XComArg) ArgName() string    { return a.Name }
func (a LiteralArg) ArgName() string { return a.Name }

func (a XComArg) Schema() *genmodels.ArgValueSchema    { return a.ValueSchema }
func (a LiteralArg) Schema() *genmodels.ArgValueSchema { return a.ValueSchema }

func (XComArg) sealedArg()    {}
func (LiteralArg) sealedArg() {}

type paramKind int

const (
	paramTIRunContext paramKind = iota
	paramContext
	paramLogger
	paramClient
	paramData
	paramLoneStruct
)

type structField struct {
	index     []int
	goName    string
	fieldType reflect.Type
	argName   string
	tagged    bool
}

type paramPlan struct {
	kind   paramKind
	typ    reflect.Type
	index  int
	fields []structField
	tagged bool
}

// Plan describes how to fill a task function's parameters.
type Plan struct {
	fnName     string
	params     []paramPlan
	numData    int
	loneStruct bool
}

// Analyze validates a task function and builds its binding plan.
func Analyze(fnType reflect.Type, fnName string) (*Plan, error) {
	p := &Plan{fnName: fnName, params: make([]paramPlan, fnType.NumIn())}
	var dataIdxs []int
	for i := range fnType.NumIn() {
		plan, err := classifyParam(fnName, fnType.In(i), i)
		if err != nil {
			return nil, err
		}
		if plan.kind == paramData {
			p.numData++
			dataIdxs = append(dataIdxs, i)
		}
		p.params[i] = plan
	}

	if p.numData == 1 {
		i := dataIdxs[0]
		if st := structParamType(p.params[i].typ); st != nil {
			fields, err := buildStructFields(fnName, st, i)
			if err != nil {
				return nil, err
			}
			p.params[i].kind = paramLoneStruct
			p.params[i].fields = fields
			p.params[i].tagged = hasArgTag(st)
			p.numData = 0
			p.loneStruct = true
			return p, nil
		}
	}

	// Tags on a non-sole struct would be silently ignored during whole-value decoding.
	for _, i := range dataIdxs {
		if st := structParamType(p.params[i].typ); st != nil && hasArgTag(st) {
			return nil, fmt.Errorf(
				"task function %s: parameter %d: a struct with `arg:` tags must be the function's "+
					"only data parameter (its fields bind TaskFlow arguments by name); it cannot be "+
					"combined with other data parameters",
				fnName, i,
			)
		}
	}
	return p, nil
}

// Resolve builds the ordered values for one task call.
func (p *Plan) Resolve(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	args []Arg,
) ([]reflect.Value, error) {
	out := p.resolveInjectables(ctx, logger, client)
	if p.loneStruct {
		return p.resolveLoneStructParam(ctx, client, args, out)
	}
	return p.resolveFlatParams(ctx, client, args, out)
}

func (p *Plan) resolveInjectables(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
) []reflect.Value {
	out := make([]reflect.Value, len(p.params))
	for i, plan := range p.params {
		switch plan.kind {
		case paramTIRunContext:
			// Rebuild the stored metadata around the live task context.
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
		case paramData, paramLoneStruct:
		}
	}
	return out
}

func (p *Plan) resolveFlatParams(
	ctx context.Context,
	c sdk.XComClient,
	args []Arg,
	out []reflect.Value,
) ([]reflect.Value, error) {
	// Captured defaults may exceed the Go function's arity.
	if len(args) != p.numData {
		args = dropDefaultedArgs(args)
	}
	if len(args) != p.numData {
		return nil, fmt.Errorf(
			"task function %s: argument count mismatch: the Dag passes %d positional argument(s) "+
				"but the Go function declares %d data parameter(s)",
			p.fnName, len(args), p.numData,
		)
	}
	raws, err := p.fetchArgValues(ctx, c, args, nil)
	if err != nil {
		return nil, err
	}
	flatIdx := 0
	for i, plan := range p.params {
		if plan.kind != paramData {
			continue
		}
		v, err := p.decodeArg(
			args[flatIdx], raws[flatIdx], plan.typ,
			fmt.Sprintf("argument %d (parameter %d)", flatIdx, plan.index),
		)
		if err != nil {
			return nil, err
		}
		out[i] = v
		flatIdx++
	}
	return out, nil
}

func (p *Plan) resolveLoneStructParam(
	ctx context.Context,
	c sdk.XComClient,
	args []Arg,
	out []reflect.Value,
) ([]reflect.Value, error) {
	var paramIdx int
	var plan paramPlan
	for i, pl := range p.params {
		if pl.kind == paramLoneStruct {
			paramIdx, plan = i, pl
			break
		}
	}

	byName := make(map[string]int, len(args))
	// Do not guess when folded names collide.
	byFolded := make(map[string]int, len(args))
	ambiguous := make(map[string]bool, len(args))
	for i, a := range args {
		if a == nil {
			continue
		}
		byName[a.ArgName()] = i
		key := foldArgName(a.ArgName())
		if _, dup := byFolded[key]; dup {
			ambiguous[key] = true
			continue
		}
		byFolded[key] = i
	}

	claimed := make([]bool, len(args))
	type fieldBind struct {
		field  structField
		argIdx int
	}
	binds := make([]fieldBind, 0, len(plan.fields))
	for _, sf := range plan.fields {
		idx, ok := byName[sf.argName]
		if !ok && !sf.tagged {
			key := foldArgName(sf.argName)
			if !ambiguous[key] {
				idx, ok = byFolded[key]
			}
		}
		if !ok {
			continue
		}
		claimed[idx] = true
		binds = append(binds, fieldBind{field: sf, argIdx: idx})
	}

	// Tagged structs never fall back to whole-value decoding, which would hide tag typos.
	if len(binds) == 0 && !plan.tagged {
		if explicit, ok := loneWholeValueArgs(args); ok {
			return p.resolveWholeStructParam(ctx, c, explicit, plan, paramIdx, out)
		}
	}

	if len(args) == 0 && len(plan.fields) > 0 {
		return nil, fmt.Errorf(
			"task function %s: no TaskFlow arg bindings arrived but the struct declares "+
				"%d bindable field(s); nothing can fill them on this execution path",
			p.fnName, len(plan.fields),
		)
	}

	var unclaimed []string
	for i, c := range claimed {
		if c {
			continue
		}
		if lit, ok := args[i].(LiteralArg); ok && lit.FromDefault {
			continue
		}
		name := "<nil>"
		if args[i] != nil {
			name = fmt.Sprintf("%q", args[i].ArgName())
		}
		unclaimed = append(unclaimed, name)
	}
	if len(unclaimed) > 0 {
		return nil, fmt.Errorf(
			"task function %s: %d TaskFlow call argument(s) not claimed by any struct "+
				"field: %s",
			p.fnName, len(unclaimed), strings.Join(unclaimed, ", "),
		)
	}

	raws, err := p.fetchArgValues(ctx, c, args, claimed)
	if err != nil {
		return nil, err
	}

	structType := plan.typ
	isPtr := structType.Kind() == reflect.Pointer
	if isPtr {
		structType = structType.Elem()
	}
	structVal := reflect.New(structType).Elem()
	for _, b := range binds {
		v, err := p.decodeArg(
			args[b.argIdx], raws[b.argIdx], b.field.fieldType,
			fmt.Sprintf("struct field %s (parameter %d)", b.field.goName, plan.index),
		)
		if err != nil {
			return nil, err
		}
		structVal.FieldByIndex(b.field.index).Set(v)
	}

	if isPtr {
		out[paramIdx] = structVal.Addr()
	} else {
		out[paramIdx] = structVal
	}
	return out, nil
}

func dropDefaultedArgs(args []Arg) []Arg {
	kept := make([]Arg, 0, len(args))
	for _, a := range args {
		if lit, ok := a.(LiteralArg); ok && lit.FromDefault {
			continue
		}
		kept = append(kept, a)
	}
	return kept
}

func loneWholeValueArgs(args []Arg) ([]Arg, bool) {
	explicit := dropDefaultedArgs(args)
	if len(explicit) != 1 || explicit[0] == nil {
		return nil, false
	}
	return explicit, true
}

func (p *Plan) resolveWholeStructParam(
	ctx context.Context,
	c sdk.XComClient,
	args []Arg,
	plan paramPlan,
	paramIdx int,
	out []reflect.Value,
) ([]reflect.Value, error) {
	raws, err := p.fetchArgValues(ctx, c, args, nil)
	if err != nil {
		return nil, err
	}
	v, err := p.decodeArg(
		args[0], raws[0], plan.typ,
		fmt.Sprintf("argument %q (parameter %d)", args[0].ArgName(), plan.index),
	)
	if err != nil {
		return nil, err
	}
	out[paramIdx] = v
	return out, nil
}

func (p *Plan) fetchArgValues(
	ctx context.Context,
	c sdk.XComClient,
	args []Arg,
	needed []bool,
) ([]any, error) {
	raws := make([]any, len(args))
	var xcomIdxs []int
	for i, a := range args {
		if needed != nil && !needed[i] {
			continue
		}
		switch a := a.(type) {
		case LiteralArg:
			raws[i] = a.Value
		case XComArg:
			xcomIdxs = append(xcomIdxs, i)
		}
	}
	if len(xcomIdxs) == 0 {
		return raws, nil
	}

	runtimeContext, ok := ctx.Value(sdkcontext.RuntimeContextKey).(sdk.TIRunContext)
	if !ok {
		return nil, fmt.Errorf(
			"task function %s: no task runtime context, cannot resolve xcom arguments", p.fnName,
		)
	}
	ti := runtimeContext.TaskInstance()
	// Stop sibling pulls after the first failure.
	pullCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Stub calls reference only the unmapped upstream return value.
	pull := func(i int) error {
		a := args[i].(XComArg)
		raw, err := c.GetXCom(
			pullCtx, ti.DagID, ti.RunID, a.TaskID, nil,
			sdk.XComReturnValueKey, nil,
		)
		if err != nil {
			return fmt.Errorf(
				"task function %s: argument %q: pulling xcom from task %q: %w",
				p.fnName, a.Name, a.TaskID, err,
			)
		}
		raws[i] = raw
		return nil
	}
	var wg sync.WaitGroup
	errs := make([]error, len(xcomIdxs))
	for j, i := range xcomIdxs {
		wg.Go(func() {
			if err := pull(i); err != nil {
				errs[j] = err
				cancel()
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return raws, nil
}

func (p *Plan) decodeArg(
	arg Arg,
	raw any,
	targetType reflect.Type,
	errCtx string,
) (reflect.Value, error) {
	if arg == nil {
		return reflect.Value{}, fmt.Errorf(
			"task function %s: %s: nil argument binding", p.fnName, errCtx,
		)
	}
	if err := checkValueType(arg.Schema(), targetType); err != nil {
		return reflect.Value{}, fmt.Errorf("task function %s: %s: %w", p.fnName, errCtx, err)
	}
	var source string
	switch a := arg.(type) {
	case LiteralArg:
		source = "literal value"
	case XComArg:
		source = fmt.Sprintf("xcom from task %q", a.TaskID)
	default:
		return reflect.Value{}, fmt.Errorf(
			"task function %s: %s: unsupported argument binding %T", p.fnName, errCtx, arg,
		)
	}
	v, err := decodeValue(raw, targetType)
	if err != nil {
		return reflect.Value{}, fmt.Errorf(
			"task function %s: %s: decoding %s into %s: %w",
			p.fnName, errCtx, source, targetType, err,
		)
	}
	return v, nil
}

func classifyParam(fnName string, in reflect.Type, index int) (paramPlan, error) {
	switch {
	case isTIRunContext(in):
		// TIRunContext also satisfies context.Context, so check it first.
		return paramPlan{kind: paramTIRunContext, index: index}, nil
	case isContext(in):
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
				"(func, chan and unsafe-pointer values cannot be decoded)",
			fnName, index, in,
		)
	}
	return paramPlan{kind: paramData, typ: in, index: index}, nil
}

func structParamType(in reflect.Type) reflect.Type {
	t := in
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	return t
}

func hasArgTag(structType reflect.Type) bool {
	for i := range structType.NumField() {
		f := structType.Field(i)
		if embedded := embeddedStructType(f); embedded != nil {
			if hasArgTag(embedded) {
				return true
			}
			continue
		}
		if f.IsExported() && f.Tag.Get("arg") != "" {
			return true
		}
	}
	return false
}

func embeddedStructType(f reflect.StructField) reflect.Type {
	if !f.Anonymous || f.Tag.Get("arg") != "" || f.Type.Kind() != reflect.Struct {
		return nil
	}
	return structParamType(f.Type)
}

func buildStructFields(
	fnName string,
	structType reflect.Type,
	paramIndex int,
) ([]structField, error) {
	var fields []structField
	// Normalized argument name to the field that claims it.
	claimed := map[string]string{}
	folded := map[string]string{}
	err := collectStructFields(
		fnName, structType, paramIndex, nil, &fields, claimed, folded,
	)
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func collectStructFields(
	fnName string,
	structType reflect.Type,
	paramIndex int,
	prefix []int,
	fields *[]structField,
	claimed map[string]string,
	folded map[string]string,
) error {
	for i := range structType.NumField() {
		f := structType.Field(i)
		index := append(append([]int{}, prefix...), i)

		// Embedded structs promote exported fields even when their type is unexported.
		if embedded := embeddedStructType(f); embedded != nil {
			err := collectStructFields(
				fnName, embedded, paramIndex, index, fields, claimed, folded,
			)
			if err != nil {
				return err
			}
			continue
		}
		if !f.IsExported() {
			continue
		}

		tag := f.Tag.Get("arg")
		if !isDecodableType(f.Type) {
			// Only an explicitly tagged, undecodable field is an error.
			if tag == "" {
				continue
			}
			return fmt.Errorf(
				"task function %s: parameter %d: struct field %s: type %s cannot receive a task "+
					"argument (func/chan/unsafe-pointer values cannot be decoded)",
				fnName, paramIndex, f.Name, f.Type,
			)
		}

		sf := structField{
			index:     index,
			goName:    f.Name,
			fieldType: f.Type,
			argName:   tag,
			tagged:    tag != "",
		}
		if sf.argName == "" {
			sf.argName = f.Name
		}
		if existing, ok := claimed[sf.argName]; ok {
			return fmt.Errorf(
				"task function %s: parameter %d: struct fields %s and %s both bind arg name %q",
				fnName, paramIndex, existing, f.Name, sf.argName,
			)
		}
		claimed[sf.argName] = f.Name
		key := foldArgName(sf.argName)
		if existing, ok := folded[key]; ok {
			return fmt.Errorf(
				"task function %s: parameter %d: struct fields %s and %s bind arg names that "+
					"differ only in case or underscores (%q), which the untagged fallback cannot "+
					"tell apart",
				fnName, paramIndex, existing, f.Name, sf.argName,
			)
		}
		folded[key] = f.Name
		*fields = append(*fields, sf)
	}
	return nil
}

func foldArgName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

type schemaShape struct {
	jsonType string
	format   string
	fragment map[string]any
}

func shapeOf(fragment map[string]any) schemaShape {
	jsonType, _ := fragment["type"].(string)
	format, _ := fragment["format"].(string)
	return schemaShape{jsonType: jsonType, format: format, fragment: fragment}
}

// Unknown schema forms are treated as unconstrained.
func schemaShapes(
	schema *genmodels.ArgValueSchema,
) (shapes []schemaShape, nullable bool, ok bool) {
	if schema == nil {
		return nil, false, false
	}
	if branches, isUnion := (*schema)["anyOf"].([]any); isUnion {
		for _, branch := range branches {
			fragment, isMap := branch.(map[string]any)
			if !isMap {
				return nil, false, false
			}
			jsonType, _ := fragment["type"].(string)
			switch jsonType {
			case "null":
				nullable = true
			case "":
				// One unknown branch makes the union unconstrained.
				return nil, false, false
			default:
				shapes = append(shapes, shapeOf(fragment))
			}
		}
		return shapes, nullable, len(shapes) > 0
	}
	if _, isString := (*schema)["type"].(string); !isString {
		return nil, false, false
	}
	// Copy the defined genmodels.JsonValue map into the plain map type.
	fragment := make(map[string]any, len(*schema))
	for k, v := range *schema {
		fragment[k] = v
	}
	return []schemaShape{shapeOf(fragment)}, false, true
}

func checkValueType(schema *genmodels.ArgValueSchema, target reflect.Type) error {
	shapes, nullable, ok := schemaShapes(schema)
	if !ok {
		return nil
	}
	if nullable && !isNilableType(target) {
		return fmt.Errorf(
			"the Dag may pass null for this argument, so Go parameter type %s must be a "+
				"pointer (or a slice, map or any)",
			target,
		)
	}
	t := target
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Interface {
		return nil
	}
	for _, shape := range shapes {
		if isBindableShape(shape, t) {
			return nil
		}
	}
	return fmt.Errorf(
		"the Dag declares %s which cannot bind to Go parameter type %s",
		describeShapes(shapes), target,
	)
}

func isBindableShape(shape schemaShape, t reflect.Type) bool {
	// Self-decoding types define their own wire representation.
	if implementsUnmarshaler(t) {
		return true
	}
	switch shape.jsonType {
	case "string":
		return t.Kind() == reflect.String
	case "integer":
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			// JSON integers can decode into Go floats.
			reflect.Float32, reflect.Float64:
			return true
		}
		return false
	case "number":
		return t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64
	case "boolean":
		return t.Kind() == reflect.Bool
	case "object":
		return t.Kind() == reflect.Struct || t.Kind() == reflect.Map
	case "array":
		return t.Kind() == reflect.Slice || t.Kind() == reflect.Array
	default:
		// A type keyword this runtime does not recognize; leave it to the decode.
		return true
	}
}

func isNilableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return true
	}
	return false
}

func describeShapes(shapes []schemaShape) string {
	parts := make([]string, len(shapes))
	for i, shape := range shapes {
		parts[i] = fmt.Sprintf("type %q", shape.jsonType)
		if shape.format != "" {
			parts[i] += fmt.Sprintf(" format %q", shape.format)
		}
	}
	if len(parts) == 1 {
		return "JSON-schema " + parts[0]
	}
	return "JSON-schema alternatives " + strings.Join(parts, " | ")
}

func decodeValue(raw any, target reflect.Type) (reflect.Value, error) {
	out := reflect.New(target)

	if raw == nil {
		if isNilableType(target) {
			return out.Elem(), nil
		}
		return reflect.Value{}, fmt.Errorf(
			"value is null but the parameter type %s is not nilable", target,
		)
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

// Struct fields are checked only if the wire value names them.
func isDecodableType(inType reflect.Type) bool {
	if implementsUnmarshaler(inType) {
		return true
	}
	switch inType.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	case reflect.Interface:
		return inType.NumMethod() == 0
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return isDecodableType(inType.Elem())
	case reflect.Map:
		return isDecodableType(inType.Key()) && isDecodableType(inType.Elem())
	}
	return true
}

func implementsUnmarshaler(t reflect.Type) bool {
	for _, cand := range []reflect.Type{t, reflect.PointerTo(t)} {
		if cand.Implements(jsonUnmarshalerType) || cand.Implements(textUnmarshalerType) {
			return true
		}
	}
	return false
}

var (
	contextType      = reflect.TypeFor[context.Context]()
	tiRunContextType = reflect.TypeFor[sdk.TIRunContext]()
	slogLoggerType   = reflect.TypeFor[*slog.Logger]()
	clientType       = reflect.TypeFor[sdk.Client]()

	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
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

// isClient reports whether inType is a non-empty subset of sdk.Client.
func isClient(inType reflect.Type) bool {
	return inType != nil && inType.Kind() == reflect.Interface &&
		inType.NumMethod() > 0 && clientType.Implements(inType)
}

func explainClientMismatch(in reflect.Type) string {
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
