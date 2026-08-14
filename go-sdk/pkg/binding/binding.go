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

// Package binding turns a task function's parameter list into the argument
// values it is called with at execution time.
//
// Parameters are either injectable runtime values -- context.Context,
// sdk.TIRunContext, *slog.Logger, any subset of sdk.Client -- filled by type in
// any position, or data parameters filled from the argument spec the Python
// stub Dag captured from the TaskFlow call. Literals decode directly; XCom
// arguments are pulled from their upstream task first, concurrently.
//
// Data parameters bind positionally, so order matters and every one must be
// filled. The exception is a sole (pointer-to-)struct data parameter, which
// binds by field name instead: each exported field claims the argument named by
// its `arg:"<name>"` tag, or its verbatim Go field name when untagged. That is
// kwarg-shaped, so an unmatched field stays at its zero value while an
// explicitly passed argument no field claims fails the task -- except that an
// untagged struct receiving a single unclaimed argument decodes whole from it,
// which is how a struct receives an upstream object.
//
// Analyze builds a Plan per function once at registration, rejecting signatures
// that cannot work -- notably an `arg:`-tagged struct alongside other data
// parameters, whose tags would silently do nothing. Resolve binds each
// execution's spec onto that Plan and fails the task before its body runs.
// Because the flat-vs-name-bound choice is made per execution, one function can
// bind either way in different Dags.
//
// Mapped (.expand()) stubs are out of scope on this wire version: their specs
// carry no bindings, so an XCom pull always targets the unmapped upstream.
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

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Arg is one positional argument, in declaration order: an XComArg or a
// LiteralArg. Sealed, and each variant is defined in terms of its generated
// schema struct, so the runtime types cannot drift from the wire model.
type Arg interface {
	// ArgName is the stub parameter name this binding fills, matched against a
	// struct field's `arg:` tag in name-based binding.
	ArgName() string
	// Schema is the pydantic-generated fragment for the argument, or nil when
	// it is unconstrained.
	Schema() *genmodels.ArgValueSchema
	// sealedArg keeps resolveOne's type switch the single exhaustive consumer.
	sealedArg()
}

// XComArg sources the argument from an upstream task's return-value XCom.
// Kind carries the wire discriminant ("xcom") from the generated shape; the
// resolve path dispatches on the Go type itself and never reads it.
type XComArg genmodels.XComArgBinding

// LiteralArg carries an inline value from the Dag file. Kind carries the wire
// discriminant ("literal") from the generated shape; the resolve path
// dispatches on the Go type itself and never reads it.
type LiteralArg genmodels.LiteralArgBinding

func (a XComArg) ArgName() string    { return a.Name }
func (a LiteralArg) ArgName() string { return a.Name }

func (a XComArg) Schema() *genmodels.ArgValueSchema    { return a.ValueSchema }
func (a LiteralArg) Schema() *genmodels.ArgValueSchema { return a.ValueSchema }

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
	// paramLoneStruct binds by field name, or decodes one argument whole; the
	// choice is made per execution.
	paramLoneStruct
)

// structField describes how Resolve fills one exported field of a name-bound
// struct parameter. Precomputed once by Analyze.
type structField struct {
	structIndex int
	// goName is the Go field name, for error messages.
	goName    string
	fieldType reflect.Type
	// argName is the field's `arg:` tag, or its Go field name verbatim.
	argName string
}

// paramPlan describes how Resolve fills a single task-function parameter.
type paramPlan struct {
	kind paramKind
	// typ is the data parameter's declared type, or the struct type when
	// kind == paramLoneStruct.
	typ reflect.Type
	// index is the position in the signature, for error messages.
	index int
	// fields and tagged are set only for kind == paramLoneStruct; tagged opts
	// the struct out of the whole-value fallback.
	fields []structField
	tagged bool
}

// Plan is the precomputed recipe for filling a task function's parameters. It
// is built once by Analyze and reused for every execution of that function.
type Plan struct {
	fnName     string
	params     []paramPlan
	numData    int
	loneStruct bool
}

// Analyze inspects the parameters of a task function type and builds a Plan.
// fnName appears in error messages only. Every parameter must be an injectable
// runtime type or a type that can receive a task argument (JSON-decodable);
// anything else is a registration error.
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

	// Any other struct data parameter is decoded whole from its positional slot.
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

	// `arg:` tags only take effect in the sole-struct name-binding path above;
	// a tagged struct alongside other data parameters would silently ignore its
	// tags (it is decoded whole), so reject it as a likely mistake.
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

// Resolve builds the ordered argument values for one call, failing the task
// before its body runs.
//
// client must be the full sdk.Client -- not just the sdk.XComClient the resolve
// helpers narrow to -- because a paramClient parameter receives it verbatim.
func (p *Plan) Resolve(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	args []Arg,
) ([]reflect.Value, error) {
	out := make([]reflect.Value, len(p.params))
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
		case paramData, paramLoneStruct:
			// Filled below, once the spec is matched and XComs are pulled.
		}
	}
	if p.loneStruct {
		return p.resolveLoneStructParam(ctx, client, args, out)
	}
	return p.resolveFlatParams(ctx, client, args, out)
}

// resolveFlatParams fills the plain data parameters positionally -- args[0]
// onto the first data parameter, and so on -- with strict arity (see the
// package doc comment).
func (p *Plan) resolveFlatParams(
	ctx context.Context,
	c sdk.XComClient,
	args []Arg,
	out []reflect.Value,
) ([]reflect.Value, error) {
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

// resolveLoneStructParam fills the sole struct data parameter by field name.
// Entries the Python side captured from stub defaults (FromDefault) may go
// unclaimed, the same way an unpassed keyword argument never reaches the
// callee; every explicitly passed one must be claimed.
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
	for i, a := range args {
		if a != nil {
			byName[a.ArgName()] = i
		}
	}

	claimed := make([]bool, len(args))
	type fieldBind struct {
		field  structField
		argIdx int
	}
	binds := make([]fieldBind, 0, len(plan.fields))
	for _, sf := range plan.fields {
		idx, ok := byName[sf.argName]
		if !ok {
			// Kwarg-style: an unpassed name leaves the field at its zero value.
			continue
		}
		claimed[idx] = true
		binds = append(binds, fieldBind{field: sf, argIdx: idx})
	}

	// A struct carrying `arg:` tags has opted into name binding, so it never
	// takes the whole-value fallback: otherwise a typo'd tag would quietly
	// decode the argument whole instead of reporting the name that matched
	// nothing. (A field name that collides with an argument name is still bound
	// by name -- only the author's tags distinguish the two modes.)
	if len(binds) == 0 && !plan.tagged && isLoneWholeValueArg(args) {
		return p.resolveWholeStructParam(ctx, c, args, plan, paramIdx, out)
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
			// The Dag author never passed this argument; the Python side filled
			// it from the stub signature's default. A struct that does not
			// mirror the defaulted parameter is fine.
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
		structVal.Field(b.field.structIndex).Set(v)
	}

	if isPtr {
		out[paramIdx] = structVal.Addr()
	} else {
		out[paramIdx] = structVal
	}
	return out, nil
}

// isLoneWholeValueArg reports whether args is a single explicitly passed
// argument (not a captured default) -- the case a sole struct parameter decodes
// whole rather than binding field-by-field.
func isLoneWholeValueArg(args []Arg) bool {
	if len(args) != 1 || args[0] == nil {
		return false
	}
	lit, ok := args[0].(LiteralArg)
	return !ok || !lit.FromDefault
}

// resolveWholeStructParam decodes the sole struct parameter's one positional
// argument whole into it, honouring pointer-ness (decodeArg allocates through
// plan.typ, the same as a flat data parameter).
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

// fetchArgValues produces the raw (pre-decode) value for each argument the
// caller will consume: a literal's inline value, or the upstream task's
// return-value XCom pulled over the API -- independent pulls run
// concurrently. needed selects which entries to fetch; nil means all.
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
		// A nil or foreign Arg implementation fails in decodeArg, which names
		// the destination parameter/field in the error.
	}
	if len(xcomIdxs) == 0 {
		return raws, nil
	}

	workload, ok := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
	if !ok {
		return nil, fmt.Errorf(
			"task function %s: no workload in context, cannot resolve xcom arguments", p.fnName,
		)
	}
	// Always the return-value XCom from the unmapped upstream: a stub Dag
	// cannot reference another key, and mapped fan-in is out of scope.
	pull := func(i int) error {
		a := args[i].(XComArg)
		raw, err := c.GetXCom(
			ctx, workload.TI.DagId, workload.TI.RunId, a.TaskID, nil, api.XComReturnValueKey, nil,
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
	if len(xcomIdxs) == 1 {
		if err := pull(xcomIdxs[0]); err != nil {
			return nil, err
		}
		return raws, nil
	}
	var wg sync.WaitGroup
	errs := make([]error, len(xcomIdxs))
	for j, i := range xcomIdxs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[j] = pull(i)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return raws, nil
}

// decodeArg decodes one argument-spec entry's raw value into targetType:
// type-check against the declared Dag type, then a strict decode. errCtx
// names the destination parameter or struct field for error messages.
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
				"(func, chan and unsafe-pointer values cannot be decoded, at any depth)",
			fnName, index, in,
		)
	}
	return paramPlan{kind: paramData, typ: in, index: index}, nil
}

// structParamType reports whether in (after dereferencing one pointer level,
// matching checkValueType's convention) is a struct, returning that struct type
// or nil.
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

// hasArgTag reports whether structType has an exported field carrying a
// non-empty `arg:` tag -- the signal that its author means for it to bind
// TaskFlow arguments by name.
func hasArgTag(structType reflect.Type) bool {
	for i := range structType.NumField() {
		f := structType.Field(i)
		if f.IsExported() && f.Tag.Get("arg") != "" {
			return true
		}
	}
	return false
}

// buildStructFields validates and precomputes the by-name field-binding plan
// for a sole struct parameter. It runs once at registration time so a
// misconfigured struct fails loudly before any task ever executes.
func buildStructFields(
	fnName string,
	structType reflect.Type,
	paramIndex int,
) ([]structField, error) {
	var fields []structField
	seenArgNames := make(map[string]string) // resolved arg name -> Go field name that claims it

	for i := range structType.NumField() {
		f := structType.Field(i)
		if !f.IsExported() {
			continue
		}

		if !isDecodableType(f.Type) {
			return nil, fmt.Errorf(
				"task function %s: parameter %d: struct field %s: type %s cannot receive a task "+
					"argument (func/chan/unsafe-pointer values cannot be decoded)",
				fnName,
				paramIndex,
				f.Name,
				f.Type,
			)
		}

		sf := structField{structIndex: i, goName: f.Name, fieldType: f.Type}
		sf.argName = f.Tag.Get("arg")
		if sf.argName == "" {
			sf.argName = f.Name
		}
		if existing, ok := seenArgNames[sf.argName]; ok {
			return nil, fmt.Errorf(
				"task function %s: parameter %d: struct fields %s and %s both bind arg name %q",
				fnName, paramIndex, existing, f.Name, sf.argName,
			)
		}
		seenArgNames[sf.argName] = f.Name
		fields = append(fields, sf)
	}
	return fields, nil
}

// checkValueType verifies the Dag-declared value schema can bind to the Go
// parameter type. The fragment is open-vocabulary JSON schema, so anything it
// cannot speak to -- no schema, no plain-string "type" (e.g. a union's
// ["string","null"]), an unknown type, an `any` parameter -- skips the check and
// leaves the strict decode to fail on unusable values.
func checkValueType(schema *genmodels.ArgValueSchema, target reflect.Type) error {
	if schema == nil {
		return nil
	}
	jsonType, ok := (*schema)["type"].(string)
	if !ok {
		return nil
	}
	t := target
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Interface {
		return nil
	}
	// A type that decodes itself maps the schema's JSON type onto its own Go
	// kind however it likes -- a Python `datetime` ships as a "string" and binds
	// onto time.Time, whose kind is struct -- so the kind table below cannot
	// judge it.
	if implementsUnmarshaler(target) || implementsUnmarshaler(t) {
		return nil
	}
	bindable := false
	switch jsonType {
	case "string":
		bindable = t.Kind() == reflect.String
	case "integer":
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			// Every JSON integer decodes into a Go float, so an int-annotated
			// stub parameter binds onto one; narrowing is the decode's call.
			reflect.Float32, reflect.Float64:
			bindable = true
		}
	case "number":
		bindable = t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64
	case "boolean":
		bindable = t.Kind() == reflect.Bool
	case "object":
		bindable = t.Kind() == reflect.Struct || t.Kind() == reflect.Map
	case "array":
		bindable = t.Kind() == reflect.Slice || t.Kind() == reflect.Array
	default:
		// A type keyword this runtime does not recognize; JSON schema is
		// open-vocabulary, so leave it to the strict decode.
		return nil
	}
	if !bindable {
		return fmt.Errorf(
			"the Dag declares JSON-schema type %q which cannot bind to Go parameter type %s",
			jsonType, target,
		)
	}
	return nil
}

// decodeValue decodes a raw value into target. Structs decode strictly, so an
// unknown or renamed key fails rather than silently leaving a field zero;
// authors opt into loose decoding by typing the parameter map[string]any or
// any. A null value is allowed only for a nilable target.
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

// isDecodableType reports whether a value can be JSON-decoded into inType,
// rejecting kinds json cannot target (func, chan, unsafe pointer) and non-empty
// interfaces. It recurses, so a nested one is caught once at registration
// rather than partway through every execution's decode.
func isDecodableType(inType reflect.Type) bool {
	return isDecodableTypeSeen(inType, map[reflect.Type]bool{})
}

func isDecodableTypeSeen(inType reflect.Type, seen map[reflect.Type]bool) bool {
	if implementsUnmarshaler(inType) {
		return true
	}
	switch inType.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	case reflect.Interface:
		return inType.NumMethod() == 0
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return isDecodableTypeSeen(inType.Elem(), seen)
	case reflect.Map:
		return isDecodableTypeSeen(inType.Key(), seen) &&
			isDecodableTypeSeen(inType.Elem(), seen)
	case reflect.Struct:
		if seen[inType] {
			// Self-referential type: the enclosing call is already validating it.
			return true
		}
		seen[inType] = true
		for i := range inType.NumField() {
			f := inType.Field(i)
			if f.IsExported() && !isDecodableTypeSeen(f.Type, seen) {
				return false
			}
		}
	}
	return true
}

// implementsUnmarshaler reports whether t decodes itself from JSON, so its
// wire representation is its own business rather than something the schema
// type check or the decodable-kind walk should secondguess. The pointer is
// checked too, since UnmarshalJSON is conventionally declared on it.
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
