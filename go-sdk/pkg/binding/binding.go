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
// Parameters fall into two groups:
//
//   - Injectable runtime values: context.Context, sdk.TIRunContext,
//     *slog.Logger, and any interface whose method set is a subset of
//     sdk.Client. These are filled by type, in any position.
//   - Data parameters: everything else, in declaration order. They receive the
//     positional arguments the Python stub Dag captured at parse time from the
//     TaskFlow call (“transform("uk", extract())“) and delivered in
//     StartupDetails. A literal argument decodes directly; an XCom argument is
//     pulled from the named upstream task in the current Dag run first
//     (independent pulls run concurrently). A struct data parameter is decoded
//     whole from its one positional argument.
//
// Data parameters are positional-argument binding: order matters, every
// parameter must be filled, and Resolve fails the task before its body runs on
// an arity or type mismatch. A declared type of "any" (or a Go parameter typed
// any) opts that argument out of the type check; the decode step still fails
// loudly on unusable values.
//
// One shape is treated specially: a function whose sole data parameter is a
// (pointer-to-)struct binds by field name (kwarg-style) instead, using the
// per-execution argument spec. Each exported field claims the argument named by
// its `arg:"<name>"` tag, or its verbatim Go field name when untagged -- so an
// untagged Name binds the argument "Name", and a snake_case argument like
// "count" needs an explicit tag. A field whose name has no corresponding
// argument is left at its Go zero value rather than failing the task, the same
// way an unpassed keyword argument falls back to its default; conversely every
// explicitly passed argument must be claimed by some field (catching typo'd
// field names), while entries the Python side captured from the stub
// signature's defaults (from_default on the wire) may go unclaimed. When the
// sole struct instead receives exactly one explicitly passed argument that no
// field claims, it falls back to a whole-value decode of that argument -- so a
// struct can still receive an upstream object as one argument. A bindable-field
// struct fails loudly when no argument spec arrives at all (e.g. the Edge
// Worker path, where nothing could ever fill it), matching the flat-parameter
// arity check.
//
// Because `arg:` tags only take effect in that sole-struct name-binding path, a
// struct data parameter that carries an `arg:` tag must be the only data
// parameter; Analyze rejects a signature that pairs a tagged struct with other
// data parameters rather than silently ignoring the tags.
//
// Analyze inspects a function once at registration and returns a Plan; Resolve
// builds the call arguments for each execution from that Plan and the
// per-execution argument spec. The flat-vs-name-bound choice for a sole struct
// parameter is made per execution, so the same function can bind either way in
// different Dags.
//
// Mapped upstream fan-in is out of scope: the wire spec may carry a map_index /
// element_index, but this version ignores them and always pulls the unmapped
// upstream instance.
package binding

import (
	"bytes"
	"context"
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

// Arg is one positional argument for a task function's data parameters, in
// declaration order: an XComArg or a LiteralArg. A sealed sum type over the
// wire model's XComArgBinding/LiteralArgBinding split; each variant is
// defined in terms of its generated schema struct so the fields cannot drift
// from the coordinator protocol.
type Arg interface {
	// ArgName is the stub function's parameter name this binding fills; used
	// to match a struct field's `arg:` tag (or its verbatim field-name
	// fallback) in name-based binding.
	ArgName() string
	// Schema is the JSON-schema fragment the Dag declared for the argument
	// (generated by pydantic from the stub annotation), or nil when the
	// argument is unconstrained; the type check reads its "type" keyword and
	// falls back to a decode-only check when nil or absent.
	Schema() *genmodels.ArgValueSchema
	// sealedArg restricts implementations to this package, keeping
	// resolveOne's type switch the single exhaustive consumer.
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
	// paramLoneStruct is the function's sole data parameter and is a
	// (pointer-to-)struct. Resolve decides per execution whether to bind its
	// fields by name (kwarg-style) or decode one argument whole into it.
	paramLoneStruct
)

// structField describes how Resolve fills one exported field of a name-bound
// struct parameter. Precomputed once by Analyze.
type structField struct {
	// structIndex is the field's index within the struct, for
	// reflect.Value.Field.
	structIndex int
	// goName is the Go field name, for error messages.
	goName    string
	fieldType reflect.Type
	// argName is the name to claim from the argument spec: the field's `arg:`
	// tag, or its Go field name verbatim when the tag is omitted.
	argName string
}

// paramPlan describes how Resolve fills a single task-function parameter.
type paramPlan struct {
	kind paramKind
	// typ is the declared Go type of a data parameter (kind == paramData) or
	// the struct type (kind == paramLoneStruct).
	typ reflect.Type
	// index is the parameter's position in the function signature, for error
	// messages.
	index int
	// fields describes each exported field's binding. Set only for
	// kind == paramLoneStruct.
	fields []structField
}

// Plan is the precomputed recipe for filling a task function's parameters. It
// is built once by Analyze and reused for every execution of that function.
type Plan struct {
	fnName  string
	params  []paramPlan
	numData int
	// loneStruct is true when the sole data parameter is a struct resolved
	// per execution (kind == paramLoneStruct).
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

	// A function whose sole data parameter is a struct binds its fields by name
	// at execution (see resolveLoneStructParam); any other struct data
	// parameter is decoded whole from its positional slot.
	if p.numData == 1 {
		i := dataIdxs[0]
		if st := structParamType(p.params[i].typ); st != nil {
			fields, err := buildStructFields(fnName, st, i)
			if err != nil {
				return nil, err
			}
			p.params[i].kind = paramLoneStruct
			p.params[i].fields = fields
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

// Resolve builds the ordered argument values for one call. Injectable
// parameters receive values derived from ctx, logger, or client. Plain flat
// data parameters consume args in declaration order; a sole struct data
// parameter is instead resolved by field name or decoded whole (see Analyze
// and resolveLoneStructParam). An error fails the task before its body runs.
//
// client must be the full sdk.Client -- not just the sdk.XComClient the
// resolve helpers narrow to -- because a paramClient parameter receives the
// client itself, verbatim.
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

// resolveLoneStructParam fills the function's sole struct data parameter. It
// binds by field name (kwarg-style) by default: every explicitly passed spec
// entry must be claimed by some field; entries the Python side captured from
// the stub signature's defaults (FromDefault) may go unclaimed, the same way an
// unpassed keyword argument never reaches the callee. When instead exactly one
// explicitly passed argument arrives that no field claims, the struct is
// decoded whole from it (see resolveWholeStructParam).
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
			// No TaskFlow call argument carries this name -- kwarg-style, an
			// unpassed name leaves the field at its Go zero value rather than
			// failing the task (see the package doc comment).
			continue
		}
		claimed[idx] = true
		binds = append(binds, fieldBind{field: sf, argIdx: idx})
	}

	// Whole-value fallback: a single explicitly passed argument that no field
	// claims decodes whole into the struct, so a sole struct parameter can still
	// receive an upstream object as one positional argument.
	if len(binds) == 0 && isLoneWholeValueArg(args) {
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
	// Always the return-value XCom -- a stub Dag cannot reference any other
	// key. Pull from the upstream's unmapped instance (map_index nil); mapped
	// upstream fan-in is out of scope for now.
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
				"(func/chan/unsafe-pointer values cannot be decoded)",
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
// parameter type. The schema is an open-vocabulary JSON-schema fragment; only
// its "type" keyword is inspected, and one pointer level is dereferenced first.
// A nil schema, a schema whose "type" is absent or not a plain string (e.g. a
// union's ["string","null"]), an unrecognized type, or an `any` parameter all
// skip the check -- the strict decode still fails loudly on unusable values.
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
	bindable := false
	switch jsonType {
	case "string":
		bindable = t.Kind() == reflect.String
	case "integer":
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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
