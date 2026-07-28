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

package binding

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type BindingSuite struct {
	suite.Suite
}

func TestBindingSuite(t *testing.T) {
	suite.Run(t, &BindingSuite{})
}

// argSchema builds a minimal JSON-schema fragment declaring only a "type",
// matching what the Python stub decorator emits for a scalar/container
// annotation.
func argSchema(jsonType string) *genmodels.ArgValueSchema {
	s := genmodels.ArgValueSchema{"type": jsonType}
	return &s
}

// fakeXComClient records GetXCom calls and returns preconfigured values.
// Resolve pulls XComs concurrently, so recording is mutex-guarded.
type fakeXComClient struct {
	sdk.Client

	values map[string]any // "<task_id>/<key>" -> raw value
	mu     sync.Mutex
	calls  []fakeXComCall
	err    error
}

type fakeXComCall struct {
	dagID, runID, taskID, key string
	mapIndex                  *int
}

func (f *fakeXComClient) GetXCom(
	ctx context.Context,
	dagID, runID, taskID string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeXComCall{dagID, runID, taskID, key, mapIndex})
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.values[taskID+"/"+key], nil
}

// workloadCtx returns a context carrying an ExecuteTaskWorkload the resolver
// reads the Dag/run identifiers from.
func workloadCtx() context.Context {
	return context.WithValue(
		context.Background(),
		sdkcontext.WorkloadContextKey,
		api.ExecuteTaskWorkload{
			TI: api.TaskInstance{
				Id:     uuid.New(),
				DagId:  "dag1",
				RunId:  "run1",
				TaskId: "transform",
			},
		},
	)
}

func analyze(s *BindingSuite, fn any) *Plan {
	plan, err := Analyze(reflect.TypeOf(fn), "testFn")
	s.Require().NoError(err)
	return plan
}

func (s *BindingSuite) resolve(fn any, args []Arg, client sdk.Client) ([]reflect.Value, error) {
	plan := analyze(s, fn)
	return plan.Resolve(workloadCtx(), slog.Default(), client, args)
}

func (s *BindingSuite) TestAnalyzeClassification() {
	plan := analyze(
		s,
		func(ctx sdk.TIRunContext, log *slog.Logger, c sdk.VariableClient, country string, extracted map[string]any) error {
			return nil
		},
	)
	s.Equal(2, plan.numData)

	s.Zero(analyze(s, func() error { return nil }).numData)
	s.Equal(
		1,
		analyze(s, func(x any) error { return nil }).numData,
		"an `any` parameter is a data parameter",
	)
}

func (s *BindingSuite) TestAnalyzeRejections() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"func-param": {
			func(cb func()) error { return nil },
			"cannot receive a task argument",
		},
		"chan-param": {
			func(ch chan int) error { return nil },
			"cannot receive a task argument",
		},
		"non-client-interface": {
			func(x interface{ NotAClientMethod() }) error { return nil },
			"sdk.Client has no method NotAClientMethod",
		},
		"context-with-extra-methods": {
			func(x interface {
				context.Context
				TaskInstance() sdk.TaskInstance
			},
			) error {
				return nil
			},
			"adds methods on top of context.Context",
		},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			_, err := Analyze(reflect.TypeOf(tt.fn), "testFn")
			if s.Assert().Error(err) {
				s.Assert().Contains(err.Error(), tt.errContains)
			}
		})
	}
}

// TestNamedClientInterfacesAreInjectable guards against sdk.Client dropping an
// embedded interface, which would break tasks declaring it.
func (s *BindingSuite) TestNamedClientInterfacesAreInjectable() {
	for name, typ := range map[string]reflect.Type{
		"Client":           reflect.TypeFor[sdk.Client](),
		"VariableClient":   reflect.TypeFor[sdk.VariableClient](),
		"ConnectionClient": reflect.TypeFor[sdk.ConnectionClient](),
		"XComClient":       reflect.TypeFor[sdk.XComClient](),
	} {
		s.True(isClient(typ), "sdk.%s must stay injectable", name)
	}
}

func (s *BindingSuite) TestResolveArityMismatch() {
	fn := func(country string) error { return nil }
	_, err := s.resolve(fn, nil, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
		s.Contains(err.Error(), "passes 0 positional argument(s)")
		s.Contains(err.Error(), "declares 1 data parameter(s)")
	}

	_, err = s.resolve(
		func() error { return nil },
		[]Arg{LiteralArg{Value: "uk"}},
		&fakeXComClient{},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "argument count mismatch")
	}
}

func (s *BindingSuite) TestResolveLiterals() {
	fn := func(country string, count int, ratio float64, on bool, tags []string, meta map[string]any) error {
		return nil
	}
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Value: "uk", ValueSchema: argSchema("string")},
		LiteralArg{Value: 3, ValueSchema: argSchema("integer")},
		LiteralArg{Value: 1.5, ValueSchema: argSchema("number")},
		LiteralArg{Value: true, ValueSchema: argSchema("boolean")},
		LiteralArg{Value: []any{"a", "b"}, ValueSchema: argSchema("array")},
		LiteralArg{Value: map[string]any{"k": "v"}, ValueSchema: argSchema("object")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("uk", got[0].Interface())
	s.Equal(3, got[1].Interface())
	s.Equal(1.5, got[2].Interface())
	s.Equal(true, got[3].Interface())
	s.Equal([]string{"a", "b"}, got[4].Interface())
	s.Equal(map[string]any{"k": "v"}, got[5].Interface())
}

func (s *BindingSuite) TestResolveInterleavedInjectables() {
	fn := func(log *slog.Logger, country string, ctx context.Context, meta map[string]any) error {
		return nil
	}
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Value: "uk", ValueSchema: argSchema("string")},
		LiteralArg{Value: map[string]any{"k": "v"}, ValueSchema: argSchema("object")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.NotNil(got[0].Interface().(*slog.Logger))
	s.Equal("uk", got[1].Interface())
	s.NotNil(got[2].Interface().(context.Context))
	s.Equal(map[string]any{"k": "v"}, got[3].Interface())
}

func (s *BindingSuite) TestCheckValueTypeMatrix() {
	unionType := &genmodels.ArgValueSchema{"type": []any{"string", "null"}}
	cases := map[string]struct {
		schema      *genmodels.ArgValueSchema
		target      reflect.Type
		errContains string
	}{
		"string-ok":         {argSchema("string"), reflect.TypeFor[string](), ""},
		"string-ptr-ok":     {argSchema("string"), reflect.TypeFor[*string](), ""},
		"string-vs-int":     {argSchema("string"), reflect.TypeFor[int](), "cannot bind"},
		"integer-ok":        {argSchema("integer"), reflect.TypeFor[int64](), ""},
		"integer-uint-ok":   {argSchema("integer"), reflect.TypeFor[uint32](), ""},
		"integer-vs-float":  {argSchema("integer"), reflect.TypeFor[float64](), "cannot bind"},
		"number-ok":         {argSchema("number"), reflect.TypeFor[float32](), ""},
		"number-vs-int":     {argSchema("number"), reflect.TypeFor[int](), "cannot bind"},
		"boolean-ok":        {argSchema("boolean"), reflect.TypeFor[bool](), ""},
		"boolean-vs-string": {argSchema("boolean"), reflect.TypeFor[string](), "cannot bind"},
		"object-map-ok":     {argSchema("object"), reflect.TypeFor[map[string]int](), ""},
		"object-struct-ok":  {argSchema("object"), reflect.TypeFor[struct{ A int }](), ""},
		"object-vs-slice":   {argSchema("object"), reflect.TypeFor[[]int](), "cannot bind"},
		"array-slice-ok":    {argSchema("array"), reflect.TypeFor[[]string](), ""},
		"array-array-ok":    {argSchema("array"), reflect.TypeFor[[2]int](), ""},
		"array-vs-map":      {argSchema("array"), reflect.TypeFor[map[string]any](), "cannot bind"},
		// A nil schema, a fragment without a plain-string "type" (a union or a
		// keyword-less fragment), an unrecognized type, or an `any` target all
		// skip the check -- the strict decode still guards the value.
		"nil-schema-skips":   {nil, reflect.TypeFor[chan int](), ""},
		"no-type-skips":      {&genmodels.ArgValueSchema{}, reflect.TypeFor[string](), ""},
		"union-type-skips":   {unionType, reflect.TypeFor[int](), ""},
		"unknown-type-skips": {argSchema("uuid"), reflect.TypeFor[string](), ""},
		"any-target-skips":   {argSchema("string"), reflect.TypeFor[any](), ""},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			err := checkValueType(tt.schema, tt.target)
			if tt.errContains == "" {
				s.NoError(err)
			} else if s.Assert().Error(err) {
				s.Contains(err.Error(), tt.errContains)
			}
		})
	}
}

func (s *BindingSuite) TestResolveTypeMismatchFailsLoudly() {
	fn := func(count int) error { return nil }
	_, err := s.resolve(
		fn,
		[]Arg{LiteralArg{Value: "uk", ValueSchema: argSchema("string")}},
		&fakeXComClient{},
	)
	if s.Assert().Error(err) {
		s.Contains(
			err.Error(),
			`the Dag declares JSON-schema type "string" which cannot bind to Go parameter type int`,
		)
	}
}

func (s *BindingSuite) TestResolveLiteralDecodeFailure() {
	// The Dag declared "any", so the type check passes but the JSON decode of a
	// string into an int must still fail loudly.
	fn := func(count int) error { return nil }
	_, err := s.resolve(fn, []Arg{LiteralArg{Value: "uk"}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "decoding literal value into int")
	}
}

type extractResult struct {
	GoVersion string `json:"go_version"`
	Timestamp int64  `json:"timestamp"`
}

// simpleInput is the minimal name-bound struct: one field, no tags, so it
// falls back to matching its own field name, verbatim.
type simpleInput struct {
	Name string
}

// twoFieldInput has one field the args always match (Name) and one whose
// arg name is never present in the tests that use it (Missing), to prove an
// unmatched field is left at its Go zero value instead of failing the task.
type twoFieldInput struct {
	Name    string
	Missing string `arg:"missing"`
}

// wholeConfig carries only `json:` tags, so it names no TaskFlow argument by
// field. As a flat data parameter it is decoded whole; as a sole struct
// parameter it is the whole-value fallback target.
type wholeConfig struct {
	Environment string `json:"environment"`
	Region      string `json:"region"`
}

// combineInput exercises both field-binding modes side by side: Name falls
// back to its verbatim field name, Count is explicitly named via its `arg:`
// tag.
type combineInput struct {
	Name  string
	Count int `arg:"count"`
}

// reportInput deliberately declares Ratio before Region, the reverse of the
// wire order those names appear in, to prove field declaration order is
// irrelevant to by-name claiming.
type reportInput struct {
	Ratio  float64
	Region string `arg:"region"`
}

func (s *BindingSuite) TestResolveXComArgs() {
	client := &fakeXComClient{values: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "timestamp": int64(42)},
		"probe/return_value":   "probe-value",
	}}

	fn := func(res extractResult, probe string) error { return nil }
	got, err := s.resolve(fn, []Arg{
		XComArg{TaskID: "extract", ValueSchema: argSchema("object")},
		XComArg{TaskID: "probe", ValueSchema: argSchema("string")},
	}, client)
	s.Require().NoError(err)
	s.Equal(extractResult{GoVersion: "go1.24", Timestamp: 42}, got[0].Interface())
	s.Equal("probe-value", got[1].Interface())

	s.Require().Len(client.calls, 2)
	taskIDs := make([]string, 0, 2)
	for _, call := range client.calls {
		// Pulls run concurrently, so assert per-call properties order-independently.
		taskIDs = append(taskIDs, call.taskID)
		s.Equal("dag1", call.dagID)
		s.Equal("run1", call.runID)
		s.Equal(
			api.XComReturnValueKey,
			call.key,
			"an XCom argument always pulls the return-value key",
		)
		s.Nil(call.mapIndex, "v1 always pulls the unmapped upstream instance")
	}
	s.ElementsMatch([]string{"extract", "probe"}, taskIDs)
}

func (s *BindingSuite) TestResolveXComStrictStructDecode() {
	client := &fakeXComClient{values: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "renamed_field": 1},
	}}
	fn := func(res extractResult) error { return nil }
	_, err := s.resolve(fn, []Arg{XComArg{TaskID: "extract"}}, client)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `decoding xcom from task "extract"`)
		s.Contains(err.Error(), "unknown field")
	}
}

func (s *BindingSuite) TestResolveXComPullFailure() {
	client := &fakeXComClient{err: sdk.XComNotFound}
	fn := func(res map[string]any) error { return nil }
	_, err := s.resolve(fn, []Arg{XComArg{TaskID: "extract"}}, client)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `pulling xcom from task "extract"`)
	}
}

func (s *BindingSuite) TestResolveXComWithoutWorkload() {
	plan := analyze(s, func(res map[string]any) error { return nil })
	_, err := plan.Resolve(
		context.Background(), slog.Default(), &fakeXComClient{},
		[]Arg{XComArg{TaskID: "extract"}},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "no workload in context")
	}
}

func (s *BindingSuite) TestResolveNullHandling() {
	fn := func(meta map[string]any) error { return nil }
	got, err := s.resolve(
		fn,
		[]Arg{LiteralArg{Value: nil, ValueSchema: argSchema("object")}},
		&fakeXComClient{},
	)
	s.Require().NoError(err)
	s.Nil(got[0].Interface())

	fnStr := func(country string) error { return nil }
	_, err = s.resolve(fnStr, []Arg{LiteralArg{Value: nil}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "not nilable")
	}
}

// fakeArg is an out-of-catalogue Arg variant: the compiler seals the sum type
// to this package, so the defensive default branch can only be reached from
// inside it.
type fakeArg struct{}

func (fakeArg) ArgName() string                   { return "fake" }
func (fakeArg) Schema() *genmodels.ArgValueSchema { return nil }
func (fakeArg) sealedArg()                        {}

func (s *BindingSuite) TestResolveUnsupportedVariant() {
	fn := func(country string) error { return nil }
	_, err := s.resolve(fn, []Arg{fakeArg{}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "unsupported argument binding binding.fakeArg")
	}
}

func (s *BindingSuite) TestResolveNilArg() {
	fn := func(country string) error { return nil }
	_, err := s.resolve(fn, []Arg{nil}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "nil argument binding")
	}
}

func (s *BindingSuite) TestResolveTIRunContextRebuild() {
	ti := sdk.TaskInstance{DagID: "dag1", RunID: "run1", TaskID: "transform"}
	dagRun := sdk.DagRun{DagID: "dag1", RunID: "run1"}
	ctx := context.WithValue(
		workloadCtx(),
		sdkcontext.RuntimeContextKey,
		sdk.NewTIRunContext(context.Background(), ti, dagRun),
	)

	plan := analyze(s, func(rc sdk.TIRunContext, country string) error { return nil })
	got, err := plan.Resolve(ctx, slog.Default(), &fakeXComClient{}, []Arg{
		LiteralArg{Value: "uk", ValueSchema: argSchema("string")},
	})
	s.Require().NoError(err)
	rc := got[0].Interface().(sdk.TIRunContext)
	s.Equal(ti, rc.TaskInstance())
	s.Equal(dagRun, rc.DagRun())
	s.Equal("uk", got[1].Interface())
}

func (s *BindingSuite) TestAnalyzeLoneStructClassification() {
	plan := analyze(s, func(input simpleInput) error { return nil })
	s.True(plan.loneStruct, "a sole struct data parameter is resolved by name at execution")
	s.Zero(plan.numData)

	ptrPlan := analyze(s, func(input *simpleInput) error { return nil })
	s.True(ptrPlan.loneStruct, "a pointer to a sole struct is detected the same way")
	s.Zero(ptrPlan.numData)

	flatPlan := analyze(s, func(prefix string, cfg wholeConfig) error { return nil })
	s.False(flatPlan.loneStruct, "a struct alongside another data parameter is a flat slot")
	s.Equal(2, flatPlan.numData)

	scalarPlan := analyze(s, func(name string) error { return nil })
	s.False(scalarPlan.loneStruct, "a sole non-struct data parameter is plain positional")
	s.Equal(1, scalarPlan.numData)
}

func (s *BindingSuite) TestAnalyzeMultipleStructsAreFlat() {
	// With no marker, the old "only one struct" / "cannot mix" restrictions are
	// gone: any struct that is not the sole data parameter is a flat whole-value
	// slot, so these signatures are now accepted rather than rejected.
	plan := analyze(s, func(a wholeConfig, b wholeConfig) error { return nil })
	s.False(plan.loneStruct)
	s.Equal(2, plan.numData)
}

func (s *BindingSuite) TestAnalyzeStructValidation() {
	type duplicateArgNames struct {
		A string
		B string `arg:"A"`
	}
	type nonDecodableField struct {
		Bad chan int
	}

	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"duplicate-arg-names": {
			func(input duplicateArgNames) error { return nil },
			`fields A and B both bind arg name "A"`,
		},
		"non-decodable-field": {
			func(input nonDecodableField) error { return nil },
			"cannot receive a task argument",
		},
		// A struct carrying `arg:` tags must be the sole data parameter, else its
		// tags would be silently ignored (the struct would be decoded whole).
		"tagged-struct-not-sole": {
			func(prefix string, input combineInput) error { return nil },
			"must be the function's only data parameter",
		},
		"tagged-struct-trailing": {
			func(input combineInput, suffix string) error { return nil },
			"must be the function's only data parameter",
		},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			_, err := Analyze(reflect.TypeOf(tt.fn), "testFn")
			if s.Assert().Error(err) {
				s.Assert().Contains(err.Error(), tt.errContains)
			}
		})
	}
}

func (s *BindingSuite) TestResolveStructAllFields() {
	fn := func(input combineInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
		LiteralArg{Name: "count", Value: 7, ValueSchema: argSchema("integer")},
	}, &fakeXComClient{})
	s.Require().NoError(err)

	input := got[0].Interface().(combineInput)
	s.Equal("widget", input.Name, "the untagged field claims its verbatim field name")
	s.Equal(7, input.Count, "the `arg:` tag claims its named entry")
}

func (s *BindingSuite) TestResolveStructXComArg() {
	fn := func(log *slog.Logger, input reportInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		XComArg{Name: "region", TaskID: "make_region", ValueSchema: argSchema("string")},
		LiteralArg{Name: "Ratio", Value: 0.5, ValueSchema: argSchema("number")},
	}, &fakeXComClient{values: map[string]any{"make_region/return_value": "east"}})
	s.Require().NoError(err)

	input := got[1].Interface().(reportInput)
	s.Equal("east", input.Region, "Region resolves by name despite being declared after Ratio")
	s.Equal(0.5, input.Ratio)
}

func (s *BindingSuite) TestResolveStructSingleClaimedArgBindsByName() {
	// A single argument whose name a field claims binds by name -- the tie-break
	// that keeps a one-field struct name-bound rather than whole-value decoded.
	fn := func(input simpleInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("widget", got[0].Interface().(simpleInput).Name)
}

func (s *BindingSuite) TestResolveStructPointer() {
	fn := func(input *simpleInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(*simpleInput)
	s.Require().NotNil(input)
	s.Equal("widget", input.Name)
}

func (s *BindingSuite) TestResolveLoneStructBothModes() {
	// The same one-struct signature resolves either way, chosen per execution
	// from the argument spec: field-by-field when the argument names match the
	// struct's fields, or whole from a single argument no field claims.
	fn := func(cfg wholeConfig) error { return nil }
	want := wholeConfig{Environment: "production", Region: "eu-west-1"}

	// Struct-based: two arguments named like the fields bind by name.
	named, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Environment", Value: "production", ValueSchema: argSchema("string")},
		LiteralArg{Name: "Region", Value: "eu-west-1", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(want, named[0].Interface(), "argument names matching the fields bind field-by-field")

	// Flat-based: one argument no field claims decodes whole into the struct.
	whole, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "cfg",
			Value:       map[string]any{"environment": "production", "region": "eu-west-1"},
			ValueSchema: argSchema("object"),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(want, whole[0].Interface(), "a single unclaimed argument decodes whole into the struct")
}

func (s *BindingSuite) TestResolveStructUnclaimedArgFailsLoudly() {
	fn := func(input combineInput) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
		LiteralArg{Name: "typo", Value: "x", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	// Name is claimed, so this is name-binding (not the whole-value fallback);
	// the leftover "typo" argument fails the task rather than being dropped.
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `not claimed by any struct field: "typo"`)
	}
}

func (s *BindingSuite) TestResolveStructUnclaimedFromDefaultAllowed() {
	fn := func(input combineInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
		// The Dag author never passed "threshold"; Python captured it from the
		// stub signature's default. The struct need not mirror it.
		LiteralArg{
			Name:        "threshold",
			Value:       0.75,
			ValueSchema: argSchema("number"),
			FromDefault: true,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("widget", got[0].Interface().(combineInput).Name)
}

func (s *BindingSuite) TestResolveStructEmptySpecFailsLoudly() {
	fn := func(input simpleInput) error { return nil }
	for name, args := range map[string][]Arg{"nil-spec": nil, "empty-spec": {}} {
		s.Run(name, func() {
			_, err := s.resolve(fn, args, &fakeXComClient{})
			// The Edge Worker path delivers no arg bindings; a struct with
			// bindable fields must fail rather than run fully zero-valued.
			if s.Assert().Error(err) {
				s.Contains(err.Error(), "no TaskFlow arg bindings arrived")
			}
		})
	}
}

func (s *BindingSuite) TestResolveStructOnlyDefaultsZeroValues() {
	// A lone from_default argument that no field claims is neither whole-value
	// decoded (defaults were never explicitly passed) nor an error; the fields
	// keep their kwarg-style zero values.
	fn := func(input twoFieldInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "threshold",
			Value:       0.75,
			ValueSchema: argSchema("number"),
			FromDefault: true,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(twoFieldInput)
	s.Equal("", input.Name, "no explicit entry arrived; fields keep kwarg-style zero values")
	s.Equal("", input.Missing)
}

func (s *BindingSuite) TestResolveStructUnmatchedFieldZeroValued() {
	fn := func(input twoFieldInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(twoFieldInput)
	s.Equal("widget", input.Name, "the matched field binds normally")
	s.Equal("", input.Missing, "the unmatched field is left at its Go zero value, not an error")
}
