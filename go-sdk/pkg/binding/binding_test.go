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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type BindingSuite struct {
	suite.Suite
}

func TestBindingSuite(t *testing.T) {
	suite.Run(t, &BindingSuite{})
}

// fakeXComClient records GetXCom calls and returns preconfigured values.
type fakeXComClient struct {
	sdk.Client

	values map[string]any // "<task_id>/<key>" -> raw value
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
	f.calls = append(f.calls, fakeXComCall{dagID, runID, taskID, key, mapIndex})
	if f.err != nil {
		return nil, f.err
	}
	return f.values[taskID+"/"+key], nil
}

// workloadCtx returns a context carrying an ExecuteTaskWorkload the resolver
// reads the dag/run identifiers from.
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
	s.Equal(2, plan.NumData())

	s.Zero(analyze(s, func() error { return nil }).NumData())
	s.Equal(
		1,
		analyze(s, func(x any) error { return nil }).NumData(),
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
		LiteralArg{Value: "uk", DataType: DataTypeString},
		LiteralArg{Value: 3, DataType: DataTypeInteger},
		LiteralArg{Value: 1.5, DataType: DataTypeNumber},
		LiteralArg{Value: true, DataType: DataTypeBoolean},
		LiteralArg{Value: []any{"a", "b"}, DataType: DataTypeArray},
		LiteralArg{Value: map[string]any{"k": "v"}, DataType: DataTypeObject},
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
		LiteralArg{Value: "uk", DataType: DataTypeString},
		LiteralArg{Value: map[string]any{"k": "v"}, DataType: DataTypeObject},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.NotNil(got[0].Interface().(*slog.Logger))
	s.Equal("uk", got[1].Interface())
	s.NotNil(got[2].Interface().(context.Context))
	s.Equal(map[string]any{"k": "v"}, got[3].Interface())
}

func (s *BindingSuite) TestCheckDataTypeMatrix() {
	cases := map[string]struct {
		dt          DataType
		target      reflect.Type
		errContains string
	}{
		"string-ok":         {DataTypeString, reflect.TypeFor[string](), ""},
		"string-ptr-ok":     {DataTypeString, reflect.TypeFor[*string](), ""},
		"string-vs-int":     {DataTypeString, reflect.TypeFor[int](), "cannot bind"},
		"integer-ok":        {DataTypeInteger, reflect.TypeFor[int64](), ""},
		"integer-uint-ok":   {DataTypeInteger, reflect.TypeFor[uint32](), ""},
		"integer-vs-float":  {DataTypeInteger, reflect.TypeFor[float64](), "cannot bind"},
		"number-ok":         {DataTypeNumber, reflect.TypeFor[float32](), ""},
		"number-vs-int":     {DataTypeNumber, reflect.TypeFor[int](), "cannot bind"},
		"boolean-ok":        {DataTypeBoolean, reflect.TypeFor[bool](), ""},
		"boolean-vs-string": {DataTypeBoolean, reflect.TypeFor[string](), "cannot bind"},
		"object-map-ok":     {DataTypeObject, reflect.TypeFor[map[string]int](), ""},
		"object-struct-ok":  {DataTypeObject, reflect.TypeFor[struct{ A int }](), ""},
		"object-vs-slice":   {DataTypeObject, reflect.TypeFor[[]int](), "cannot bind"},
		"array-slice-ok":    {DataTypeArray, reflect.TypeFor[[]string](), ""},
		"array-array-ok":    {DataTypeArray, reflect.TypeFor[[2]int](), ""},
		"array-vs-map":      {DataTypeArray, reflect.TypeFor[map[string]any](), "cannot bind"},
		"any-skips":         {DataTypeAny, reflect.TypeFor[chan int](), ""},
		"empty-skips":       {DataType(""), reflect.TypeFor[string](), ""},
		"any-target-skips":  {DataTypeString, reflect.TypeFor[any](), ""},
		"unknown-dt":        {DataType("uuid"), reflect.TypeFor[string](), "unknown declared type"},
	}
	for name, tt := range cases {
		s.Run(name, func() {
			err := checkDataType(tt.dt, tt.target)
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
		[]Arg{LiteralArg{Value: "uk", DataType: DataTypeString}},
		&fakeXComClient{},
	)
	if s.Assert().Error(err) {
		s.Contains(
			err.Error(),
			`the Dag declares type "string" which cannot bind to Go parameter type int`,
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

// simpleTaskInput is the minimal TaskInput struct: one field, no tags, so it
// falls back to matching its own field name, verbatim.
type simpleTaskInput struct {
	sdk.TaskInput
	Name string
}

// twoFieldTaskInput has one field the args always match (Name) and one whose
// arg name is never present in the tests that use it (Missing), to prove an
// unmatched field is left at its Go zero value instead of failing the task.
type twoFieldTaskInput struct {
	sdk.TaskInput
	Name    string
	Missing string `arg:"missing"`
}

// nonEmbeddingStruct has no sdk.TaskInput sentinel, so it must keep resolving
// as today's whole-value decode target, not per-field TaskInput binding.
type nonEmbeddingStruct struct {
	Name string
}

// combineInput exercises both TaskInput field-binding modes side by side:
// Name falls back to its verbatim field name, Count is explicitly named
// via its `arg:` tag.
type combineInput struct {
	sdk.TaskInput
	Name  string
	Count int `arg:"count"`
}

// reportInput deliberately declares Ratio before Region, the reverse of the
// wire order those names appear in, to prove field declaration order is
// irrelevant to by-name claiming.
type reportInput struct {
	sdk.TaskInput
	Ratio  float64
	Region string `arg:"region"`
}

// mixedInput pairs a TaskInput struct with a plain data parameter in the
// functions that assert Analyze rejects that combination.
type mixedInput struct {
	sdk.TaskInput
	Name string
}

func (s *BindingSuite) TestResolveXComArgs() {
	client := &fakeXComClient{values: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "timestamp": int64(42)},
		"probe/return_value":   "probe-value",
	}}

	fn := func(res extractResult, probe string) error { return nil }
	got, err := s.resolve(fn, []Arg{
		XComArg{TaskID: "extract", DataType: DataTypeObject},
		XComArg{TaskID: "probe", DataType: DataTypeString},
	}, client)
	s.Require().NoError(err)
	s.Equal(extractResult{GoVersion: "go1.24", Timestamp: 42}, got[0].Interface())
	s.Equal("probe-value", got[1].Interface())

	s.Require().Len(client.calls, 2)
	s.Equal("dag1", client.calls[0].dagID)
	s.Equal("run1", client.calls[0].runID)
	s.Equal("extract", client.calls[0].taskID)
	s.Equal(
		api.XComReturnValueKey,
		client.calls[0].key,
		"an XCom argument always pulls the return-value key",
	)
	s.Nil(client.calls[0].mapIndex, "v1 always pulls the unmapped upstream instance")
	s.Equal(api.XComReturnValueKey, client.calls[1].key)
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
		[]Arg{LiteralArg{Value: nil, DataType: DataTypeObject}},
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

func (fakeArg) ArgName() string        { return "fake" }
func (fakeArg) DeclaredType() DataType { return DataTypeAny }
func (fakeArg) sealedArg()             {}

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
		LiteralArg{Value: "uk", DataType: DataTypeString},
	})
	s.Require().NoError(err)
	rc := got[0].Interface().(sdk.TIRunContext)
	s.Equal(ti, rc.TaskInstance())
	s.Equal(dagRun, rc.DagRun())
	s.Equal("uk", got[1].Interface())
}

func (s *BindingSuite) TestAnalyzeTaskInputClassification() {
	plan := analyze(s, func(input simpleTaskInput) error { return nil })
	s.Zero(plan.NumData(), "a TaskInput struct claims by name, not by position")

	ptrPlan := analyze(s, func(input *simpleTaskInput) error { return nil })
	s.Zero(ptrPlan.NumData(), "a pointer to a TaskInput struct is detected the same way")

	plainPlan := analyze(s, func(cfg nonEmbeddingStruct) error { return nil })
	s.Equal(
		1, plainPlan.NumData(),
		"a plain struct without the TaskInput sentinel stays a whole-value data parameter",
	)
}

func (s *BindingSuite) TestAnalyzeTaskInputValidation() {
	type duplicateArgNames struct {
		sdk.TaskInput
		A string
		B string `arg:"A"`
	}
	type nonDecodableField struct {
		sdk.TaskInput
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
		"two-taskinput-params": {
			func(a simpleTaskInput, b simpleTaskInput) error { return nil },
			"only one TaskInput struct parameter is allowed",
		},
		"mixed-with-flat-data-param": {
			func(prefix string, input mixedInput) error { return nil },
			"cannot mix a TaskInput struct parameter",
		},
		"mixed-with-trailing-flat-data-param": {
			func(input mixedInput, suffix string) error { return nil },
			"cannot mix a TaskInput struct parameter",
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

func (s *BindingSuite) TestResolveTaskInputAllStruct() {
	fn := func(input combineInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", DataType: DataTypeString},
		LiteralArg{Name: "count", Value: 7, DataType: DataTypeInteger},
	}, &fakeXComClient{})
	s.Require().NoError(err)

	input := got[0].Interface().(combineInput)
	s.Equal("widget", input.Name, "the untagged field claims its verbatim field name")
	s.Equal(7, input.Count, "the `arg:` tag claims its named entry")
}

func (s *BindingSuite) TestResolveTaskInputXComArg() {
	fn := func(log *slog.Logger, input reportInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		XComArg{Name: "region", TaskID: "make_region", DataType: DataTypeString},
		LiteralArg{Name: "Ratio", Value: 0.5, DataType: DataTypeNumber},
	}, &fakeXComClient{values: map[string]any{"make_region/return_value": "east"}})
	s.Require().NoError(err)

	input := got[1].Interface().(reportInput)
	s.Equal("east", input.Region, "Region resolves by name despite being declared after Ratio")
	s.Equal(0.5, input.Ratio)
}

func (s *BindingSuite) TestResolveTaskInputLiteralThroughArgName() {
	fn := func(input simpleTaskInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", DataType: DataTypeString},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("widget", got[0].Interface().(simpleTaskInput).Name)
}

func (s *BindingSuite) TestResolveTaskInputPointerStruct() {
	fn := func(input *simpleTaskInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", DataType: DataTypeString},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(*simpleTaskInput)
	s.Require().NotNil(input)
	s.Equal("widget", input.Name)
}

func (s *BindingSuite) TestResolveTaskInputUnclaimedArgFailsLoudly() {
	fn := func(input simpleTaskInput) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "different_name", Value: "x", DataType: DataTypeString},
	}, &fakeXComClient{})
	// No TaskInput field claims "different_name"; the leftover argument fails
	// the task rather than being dropped silently.
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `not claimed by any TaskInput field: "different_name"`)
	}
}

func (s *BindingSuite) TestResolveTaskInputUnmatchedArgNameZeroValuedAlongsideMatch() {
	fn := func(input twoFieldTaskInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", DataType: DataTypeString},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(twoFieldTaskInput)
	s.Equal("widget", input.Name, "the matched field binds normally")
	s.Equal("", input.Missing, "the unmatched field is left at its Go zero value, not an error")
}
