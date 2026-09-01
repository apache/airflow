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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

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

func argSchema(jsonType string) *genmodels.ArgValueSchema {
	s := genmodels.ArgValueSchema{"type": jsonType}
	return &s
}

func anyOfSchema(branches ...map[string]any) *genmodels.ArgValueSchema {
	alternatives := make([]any, len(branches))
	for i, b := range branches {
		alternatives[i] = b
	}
	s := genmodels.ArgValueSchema{"anyOf": alternatives}
	return &s
}

// fakeXComClient records concurrent GetXCom calls.
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

func runtimeCtx() context.Context {
	ti := sdk.TaskInstance{
		DagID:  "dag1",
		RunID:  "run1",
		TaskID: "transform",
	}
	return context.WithValue(
		context.Background(),
		sdkcontext.RuntimeContextKey,
		sdk.NewTIRunContext(context.Background(), ti, sdk.DagRun{DagID: ti.DagID, RunID: ti.RunID}),
	)
}

func analyze(s *BindingSuite, fn any) *Plan {
	plan, err := Analyze(reflect.TypeOf(fn), "testFn")
	s.Require().NoError(err)
	return plan
}

func (s *BindingSuite) resolve(fn any, args []Arg, client sdk.Client) ([]reflect.Value, error) {
	plan := analyze(s, fn)
	return plan.Resolve(runtimeCtx(), slog.Default(), client, args)
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
		"pointer-to-func-param": {
			func(cb *func()) error { return nil },
			"cannot receive a task argument",
		},
		"slice-of-func-param": {
			func(cbs []func()) error { return nil },
			"cannot receive a task argument",
		},
		"map-with-chan-value-param": {
			func(m map[string]chan int) error { return nil },
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

// selfDecodingNode must bypass recursive field inspection.
type selfDecodingNode struct {
	Cb   func()
	Next *selfDecodingNode
}

func (n *selfDecodingNode) UnmarshalJSON([]byte) error { return nil }

type recursiveNode struct {
	Name string
	Next *recursiveNode
}

func (s *BindingSuite) TestAnalyzeAcceptsSelfDecodingAndRecursiveTypes() {
	for name, fn := range map[string]any{
		"time.Time":          func(when time.Time) error { return nil },
		"slice-of-time":      func(when []time.Time) error { return nil },
		"self-decoding":      func(name string, n selfDecodingNode) error { return nil },
		"self-decoding-sole": func(n selfDecodingNode) error { return nil },
		"recursive-struct":   func(n recursiveNode) error { return nil },
	} {
		s.Run(name, func() {
			_, err := Analyze(reflect.TypeOf(fn), "testFn")
			s.Assert().NoError(err)
		})
	}
}

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

func (s *BindingSuite) TestResolveSelfDecodingLiterals() {
	fn := func(when time.Time, id uuid.UUID, ratio float64) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Value:       "2024-01-02T03:04:05Z",
			ValueSchema: &genmodels.ArgValueSchema{"type": "string", "format": "date-time"},
		},
		LiteralArg{
			Value:       "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			ValueSchema: &genmodels.ArgValueSchema{"type": "string", "format": "uuid"},
		},
		LiteralArg{Value: 3, ValueSchema: argSchema("integer")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), got[0].Interface())
	s.Equal(uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), got[1].Interface())
	s.Equal(3.0, got[2].Interface())
}

func (s *BindingSuite) TestResolveTypedMapParam() {
	fn := func(labels map[string]string) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "labels",
			Value:       map[string]any{"team": "data", "tier": "gold"},
			ValueSchema: argSchema("object"),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(map[string]string{"team": "data", "tier": "gold"}, got[0].Interface())
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
		"integer-float-ok":  {argSchema("integer"), reflect.TypeFor[float64](), ""},
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
		// Schemas without a usable type are unconstrained.
		"nil-schema-skips":   {nil, reflect.TypeFor[chan int](), ""},
		"no-type-skips":      {&genmodels.ArgValueSchema{}, reflect.TypeFor[string](), ""},
		"union-type-skips":   {unionType, reflect.TypeFor[int](), ""},
		"unknown-type-skips": {argSchema("uuid"), reflect.TypeFor[string](), ""},
		"any-target-skips":   {argSchema("string"), reflect.TypeFor[any](), ""},

		// Pydantic encodes bytes as a string.
		"binary-to-string": {
			&genmodels.ArgValueSchema{"type": "string", "format": "binary"},
			reflect.TypeFor[string](), "",
		},
		"binary-vs-bytes": {
			&genmodels.ArgValueSchema{"type": "string", "format": "binary"},
			reflect.TypeFor[[]byte](), "cannot bind",
		},

		// Unions match any branch but preserve nullability.
		"anyof-matching-branch": {
			anyOfSchema(map[string]any{"type": "integer"}, map[string]any{"type": "string"}),
			reflect.TypeFor[string](), "",
		},
		"anyof-no-branch": {
			anyOfSchema(map[string]any{"type": "integer"}, map[string]any{"type": "string"}),
			reflect.TypeFor[bool](), "cannot bind",
		},
		"anyof-nullable-ptr-ok": {
			anyOfSchema(
				map[string]any{"type": "string"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[*string](), "",
		},
		"anyof-nullable-needs-pointer": {
			anyOfSchema(
				map[string]any{"type": "string"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[string](), "must be a pointer",
		},
		"anyof-nullable-wrong-type": {
			anyOfSchema(
				map[string]any{"type": "string"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[*int64](), "cannot bind",
		},
		"anyof-unreadable-branch-skips": {
			anyOfSchema(map[string]any{"type": "string"}, map[string]any{"$ref": "#/x"}),
			reflect.TypeFor[bool](), "",
		},
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

type simpleInput struct {
	Name string
}

type twoFieldInput struct {
	Name    string
	Missing string `arg:"missing"`
}

type wholeConfig struct {
	Environment string `json:"environment"`
	Region      string `json:"region"`
}

type combineInput struct {
	Name  string
	Count int `arg:"count"`
}

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
		// Pull order is nondeterministic.
		taskIDs = append(taskIDs, call.taskID)
		s.Equal("dag1", call.dagID)
		s.Equal("run1", call.runID)
		s.Equal(
			sdk.XComReturnValueKey,
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

func (s *BindingSuite) TestResolveMultipleXComPullFailures() {
	client := &fakeXComClient{err: sdk.XComNotFound}
	fn := func(a map[string]any, b map[string]any, c map[string]any) error { return nil }
	_, err := s.resolve(fn, []Arg{
		XComArg{TaskID: "extract_a"},
		XComArg{TaskID: "extract_b"},
		XComArg{TaskID: "extract_c"},
	}, client)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `pulling xcom from task "extract_a"`)
		s.Contains(err.Error(), `pulling xcom from task "extract_b"`)
		s.Contains(err.Error(), `pulling xcom from task "extract_c"`)
	}
}

func (s *BindingSuite) TestResolveWholeStructFromXCom() {
	client := &fakeXComClient{values: map[string]any{
		"make_config/return_value": map[string]any{
			"environment": "production",
			"region":      "eu-west-1",
		},
	}}
	fn := func(cfg wholeConfig) error { return nil }
	got, err := s.resolve(fn, []Arg{
		XComArg{Name: "cfg", TaskID: "make_config", ValueSchema: argSchema("object")},
	}, client)
	s.Require().NoError(err)
	s.Equal(
		wholeConfig{Environment: "production", Region: "eu-west-1"},
		got[0].Interface(),
		"an XCom argument no field claims decodes whole into the struct",
	)
}

func (s *BindingSuite) TestResolveXComWithoutRuntimeContext() {
	plan := analyze(s, func(res map[string]any) error { return nil })
	_, err := plan.Resolve(
		context.Background(), slog.Default(), &fakeXComClient{},
		[]Arg{XComArg{TaskID: "extract"}},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "no task runtime context")
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

// fakeArg exercises the defensive branch of the sealed interface.
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
		runtimeCtx(),
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
	plan := analyze(s, func(a wholeConfig, b wholeConfig) error { return nil })
	s.False(plan.loneStruct)
	s.Equal(2, plan.numData)
}

func (s *BindingSuite) TestAnalyzeStructValidation() {
	type duplicateArgNames struct {
		A string
		B string `arg:"A"`
	}
	type taggedNonDecodableField struct {
		Bad chan int `arg:"bad"`
	}
	type foldedDuplicateArgNames struct {
		RegionCode  string
		Region_code string
	}

	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"duplicate-arg-names": {
			func(input duplicateArgNames) error { return nil },
			`fields A and B both bind arg name "A"`,
		},
		"folded-duplicate-arg-names": {
			func(input foldedDuplicateArgNames) error { return nil },
			"differ only in case or underscores",
		},
		"tagged-non-decodable-field": {
			func(input taggedNonDecodableField) error { return nil },
			"cannot receive a task argument",
		},
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
	fn := func(cfg wholeConfig) error { return nil }
	want := wholeConfig{Environment: "production", Region: "eu-west-1"}

	named, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Environment", Value: "production", ValueSchema: argSchema("string")},
		LiteralArg{Name: "Region", Value: "eu-west-1", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(want, named[0].Interface(), "argument names matching the fields bind field-by-field")

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

type taggedRegionInput struct {
	Region string `arg:"regon_code"` // deliberate typo
}

func (s *BindingSuite) TestResolveTaggedStructNeverFallsBackToWholeValue() {
	fn := func(input taggedRegionInput) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "region_code", Value: "eu-west-1", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `not claimed by any struct field: "region_code"`)
	}
}

func (s *BindingSuite) TestResolveStructUnclaimedArgFailsLoudly() {
	fn := func(input combineInput) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
		LiteralArg{Name: "typo", Value: "x", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `not claimed by any struct field: "typo"`)
	}
}

func (s *BindingSuite) TestResolveStructUnclaimedFromDefaultAllowed() {
	fn := func(input combineInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "Name", Value: "widget", ValueSchema: argSchema("string")},
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
			if s.Assert().Error(err) {
				s.Contains(err.Error(), "no TaskFlow arg bindings arrived")
			}
		})
	}
}

func (s *BindingSuite) TestResolveStructOnlyDefaultsZeroValues() {
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

func (s *BindingSuite) TestResolveFlatParamsToleratesCapturedDefaults() {
	fn := func(country string) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "country", Value: "uk", ValueSchema: argSchema("string")},
		LiteralArg{
			Name:        "verbose",
			Value:       false,
			ValueSchema: argSchema("boolean"),
			FromDefault: true,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("uk", got[0].Interface())
}

func (s *BindingSuite) TestResolveFlatParamsBindsDefaultsWhenDeclared() {
	fn := func(country string, verbose bool) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "country", Value: "uk", ValueSchema: argSchema("string")},
		LiteralArg{
			Name:        "verbose",
			Value:       true,
			ValueSchema: argSchema("boolean"),
			FromDefault: true,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal("uk", got[0].Interface())
	s.Equal(true, got[1].Interface())
}

func (s *BindingSuite) TestResolveWholeStructIgnoresCapturedDefaults() {
	fn := func(config wholeConfig) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "config",
			Value:       map[string]any{"environment": "prod", "region": "eu-west-1"},
			ValueSchema: argSchema("object"),
		},
		LiteralArg{
			Name:        "verbose",
			Value:       false,
			ValueSchema: argSchema("boolean"),
			FromDefault: true,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(wholeConfig{Environment: "prod", Region: "eu-west-1"}, got[0].Interface())
}

type snakeCaseInput struct {
	RegionCode string
	Threshold  float64
}

func (s *BindingSuite) TestResolveUntaggedFieldsBindSnakeCaseArguments() {
	fn := func(input snakeCaseInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "region_code", Value: "eu-west-1", ValueSchema: argSchema("string")},
		LiteralArg{Name: "threshold", Value: 0.75, ValueSchema: argSchema("number")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(snakeCaseInput{RegionCode: "eu-west-1", Threshold: 0.75}, got[0].Interface())
}

type embeddedCommon struct {
	Region string `arg:"region"`
}

type embeddedInput struct {
	embeddedCommon
	Threshold float64
}

func (s *BindingSuite) TestResolveEmbeddedStructFields() {
	fn := func(input embeddedInput) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "region", Value: "eu-west-1", ValueSchema: argSchema("string")},
		LiteralArg{Name: "threshold", Value: 0.75, ValueSchema: argSchema("number")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	input := got[0].Interface().(embeddedInput)
	s.Equal("eu-west-1", input.Region)
	s.Equal(0.75, input.Threshold)
}

type money struct {
	Amount string
}

func (m *money) UnmarshalJSON([]byte) error { m.Amount = "decoded"; return nil }

func (s *BindingSuite) TestResolveSelfDecodingTypeAgainstStringSchema() {
	fn := func(price money) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "price", Value: "12.34", ValueSchema: argSchema("string")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(money{Amount: "decoded"}, got[0].Interface())
}

type callbackConfig struct {
	Name string `json:"name"`
	Cb   func() `json:"-"`
}

func (s *BindingSuite) TestResolveStructWithNonBindableField() {
	fn := func(config callbackConfig) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "config",
			Value:       map[string]any{"name": "widget"},
			ValueSchema: argSchema("object"),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	config := got[0].Interface().(callbackConfig)
	s.Equal("widget", config.Name)
	s.Nil(config.Cb)
}

func (s *BindingSuite) TestResolveEmptyInterfaceDataParam() {
	fn := func(payload any) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "payload",
			Value:       map[string]any{"k": "v"},
			ValueSchema: argSchema("object"),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(map[string]any{"k": "v"}, got[0].Interface())
}
