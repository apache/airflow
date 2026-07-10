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
		[]Arg{{Kind: ArgKindLiteral, Value: "uk"}},
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
		{Kind: ArgKindLiteral, Value: "uk", DataType: DataTypeString},
		{Kind: ArgKindLiteral, Value: 3, DataType: DataTypeInteger},
		{Kind: ArgKindLiteral, Value: 1.5, DataType: DataTypeNumber},
		{Kind: ArgKindLiteral, Value: true, DataType: DataTypeBoolean},
		{Kind: ArgKindLiteral, Value: []any{"a", "b"}, DataType: DataTypeArray},
		{Kind: ArgKindLiteral, Value: map[string]any{"k": "v"}, DataType: DataTypeObject},
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
		{Kind: ArgKindLiteral, Value: "uk", DataType: DataTypeString},
		{Kind: ArgKindLiteral, Value: map[string]any{"k": "v"}, DataType: DataTypeObject},
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
		[]Arg{{Kind: ArgKindLiteral, Value: "uk", DataType: DataTypeString}},
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
	_, err := s.resolve(fn, []Arg{{Kind: ArgKindLiteral, Value: "uk"}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "decoding literal value into int")
	}
}

type extractResult struct {
	GoVersion string `json:"go_version"`
	Timestamp int64  `json:"timestamp"`
}

func (s *BindingSuite) TestResolveXComArgs() {
	client := &fakeXComClient{values: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "timestamp": int64(42)},
		"extract/part":         "part-value",
	}}

	fn := func(res extractResult, part string) error { return nil }
	got, err := s.resolve(fn, []Arg{
		{Kind: ArgKindXCom, TaskID: "extract", DataType: DataTypeObject},
		{Kind: ArgKindXCom, TaskID: "extract", Key: "part", DataType: DataTypeString},
	}, client)
	s.Require().NoError(err)
	s.Equal(extractResult{GoVersion: "go1.24", Timestamp: 42}, got[0].Interface())
	s.Equal("part-value", got[1].Interface())

	s.Require().Len(client.calls, 2)
	s.Equal("dag1", client.calls[0].dagID)
	s.Equal("run1", client.calls[0].runID)
	s.Equal("extract", client.calls[0].taskID)
	s.Equal(
		api.XComReturnValueKey,
		client.calls[0].key,
		"an empty key must default to the return-value key",
	)
	s.Nil(client.calls[0].mapIndex, "v1 always pulls the unmapped upstream instance")
	s.Equal("part", client.calls[1].key)
}

func (s *BindingSuite) TestResolveXComStrictStructDecode() {
	client := &fakeXComClient{values: map[string]any{
		"extract/return_value": map[string]any{"go_version": "go1.24", "renamed_field": 1},
	}}
	fn := func(res extractResult) error { return nil }
	_, err := s.resolve(fn, []Arg{{Kind: ArgKindXCom, TaskID: "extract"}}, client)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `decoding xcom from task "extract"`)
		s.Contains(err.Error(), "unknown field")
	}
}

func (s *BindingSuite) TestResolveXComPullFailure() {
	client := &fakeXComClient{err: sdk.XComNotFound}
	fn := func(res map[string]any) error { return nil }
	_, err := s.resolve(fn, []Arg{{Kind: ArgKindXCom, TaskID: "extract"}}, client)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `pulling xcom from task "extract"`)
	}
}

func (s *BindingSuite) TestResolveXComWithoutWorkload() {
	plan := analyze(s, func(res map[string]any) error { return nil })
	_, err := plan.Resolve(
		context.Background(), slog.Default(), &fakeXComClient{},
		[]Arg{{Kind: ArgKindXCom, TaskID: "extract"}},
	)
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "no workload in context")
	}
}

func (s *BindingSuite) TestResolveNullHandling() {
	fn := func(meta map[string]any) error { return nil }
	got, err := s.resolve(
		fn,
		[]Arg{{Kind: ArgKindLiteral, Value: nil, DataType: DataTypeObject}},
		&fakeXComClient{},
	)
	s.Require().NoError(err)
	s.Nil(got[0].Interface())

	fnStr := func(country string) error { return nil }
	_, err = s.resolve(fnStr, []Arg{{Kind: ArgKindLiteral, Value: nil}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "not nilable")
	}
}

func (s *BindingSuite) TestResolveUnknownKind() {
	fn := func(country string) error { return nil }
	_, err := s.resolve(fn, []Arg{{Kind: ArgKind("template"), Value: "x"}}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), `unknown argument kind "template"`)
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
		{Kind: ArgKindLiteral, Value: "uk", DataType: DataTypeString},
	})
	s.Require().NoError(err)
	rc := got[0].Interface().(sdk.TIRunContext)
	s.Equal(ti, rc.TaskInstance())
	s.Equal(dagRun, rc.DagRun())
	s.Equal("uk", got[1].Interface())
}
