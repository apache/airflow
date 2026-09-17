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

func temporalSchema(format string) *genmodels.ArgValueSchema {
	s := genmodels.ArgValueSchema{"type": "string", "format": format}
	return &s
}

func arraySchema(items map[string]any) *genmodels.ArgValueSchema {
	s := genmodels.ArgValueSchema{"type": "array", "items": items}
	return &s
}

func objectSchema(values map[string]any) *genmodels.ArgValueSchema {
	s := genmodels.ArgValueSchema{"type": "object", "additionalProperties": values}
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

func (s *BindingSuite) TestResolveTemporalLiterals() {
	fn := func(
		aware time.Time, naive time.Time, day time.Time, clock time.Time, window time.Duration,
	) error {
		return nil
	}
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Value: "2024-01-02T03:04:05Z", ValueSchema: temporalSchema("date-time")},
		LiteralArg{Value: "2024-01-02T03:04:05", ValueSchema: temporalSchema("date-time")},
		LiteralArg{Value: "2024-01-02", ValueSchema: temporalSchema("date")},
		LiteralArg{Value: "03:04:05", ValueSchema: temporalSchema("time")},
		LiteralArg{Value: "P1DT2H3M4S", ValueSchema: temporalSchema("duration")},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), got[0].Interface())
	s.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), got[1].Interface(),
		"an offset-less timestamp is read as UTC")
	s.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), got[2].Interface())
	s.Equal(time.Date(0, 1, 1, 3, 4, 5, 0, time.UTC), got[3].Interface())
	s.Equal(26*time.Hour+3*time.Minute+4*time.Second, got[4].Interface())
}

func (s *BindingSuite) TestSoleTemporalParamStaysFlat() {
	for name, fn := range map[string]any{
		"time.Time":  func(when time.Time) error { return nil },
		"*time.Time": func(when *time.Time) error { return nil },
	} {
		s.Run(name, func() {
			plan := analyze(s, fn)
			s.False(plan.loneStruct, "a sole timestamp parameter must bind positionally")
			s.Equal(1, plan.numData)
		})
	}

	got, err := s.resolve(
		func(when time.Time) error { return nil },
		[]Arg{LiteralArg{Value: "2024-01-02T03:04:05Z", ValueSchema: temporalSchema("date-time")}},
		&fakeXComClient{},
	)
	s.Require().NoError(err)
	s.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), got[0].Interface())
}

func (s *BindingSuite) TestResolveNestedTemporalContainers() {
	fn := func(
		windows []time.Duration,
		days []time.Time,
		stamps []time.Time,
		byName map[string]time.Duration,
	) error {
		return nil
	}
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Value:       []any{"PT5M", "P1DT2H"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "duration"}),
		},
		LiteralArg{
			Value:       []any{"2024-01-02"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "date"}),
		},
		LiteralArg{
			Value:       []any{"2024-01-02T03:04:05Z"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "date-time"}),
		},
		LiteralArg{
			Value:       map[string]any{"short": "PT30S"},
			ValueSchema: objectSchema(map[string]any{"type": "string", "format": "duration"}),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal([]time.Duration{5 * time.Minute, 26 * time.Hour}, got[0].Interface())
	s.Equal([]time.Time{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}, got[1].Interface())
	s.Equal([]time.Time{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}, got[2].Interface())
	s.Equal(map[string]time.Duration{"short": 30 * time.Second}, got[3].Interface())
}

func (s *BindingSuite) TestResolveNestedTemporalReportsTheBadElement() {
	fn := func(windows []time.Duration) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{
			Value:       []any{"PT5M", "nope"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "duration"}),
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "element 1")
		s.Contains(err.Error(), "not an ISO-8601 duration")
	}
}

func (s *BindingSuite) TestResolveNestedUUIDContainers() {
	a := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	b := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	fn := func(ids []uuid.UUID, byName map[string]uuid.UUID) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Value:       []any{a.String(), b.String()},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "uuid"}),
		},
		LiteralArg{
			Value:       map[string]any{"primary": a.String()},
			ValueSchema: objectSchema(map[string]any{"type": "string", "format": "uuid"}),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal([]uuid.UUID{a, b}, got[0].Interface())
	s.Equal(map[string]uuid.UUID{"primary": a}, got[1].Interface())
}

func (s *BindingSuite) TestResolveNestedUUIDRejectsFormatMismatch() {
	fn := func(ids []uuid.UUID) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{
			Value:       []any{"550e8400-e29b-41d4-a716-446655440000"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "date-time"}),
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "cannot bind")
	}
}

func (s *BindingSuite) TestResolveNullableTemporalLiteral() {
	nullable := anyOfSchema(
		map[string]any{"type": "string", "format": "date-time"},
		map[string]any{"type": "null"},
	)
	fn := func(when *time.Time) error { return nil }

	got, err := s.resolve(fn, []Arg{
		LiteralArg{Value: nil, ValueSchema: nullable, FromDefault: true},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.True(got[0].IsNil(), "a null datetime arrives as a nil pointer")

	got, err = s.resolve(fn, []Arg{
		LiteralArg{Value: "2024-01-02T03:04:05Z", ValueSchema: nullable},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), got[0].Elem().Interface())
}

func (s *BindingSuite) TestResolveDurationRejectsNumber() {
	fn := func(window time.Duration) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{Value: 300, ValueSchema: temporalSchema("duration")},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "would be read as nanoseconds")
	}
}

func (s *BindingSuite) TestParseISO8601Duration() {
	valid := map[string]time.Duration{
		"PT5M":        5 * time.Minute,
		"P1DT2H3M4S":  26*time.Hour + 3*time.Minute + 4*time.Second,
		"-PT1M30S":    -(time.Minute + 30*time.Second),
		"PT1.5S":      1500 * time.Millisecond,
		"P2W":         14 * 24 * time.Hour,
		"P1D":         24 * time.Hour,
		"PT0S":        0,
		"+PT10M":      10 * time.Minute,
		"P1DT2H":      26 * time.Hour,
		"PT0.000001S": time.Microsecond,
	}
	for text, want := range valid {
		s.Run(text, func() {
			got, err := parseISO8601Duration(text)
			s.Require().NoError(err)
			s.Equal(want, got)
		})
	}

	invalid := map[string]string{
		"P1M":     "no fixed length",
		"P1Y":     "no fixed length",
		"-P2M":    "no fixed length",
		"300":     "not an ISO-8601 duration",
		"5m":      "not an ISO-8601 duration",
		"":        "not an ISO-8601 duration",
		"P":       "carries no components",
		"PT":      "carries no components",
		"PT5X":    "not an ISO-8601 duration",
		"P1DT2H3": "not an ISO-8601 duration",
	}
	for text, wantErr := range invalid {
		s.Run("invalid/"+text, func() {
			_, err := parseISO8601Duration(text)
			if s.Assert().Error(err) {
				s.Contains(err.Error(), wantErr)
			}
		})
	}
}

func (s *BindingSuite) TestParseTemporalRejectsMalformedValues() {
	for _, tc := range []struct{ text, format string }{
		{"not-a-date", "date-time"},
		{"2024-13-45", "date"},
		{"99:99:99", "time"},
		{"2024-01-02", "date-time"},
	} {
		s.Run(tc.format+"/"+tc.text, func() {
			_, err := parseTemporal(tc.text, tc.format)
			if s.Assert().Error(err) {
				s.Contains(err.Error(), "is not a valid")
			}
		})
	}
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

		"datetime-to-time":     {temporalSchema("date-time"), reflect.TypeFor[time.Time](), ""},
		"datetime-to-time-ptr": {temporalSchema("date-time"), reflect.TypeFor[*time.Time](), ""},
		"date-to-time":         {temporalSchema("date"), reflect.TypeFor[time.Time](), ""},
		"time-to-time":         {temporalSchema("time"), reflect.TypeFor[time.Time](), ""},
		"duration-to-duration": {temporalSchema("duration"), reflect.TypeFor[time.Duration](), ""},
		"uuid-to-uuid":         {temporalSchema("uuid"), reflect.TypeFor[uuid.UUID](), ""},
		"datetime-to-string":   {temporalSchema("date-time"), reflect.TypeFor[string](), ""},

		"integer-vs-time":      {argSchema("integer"), reflect.TypeFor[time.Time](), "cannot bind"},
		"plain-string-vs-time": {argSchema("string"), reflect.TypeFor[time.Time](), "cannot bind"},
		"plain-string-vs-uuid": {argSchema("string"), reflect.TypeFor[uuid.UUID](), "cannot bind"},
		"date-time-vs-uuid": {
			temporalSchema("date-time"),
			reflect.TypeFor[uuid.UUID](),
			"cannot bind",
		},
		"date-time-vs-slice": {
			temporalSchema("date-time"),
			reflect.TypeFor[[]string](),
			"cannot bind",
		},
		"integer-vs-duration": {
			argSchema("integer"),
			reflect.TypeFor[time.Duration](),
			"cannot bind",
		},
		"duration-vs-int64": {
			temporalSchema("duration"),
			reflect.TypeFor[int64](),
			"cannot bind",
		},
		// Pydantic encodes bytes as a string.
		"binary-to-string": {temporalSchema("binary"), reflect.TypeFor[string](), ""},
		"binary-vs-bytes":  {temporalSchema("binary"), reflect.TypeFor[[]byte](), "cannot bind"},

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
				map[string]any{"type": "string", "format": "date-time"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[*time.Time](), "",
		},
		"anyof-nullable-string-ptr-ok": {
			anyOfSchema(
				map[string]any{"type": "string"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[*string](), "",
		},
		"anyof-nullable-needs-pointer": {
			anyOfSchema(
				map[string]any{"type": "string", "format": "date-time"},
				map[string]any{"type": "null"},
			),
			reflect.TypeFor[time.Time](), "must be a pointer",
		},
		"anyof-nullable-wrong-type": {
			anyOfSchema(
				map[string]any{"type": "string", "format": "date-time"},
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
			_, err := checkValueType(tt.schema, tt.target)
			if tt.errContains == "" {
				s.NoError(err)
			} else if s.Assert().Error(err) {
				s.Contains(err.Error(), tt.errContains)
			}
		})
	}
}

func (s *BindingSuite) TestCheckValueTypeReturnsMatchedShape() {
	shape, err := checkValueType(temporalSchema("date-time"), reflect.TypeFor[time.Time]())
	s.Require().NoError(err)
	s.Equal("date-time", shape.format)

	shape, err = checkValueType(
		anyOfSchema(
			map[string]any{"type": "string", "format": "date-time"},
			map[string]any{"type": "null"},
		),
		reflect.TypeFor[*time.Time](),
	)
	s.Require().NoError(err)
	s.Equal("date-time", shape.format)

	shape, err = checkValueType(
		arraySchema(map[string]any{"type": "string", "format": "duration"}),
		reflect.TypeFor[[]time.Duration](),
	)
	s.Require().NoError(err)
	s.Equal("duration", shape.child("items").format)
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

type windowConfig struct {
	Name   string        `json:"name"`
	Window time.Duration `json:"window"`
	Start  time.Time     `json:"start"`
}

func (s *BindingSuite) TestResolveNativeStructFields() {
	schema := genmodels.ArgValueSchema{
		"type": "object",
		"properties": map[string]any{
			"window": map[string]any{"type": "string", "format": "duration"},
			"start":  map[string]any{"type": "string", "format": "date"},
		},
	}
	fn := func(config windowConfig) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name: "config",
			Value: map[string]any{
				"name":   "nightly",
				"window": "PT5M",
				"start":  "2024-01-02",
			},
			ValueSchema: &schema,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal(windowConfig{
		Name:   "nightly",
		Window: 5 * time.Minute,
		Start:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}, got[0].Interface())
}

func (s *BindingSuite) TestResolveNativeContainerWithoutItemsSchema() {
	fn := func(windows []time.Duration, byName map[string]time.Duration) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "windows", Value: []any{"PT5M", "PT30S"}},
		LiteralArg{Name: "by_name", Value: map[string]any{"short": "PT1S"}},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal([]time.Duration{5 * time.Minute, 30 * time.Second}, got[0].Interface())
	s.Equal(map[string]time.Duration{"short": time.Second}, got[1].Interface())
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

func (s *BindingSuite) TestParseISO8601DurationRejectsOverflow() {
	_, err := parseISO8601Duration("P200000D")
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "does not fit in a time.Duration")
	}
}

type nativeCommon struct {
	Window time.Duration `json:"window"`
}

type embeddedNativeConfig struct {
	nativeCommon
	Name string `json:"name"`
}

func (s *BindingSuite) TestResolveNativeFieldPromotedFromEmbeddedStruct() {
	schema := genmodels.ArgValueSchema{
		"type": "object",
		"properties": map[string]any{
			"window": map[string]any{"type": "string", "format": "duration"},
		},
	}
	fn := func(config embeddedNativeConfig) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "config",
			Value:       map[string]any{"name": "nightly", "window": "PT5M"},
			ValueSchema: &schema,
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	config := got[0].Interface().(embeddedNativeConfig)
	s.Equal("nightly", config.Name)
	s.Equal(5*time.Minute, config.Window)
}

func (s *BindingSuite) TestResolveNativeFixedArray() {
	fn := func(windows [2]time.Duration) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "windows",
			Value:       []any{"PT5M", "PT30S"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "duration"}),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	s.Equal([2]time.Duration{5 * time.Minute, 30 * time.Second}, got[0].Interface())

	_, err = s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "windows",
			Value:       []any{"PT5M"},
			ValueSchema: arraySchema(map[string]any{"type": "string", "format": "duration"}),
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "expected 2 elements")
	}
}

func (s *BindingSuite) TestParseISO8601DurationRejectsBoundaryOverflow() {
	_, err := parseISO8601Duration("PT9223372036.854775808S")
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "does not fit in a time.Duration")
	}
}

func (s *BindingSuite) TestResolveNestedTypeMismatchFailsLoudly() {
	fn := func(windows []time.Duration) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:        "windows",
			Value:       []any{"PT5M"},
			ValueSchema: arraySchema(map[string]any{"type": "string"}),
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "cannot bind to Go type time.Duration")
	}

	fn2 := func(config windowConfig) error { return nil }
	_, err = s.resolve(fn2, []Arg{
		LiteralArg{
			Name:  "config",
			Value: map[string]any{"name": "nightly", "window": "PT5M"},
			ValueSchema: &genmodels.ArgValueSchema{
				"type":       "object",
				"properties": map[string]any{"window": map[string]any{"type": "string"}},
			},
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "cannot bind to Go type time.Duration")
	}
}

func (s *BindingSuite) TestResolveNestedNullFailsLoudly() {
	fn := func(windows []time.Duration) error { return nil }
	_, err := s.resolve(fn, []Arg{
		LiteralArg{
			Name:  "windows",
			Value: []any{"PT5M", nil},
			ValueSchema: arraySchema(map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string", "format": "duration"},
					map[string]any{"type": "null"},
				},
			}),
		},
	}, &fakeXComClient{})
	if s.Assert().Error(err) {
		s.Contains(err.Error(), "element 1")
		s.Contains(err.Error(), "not nilable")
	}

	fnPtr := func(windows []*time.Duration) error { return nil }
	got, err := s.resolve(fnPtr, []Arg{
		LiteralArg{
			Name:  "windows",
			Value: []any{"PT5M", nil},
			ValueSchema: arraySchema(map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string", "format": "duration"},
					map[string]any{"type": "null"},
				},
			}),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	windows := got[0].Interface().([]*time.Duration)
	s.Require().Len(windows, 2)
	s.Equal(5*time.Minute, *windows[0])
	s.Nil(windows[1], "a nested None arrives as a nil pointer")
}

type shallowOuter struct {
	inner
	Window string `json:"window"`
}

type inner struct {
	Window time.Duration `json:"window"`
}

func (s *BindingSuite) TestResolveShallowestFieldWinsOverEmbedded() {
	fn := func(prefix string, config shallowOuter) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "prefix", Value: "p", ValueSchema: argSchema("string")},
		LiteralArg{
			Name:        "config",
			Value:       map[string]any{"window": "PT5M"},
			ValueSchema: argSchema("object"),
		},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	config := got[1].Interface().(shallowOuter)
	s.Equal("PT5M", config.Window, "the shallow string field takes the value verbatim")
	s.Zero(config.inner.Window, "the embedded duration field is not the target")
}

func (s *BindingSuite) TestResolveNestedNullableTemporalKeepsFormat() {
	nullableDate := arraySchema(map[string]any{
		"anyOf": []any{
			map[string]any{"type": "string", "format": "date"},
			map[string]any{"type": "null"},
		},
	})
	fn := func(days []*time.Time) error { return nil }
	got, err := s.resolve(fn, []Arg{
		LiteralArg{Name: "days", Value: []any{"2024-01-02", nil}, ValueSchema: nullableDate},
	}, &fakeXComClient{})
	s.Require().NoError(err)
	days := got[0].Interface().([]*time.Time)
	s.Require().Len(days, 2)
	s.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), *days[0])
	s.Nil(days[1])
}
