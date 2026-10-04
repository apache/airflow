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
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

type declRegion string

type declLevel int

func (l *declLevel) UnmarshalText([]byte) error { return nil }

type declCustom struct{ V int }

func (c *declCustom) UnmarshalJSON([]byte) error { return nil }

type declTree []declTree

type declEmbedded struct{ Count int }

type declTaggedInput struct {
	Region    string `arg:"region_code"`
	Threshold float64
	declEmbedded
}

type declUntaggedInput struct {
	RegionCode string
	Labels     map[string]string
}

// declUnbindable has an exported field, but a func cannot receive a task argument.
type declUnbindable struct {
	Callback func()
}

var (
	stringFragment = map[string]any{"type": "string"}
	int64Fragment  = map[string]any{"type": "integer", "format": "int64"}
	doubleFragment = map[string]any{"type": "number", "format": "double"}
)

func nullOr(fragment map[string]any) map[string]any {
	return map[string]any{"anyOf": []any{fragment, map[string]any{"type": "null"}}}
}

func schemaOf(fragment map[string]any) *genmodels.ArgValueSchema {
	schema := make(genmodels.ArgValueSchema, len(fragment))
	for k, v := range fragment {
		schema[k] = v
	}
	return &schema
}

func TestValueSchema(t *testing.T) {
	for name, tc := range map[string]struct {
		typ  reflect.Type
		want map[string]any
	}{
		"string":       {reflect.TypeFor[string](), stringFragment},
		"named string": {reflect.TypeFor[declRegion](), stringFragment},
		"bool":         {reflect.TypeFor[bool](), map[string]any{"type": "boolean"}},
		"int":          {reflect.TypeFor[int](), int64Fragment},
		"int64":        {reflect.TypeFor[int64](), int64Fragment},
		"int32": {
			reflect.TypeFor[int32](),
			map[string]any{"type": "integer", "format": "int32"},
		},
		"int8": {
			reflect.TypeFor[int8](),
			map[string]any{"type": "integer", "minimum": int64(-128), "maximum": int64(127)},
		},
		"int16": {
			reflect.TypeFor[int16](),
			map[string]any{"type": "integer", "minimum": int64(-32768), "maximum": int64(32767)},
		},
		"uint8": {
			reflect.TypeFor[uint8](),
			map[string]any{"type": "integer", "minimum": uint64(0), "maximum": uint64(255)},
		},
		"uint32": {
			reflect.TypeFor[uint32](),
			map[string]any{"type": "integer", "minimum": uint64(0), "maximum": uint64(math.MaxUint32)},
		},
		"uint64": {
			reflect.TypeFor[uint64](),
			map[string]any{"type": "integer", "minimum": uint64(0), "maximum": uint64(math.MaxUint64)},
		},
		"float32": {
			reflect.TypeFor[float32](),
			map[string]any{"type": "number", "format": "float"},
		},
		"float64": {reflect.TypeFor[float64](), doubleFragment},
		"time.Time": {
			reflect.TypeFor[time.Time](),
			map[string]any{"type": "string", "format": "date-time"},
		},
		"pointer to time.Time": {
			reflect.TypeFor[*time.Time](),
			nullOr(map[string]any{"type": "string", "format": "date-time"}),
		},
		"text unmarshaler on the pointer": {reflect.TypeFor[declLevel](), stringFragment},
		"json unmarshaler":                {reflect.TypeFor[declCustom](), nil},
		"json.Number":                     {reflect.TypeFor[json.Number](), nil},
		"byte slice":                      {reflect.TypeFor[[]byte](), nil},
		"any":                             {reflect.TypeFor[any](), nil},
		"pointer to any":                  {reflect.TypeFor[*any](), nil},
		"complex":                         {reflect.TypeFor[complex128](), nil},
		"slice": {
			reflect.TypeFor[[]string](),
			nullOr(map[string]any{"type": "array", "items": stringFragment}),
		},
		"slice of any": {
			reflect.TypeFor[[]any](),
			nullOr(map[string]any{"type": "array", "items": map[string]any{}}),
		},
		"array": {
			reflect.TypeFor[[3]int](),
			map[string]any{"type": "array", "items": int64Fragment},
		},
		"map": {
			reflect.TypeFor[map[string]int](),
			nullOr(map[string]any{"type": "object", "additionalProperties": int64Fragment}),
		},
		"map of any": {
			reflect.TypeFor[map[string]any](),
			nullOr(map[string]any{"type": "object", "additionalProperties": true}),
		},
		"struct":             {reflect.TypeFor[declUntaggedInput](), map[string]any{"type": "object"}},
		"pointer":            {reflect.TypeFor[*int](), nullOr(int64Fragment)},
		"pointer to pointer": {reflect.TypeFor[**int](), nullOr(int64Fragment)},
		"pointer to slice": {
			reflect.TypeFor[*[]int](),
			nullOr(map[string]any{"type": "array", "items": int64Fragment}),
		},
		"recursive": {
			reflect.TypeFor[declTree](),
			nullOr(map[string]any{"type": "array", "items": map[string]any{}}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := valueSchema(tc.typ)
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, schemaOf(tc.want), got)
		})
	}
}

func declare(t *testing.T, fn any) genmodels.TaskHandlerDeclaration {
	t.Helper()
	plan, err := Analyze(reflect.TypeOf(fn), "testFn")
	require.NoError(t, err)
	return plan.Declare("task")
}

func TestDeclareFlatParams(t *testing.T) {
	decl := declare(t, func(
		actx contexttest.Context, region string, count int, note *string, config declUntaggedInput,
	) error {
		return nil
	})

	assert.Equal(t, genmodels.TaskHandlerDeclaration{
		TaskID:  "task",
		Binding: genmodels.TaskHandlerDeclarationBindingPositional,
		Params: &genmodels.TaskHandlerParams{
			{ValueSchema: schemaOf(stringFragment)},
			{ValueSchema: schemaOf(int64Fragment)},
			{ValueSchema: schemaOf(nullOr(stringFragment))},
			{ValueSchema: schemaOf(map[string]any{"type": "object"})},
		},
	}, decl)
}

func TestDeclareTaggedStruct(t *testing.T) {
	decl := declare(t, func(actx contexttest.Context, in declTaggedInput) error { return nil })

	assert.Equal(t, genmodels.TaskHandlerDeclaration{
		TaskID:  "task",
		Binding: genmodels.TaskHandlerDeclarationBindingNamed,
		Params: &genmodels.TaskHandlerParams{
			{Name: "region_code", ExactName: true, ValueSchema: schemaOf(stringFragment)},
			{Name: "Threshold", ValueSchema: schemaOf(doubleFragment)},
			{Name: "Count", ValueSchema: schemaOf(int64Fragment)},
		},
	}, decl)
}

func TestDeclareUntaggedStruct(t *testing.T) {
	want := genmodels.TaskHandlerDeclaration{
		TaskID:  "task",
		Binding: genmodels.TaskHandlerDeclarationBindingNamed,
		Params: &genmodels.TaskHandlerParams{
			{Name: "RegionCode", ValueSchema: schemaOf(stringFragment)},
			{
				Name: "Labels",
				ValueSchema: schemaOf(nullOr(map[string]any{
					"type": "object", "additionalProperties": stringFragment,
				})),
			},
		},
	}
	for name, fn := range map[string]any{
		"value":   func(actx contexttest.Context, in declUntaggedInput) error { return nil },
		"pointer": func(actx contexttest.Context, in *declUntaggedInput) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, declare(t, fn))
		})
	}
}

// A lone struct no field of which can bind an argument still declares as named, but with no
// params to check: the runtime takes a lone argument as its whole value, which a list cannot name.
func TestDeclareLoneStructWithoutBindableFields(t *testing.T) {
	for name, fn := range map[string]any{
		"time.Time":            func(actx contexttest.Context, at time.Time) error { return nil },
		"pointer to time.Time": func(actx contexttest.Context, at *time.Time) error { return nil },
		"empty struct":         func(actx contexttest.Context, in struct{}) error { return nil },
		"unbindable field":     func(actx contexttest.Context, in declUnbindable) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			decl := declare(t, fn)

			assert.Equal(t, genmodels.TaskHandlerDeclarationBindingNamed, decl.Binding)
			assert.Nil(t, decl.Params)
		})
	}
}

func TestDeclareContextOnly(t *testing.T) {
	decl := declare(t, func(actx contexttest.Context) error { return nil })

	assert.Equal(t, genmodels.TaskHandlerDeclaration{
		TaskID:  "task",
		Binding: genmodels.TaskHandlerDeclarationBindingPositional,
		Params:  &genmodels.TaskHandlerParams{},
	}, decl)
}
