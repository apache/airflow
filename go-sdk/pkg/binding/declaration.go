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
	"time"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// Declare describes the parameters that the task function's TaskFlow arguments fill, so the Dag
// processor can check a stub task against its Go handler before the task runs.
//
// Flat parameters bind by position and carry no name. A lone struct binds by field name,
// exactly for an `arg:`-tagged field and ignoring case and underscores otherwise. An untagged
// lone struct can also take one unmatched argument as a whole value.
func (p *Plan) Declare(taskID string) genmodels.TaskHandlerDeclaration {
	decl := genmodels.TaskHandlerDeclaration{
		TaskID:  taskID,
		Binding: genmodels.TaskHandlerDeclarationBindingPositional,
		Params:  []genmodels.TaskHandlerParam{},
	}
	for _, plan := range p.params {
		switch plan.kind {
		case paramData:
			decl.Params = append(decl.Params, genmodels.TaskHandlerParam{
				Required:    true,
				ValueSchema: valueSchema(plan.typ),
			})
		case paramLoneStruct:
			decl.Binding = genmodels.TaskHandlerDeclarationBindingNamedOrWhole
			if plan.tagged {
				decl.Binding = genmodels.TaskHandlerDeclarationBindingNamed
			}
			// An unfilled field keeps its zero value, so none is required.
			for _, sf := range plan.fields {
				decl.Params = append(decl.Params, genmodels.TaskHandlerParam{
					Name:        sf.argName,
					ExactName:   sf.tagged,
					ValueSchema: valueSchema(sf.fieldType),
				})
			}
		}
	}
	return decl
}

// valueSchema returns the JSON Schema of the values a parameter of type t accepts, or nil when
// no schema can state them.
//
// It states only what decoding into t enforces, so a value the schema rejects is one the task
// would fail on. The exception is a null element or map value, which decoding turns into the
// zero value; like the runtime's own argument check, the schema states null only at the top level.
// The vocabulary is the one build_arg_bindings emits for a stub parameter's Python annotation,
// so the common pairs, such as int and int, come out equal.
func valueSchema(t reflect.Type) *genmodels.ArgValueSchema {
	fragment := schemaFragment(t, map[reflect.Type]bool{})
	if fragment == nil {
		return nil
	}
	schema := make(genmodels.ArgValueSchema, len(fragment))
	for k, v := range fragment {
		schema[k] = v
	}
	return &schema
}

var (
	timeType       = reflect.TypeFor[time.Time]()
	jsonNumberType = reflect.TypeFor[json.Number]()
)

func schemaFragment(t reflect.Type, visiting map[reflect.Type]bool) map[string]any {
	// A type that contains itself is left unconstrained where it recurs.
	if visiting[t] {
		return nil
	}
	visiting[t] = true
	defer delete(visiting, t)

	switch t.Kind() {
	case reflect.Pointer:
		return nullable(schemaFragment(t.Elem(), visiting))
	case reflect.Slice, reflect.Map:
		// Decoding null into a slice or map leaves it nil.
		return nullable(nonNullFragment(t, visiting))
	default:
		return nonNullFragment(t, visiting)
	}
}

func nonNullFragment(t reflect.Type, visiting map[reflect.Type]bool) map[string]any {
	switch {
	case t == timeType:
		return map[string]any{"type": "string", "format": "date-time"}
	// json.Number takes a number or a numeric string.
	case t == jsonNumberType:
		return nil
	case implementsJSONUnmarshaler(t):
		return nil
	case implementsTextUnmarshaler(t):
		return map[string]any{"type": "string"}
	}

	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return signedIntFragment(t.Bits())
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr:
		return unsignedIntFragment(t.Bits())
	case reflect.Float32:
		return map[string]any{"type": "number", "format": "float"}
	case reflect.Float64:
		return map[string]any{"type": "number", "format": "double"}
	case reflect.Slice:
		// A byte slice takes a base64 string as well as an array.
		if t.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		return arrayFragment(t.Elem(), visiting)
	case reflect.Array:
		return arrayFragment(t.Elem(), visiting)
	case reflect.Map:
		var values any = true
		if fragment := schemaFragment(t.Elem(), visiting); fragment != nil {
			values = fragment
		}
		return map[string]any{"type": "object", "additionalProperties": values}
	case reflect.Struct:
		return map[string]any{"type": "object"}
	default:
		return nil
	}
}

func signedIntFragment(bits int) map[string]any {
	switch bits {
	case 64:
		return map[string]any{"type": "integer", "format": "int64"}
	case 32:
		return map[string]any{"type": "integer", "format": "int32"}
	default:
		return map[string]any{
			"type":    "integer",
			"minimum": -(int64(1) << (bits - 1)),
			"maximum": int64(1)<<(bits-1) - 1,
		}
	}
}

func unsignedIntFragment(bits int) map[string]any {
	return map[string]any{
		"type":    "integer",
		"minimum": uint64(0),
		"maximum": uint64(math.MaxUint64) >> (64 - bits),
	}
}

func arrayFragment(elem reflect.Type, visiting map[reflect.Type]bool) map[string]any {
	items := schemaFragment(elem, visiting)
	if items == nil {
		items = map[string]any{}
	}
	return map[string]any{"type": "array", "items": items}
}

func nullable(fragment map[string]any) map[string]any {
	if fragment == nil {
		return nil
	}
	if branches, isUnion := fragment["anyOf"].([]any); isUnion {
		for _, branch := range branches {
			if b, ok := branch.(map[string]any); ok && b["type"] == "null" {
				return fragment
			}
		}
	}
	return map[string]any{"anyOf": []any{fragment, map[string]any{"type": "null"}}}
}

func implementsJSONUnmarshaler(t reflect.Type) bool {
	return t.Implements(jsonUnmarshalerType) || reflect.PointerTo(t).Implements(jsonUnmarshalerType)
}

func implementsTextUnmarshaler(t reflect.Type) bool {
	return t.Implements(textUnmarshalerType) || reflect.PointerTo(t).Implements(textUnmarshalerType)
}
