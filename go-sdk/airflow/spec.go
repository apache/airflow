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

package airflow

import "reflect"

// DagSpec and TaskSpec are generated from Airflow core's Dag serialization schema,
// which Python owns, so that neither struct drifts from it silently. genspec
// rewrites the schema into the authoring shape, go-jsonschema writes the structs,
// and genspec puts the license header back on what it wrote. The rewritten schema
// is a build artifact under .build; spec.gen.go is committed.
//
// To change a field, change the schema on the Python side, or the exclusions, type
// overrides and injected properties in internal/genspec/authoring.go, and run
// `just generate-specs`.
//
// The structs carry no struct tags, because they are the authoring shape and not the
// wire format: encoding/json would write a time.Duration as the nanoseconds Go counts it
// in where the schema means seconds, and omitempty would drop a value an author set to
// the zero value. Serializing a Dag converts the fields; a tag would invite
// json.Marshal(spec) to skip that step and produce a shape core misreads.

//go:generate go run ../internal/genspec -schema ../../airflow-core/src/airflow/serialization/schema.json -out ../../.build/go-sdk/spec.schema.json
//go:generate go run github.com/atombender/go-jsonschema@v0.23.1 --only-models --struct-name-from-title --tags "" --capitalization ID --capitalization JSON --capitalization MD --capitalization XCom -p airflow -o spec.gen.go ../../.build/go-sdk/spec.schema.json
//go:generate go run ../internal/genspec -license spec.gen.go

// copySpec returns a copy of spec that shares no slice, map or pointer with it, so that a
// caller changing what it still holds cannot change a registered Dag. Assigning a spec
// copies a slice header or a pointer and not what it points at, which is the sharing this
// undoes.
//
// It walks the spec with reflection rather than naming the fields because the spec types
// are generated: a field named here would have to be added again every time the
// serialization schema grows one, and the copy would go quietly missing until someone
// noticed a registered Dag changing underneath them.
func copySpec[T any](spec T) T {
	return copyValue(reflect.ValueOf(spec)).Interface().(T)
}

func copyValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(copyValue(v.Elem()))
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			out.Index(i).Set(copyValue(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		for iter := v.MapRange(); iter.Next(); {
			out.SetMapIndex(copyValue(iter.Key()), copyValue(iter.Value()))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		// Assigning first leaves an unexported field the shallow copy Go itself would
		// make; only the fields reflection can set are replaced by a copy of their own.
		out.Set(v)
		for i := range v.NumField() {
			if field := out.Field(i); field.CanSet() {
				field.Set(copyValue(v.Field(i)))
			}
		}
		return out
	default:
		return v
	}
}
