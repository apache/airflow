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

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/contexttest"
)

// methodSet maps each method of typ to its signature without the receiver.
func methodSet(typ reflect.Type) map[string]string {
	methods := make(map[string]string, typ.NumMethod())
	for i := range typ.NumMethod() {
		method := typ.Method(i)
		in := make([]reflect.Type, 0, method.Type.NumIn()-1)
		for j := 1; j < method.Type.NumIn(); j++ {
			in = append(in, method.Type.In(j))
		}
		out := make([]reflect.Type, 0, method.Type.NumOut())
		for j := range method.Type.NumOut() {
			out = append(out, method.Type.Out(j))
		}
		methods[method.Name] = reflect.FuncOf(in, out, method.Type.IsVariadic()).String()
	}
	return methods
}

// The tests of pkg/binding, pkg/execution and internal/bundle declare contexttest.Context where
// a handler takes a Context. Those tests stop proving anything about Context when the two types
// have different methods.
func TestContexttestMatchesContext(t *testing.T) {
	assert.Equal(t,
		methodSet(reflect.TypeFor[Context]()),
		methodSet(reflect.TypeFor[contexttest.Context]()),
	)

	real, standIn := reflect.TypeOf(NewContext), reflect.TypeOf(contexttest.New)
	require.Equal(t, real.NumIn(), standIn.NumIn())
	for i := range real.NumIn() {
		assert.Equal(t, real.In(i), standIn.In(i), "parameter %d", i)
	}
}
