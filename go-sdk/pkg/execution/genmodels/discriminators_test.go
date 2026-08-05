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

package genmodels

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureType(t *testing.T) {
	t.Run("value body is stamped", func(t *testing.T) {
		got, ok := EnsureType(GetVariable{Key: "k"}).(GetVariable)
		require.True(t, ok)
		assert.Equal(t, TypeGetVariable, got.Type)
	})

	t.Run("wrong type is corrected", func(t *testing.T) {
		got, ok := EnsureType(SetXCom{Type: TypeGetConnection}).(SetXCom)
		require.True(t, ok)
		assert.Equal(t, TypeSetXCom, got.Type)
	})

	t.Run("pointer body is dereferenced and stamped", func(t *testing.T) {
		got, ok := EnsureType(&GetVariable{Key: "k"}).(GetVariable)
		require.True(t, ok)
		assert.Equal(t, TypeGetVariable, got.Type)
	})

	t.Run("non-body value passes through", func(t *testing.T) {
		m := map[string]any{"type": "x"}
		assert.Equal(t, m, EnsureType(m))
		assert.Nil(t, EnsureType(nil))
	})
}
