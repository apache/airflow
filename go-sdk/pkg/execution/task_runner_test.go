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

package execution

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeDagRunConfNumbers(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  any
	}{
		{name: "int", value: int(7), want: int64(7)},
		{name: "int8", value: int8(-1), want: int64(-1)},
		{name: "int16", value: int16(-129), want: int64(-129)},
		{name: "int32", value: int32(-32769), want: int64(-32769)},
		{name: "uint", value: uint(500), want: int64(500)},
		{name: "uint8", value: uint8(200), want: int64(200)},
		{name: "uint16", value: uint16(500), want: int64(500)},
		{name: "uint32", value: uint32(70000), want: int64(70000)},
		{
			name:  "large exact integer",
			value: uint64(9007199254740993),
			want:  int64(9007199254740993),
		},
		{name: "max int64", value: uint64(math.MaxInt64), want: int64(math.MaxInt64)},
		{
			name:  "above max int64",
			value: uint64(math.MaxInt64) + 1,
			want:  uint64(math.MaxInt64) + 1,
		},
		{name: "float32", value: float32(1.25), want: float64(1.25)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := map[string]any{"value": tc.value}
			got := normalizeDagRunConf(original)
			assert.Equal(t, tc.want, got["value"])
			assert.Equal(t, tc.value, original["value"])
		})
	}
}

func TestNormalizeDagRunConfCopiesContainers(t *testing.T) {
	original := map[string]any{
		"label":       "original",
		"nested":      map[string]any{"bucket": "reports"},
		"list":        []any{map[string]any{"region": "us"}, []any{"eu"}},
		"nil_map":     map[string]any(nil),
		"empty_map":   map[string]any{},
		"nil_slice":   []any(nil),
		"empty_slice": []any{},
		"flag":        true,
		"null":        nil,
	}
	normalized := normalizeDagRunConf(original)
	require.Equal(t, original, normalized)

	normalized["label"] = "changed"
	normalized["nested"].(map[string]any)["bucket"] = "changed"
	normalized["list"].([]any)[0].(map[string]any)["region"] = "changed"
	normalized["list"].([]any)[1].([]any)[0] = "changed"

	assert.Equal(t, "original", original["label"])
	assert.Equal(t, "reports", original["nested"].(map[string]any)["bucket"])
	assert.Equal(t, "us", original["list"].([]any)[0].(map[string]any)["region"])
	assert.Equal(t, "eu", original["list"].([]any)[1].([]any)[0])
}
