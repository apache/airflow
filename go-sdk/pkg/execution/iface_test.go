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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIfaceInt(t *testing.T) {
	for _, v := range []any{
		int(42), int8(42), int16(42), int32(42), int64(42),
		uint(42), uint8(42), uint16(42), uint32(42), uint64(42),
		float32(42), float64(42),
	} {
		assert.Equal(t, 42, ifaceInt(v, -1))
	}
	assert.Equal(t, -1, ifaceInt(nil, -1))
	assert.Equal(t, -1, ifaceInt("not a number", -1))
}

func TestIfaceFloat(t *testing.T) {
	for _, v := range []any{
		int(42), int8(42), int16(42), int32(42), int64(42),
		uint(42), uint8(42), uint16(42), uint32(42), uint64(42),
		float32(42), float64(42),
	} {
		f, ok := ifaceFloat(v)
		assert.True(t, ok)
		assert.Equal(t, 42.0, f)
	}
	f, ok := ifaceFloat(1.5)
	assert.True(t, ok)
	assert.Equal(t, 1.5, f)
	for _, v := range []any{nil, "not a number"} {
		_, ok := ifaceFloat(v)
		assert.False(t, ok)
	}
}

func TestIfaceStringHelpers(t *testing.T) {
	assert.Equal(t, "v", ifaceString("v"))
	assert.Equal(t, "", ifaceString(nil))
	assert.Equal(t, "", ifaceString(42))

	assert.Nil(t, ifaceStringPtr(nil))
	assert.Nil(t, ifaceStringPtr(42))
	if p := ifaceStringPtr(""); assert.NotNil(t, p) {
		assert.Equal(t, "", *p) // explicit "" stays distinct from "not set"
	}
}

func TestIfaceTimePtr(t *testing.T) {
	assert.Nil(t, ifaceTimePtr(nil))
	assert.Nil(t, ifaceTimePtr("not a time"))
	now := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if p := ifaceTimePtr(now); assert.NotNil(t, p) {
		assert.Equal(t, now, *p)
	}
}
