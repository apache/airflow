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

// ifaceString returns the string carried by a nullable schema field decoded as
// any, or "" when the value is nil or not a string.
func ifaceString(v any) string {
	s, _ := v.(string)
	return s
}

// ifaceStringPtr preserves the null-vs-empty distinction for a nullable string
// field decoded as any: nil when missing/null, else a pointer to the string.
// Used for connection credentials, where an explicit "" must stay distinct from
// "not set".
func ifaceStringPtr(v any) *string {
	s, ok := v.(string)
	if !ok {
		return nil
	}
	return &s
}

// ifaceInt converts a numeric value decoded as any into int, truncating a float,
// and returns def when the value is nil or non-numeric.
func ifaceInt(v any, def int) int {
	if n, ok := ifaceNumber[int](v); ok {
		return n
	}
	return def
}

// ifaceFloat converts a numeric value decoded as any into float64, reporting
// false when it is not numeric.
func ifaceFloat(v any) (float64, bool) {
	return ifaceNumber[float64](v)
}

// ifaceNumber converts a numeric value decoded as any into T, reporting false
// when it is not numeric. msgpack decodes a number into whichever int, uint or
// float width fits it.
func ifaceNumber[T int | float64](v any) (T, bool) {
	switch n := v.(type) {
	case int:
		return T(n), true
	case int8:
		return T(n), true
	case int16:
		return T(n), true
	case int32:
		return T(n), true
	case int64:
		return T(n), true
	case uint:
		return T(n), true
	case uint8:
		return T(n), true
	case uint16:
		return T(n), true
	case uint32:
		return T(n), true
	case uint64:
		return T(n), true
	case float32:
		return T(n), true
	case float64:
		return T(n), true
	default:
		return 0, false
	}
}
