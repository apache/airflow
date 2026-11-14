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

// Package jsonvalue prepares a value decoded from JSON to be sent to Airflow as msgpack.
package jsonvalue

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// ResolveNumbers replaces each json.Number in value with an int64, a uint64 or a float64, and
// returns value. It changes a map or a slice in value in place. path names value in an error, as
// in ["rows"][2].
func ResolveNumbers(value any, path string) (any, error) {
	switch v := value.(type) {
	case json.Number:
		return resolveNumber(v, path)
	case map[string]any:
		// The keys are sorted, so that a value with more than one bad number always gets the same
		// error.
		for _, key := range slices.Sorted(maps.Keys(v)) {
			resolved, err := ResolveNumbers(v[key], path+"["+strconv.Quote(key)+"]")
			if err != nil {
				return nil, err
			}
			v[key] = resolved
		}
	case []any:
		for i, item := range v {
			resolved, err := ResolveNumbers(item, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			v[i] = resolved
		}
	}
	return value, nil
}

func resolveNumber(n json.Number, path string) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return u, nil
	}
	f, err := n.Float64()
	// encoding/json writes a float64 such as 1e20 with neither a point nor an exponent, so a number
	// written like an integer can come from a float64. Such a number is read as a float64 only when
	// the float64 formats back to the same digits. Otherwise the number is an integer that does not
	// fit in 64 bits.
	if !strings.ContainsAny(n.String(), ".eE") &&
		(err != nil || strconv.FormatFloat(f, 'f', -1, 64) != n.String()) {
		return nil, fmt.Errorf("the integer at %s is %s, which does not fit in 64 bits", path, n)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"the number at %s is %s, which is past the range of a float64",
			path,
			n,
		)
	}
	return f, nil
}
