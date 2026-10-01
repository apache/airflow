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
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// specPath is where the Go half of each value set is declared, reached from this package.
const specPath = "spec.go"

func readLines(t *testing.T, path, pattern string) []string {
	t.Helper()

	body, err := os.ReadFile(filepath.FromSlash(path))
	require.NoError(t, err)
	matches := regexp.MustCompile(pattern).FindAllStringSubmatch(string(body), -1)
	require.NotEmpty(t, matches, "%s matched nothing, so the file's shape has changed", path)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}
	return values
}

// TestRuleConstantsMatchPython is the tripwire for a trigger rule or a weight rule
// added or renamed on the Python side. The serialization schema types both fields as a
// plain string and names none of their values, so the constants here are hand-written
// and the spec drift check cannot see them; Airflow rejects a rule it does not know, so
// a missing constant is a Dag that fails to register.
func TestRuleConstantsMatchPython(t *testing.T) {
	for _, tt := range []struct {
		goType     string
		pythonPath string
	}{
		{goType: "TriggerRule", pythonPath: "../../airflow-core/src/airflow/task/trigger_rule.py"},
		{goType: "WeightRule", pythonPath: "../../airflow-core/src/airflow/task/weight_rule.py"},
	} {
		t.Run(tt.goType, func(t *testing.T) {
			python := readLines(t, tt.pythonPath, `(?m)^    [A-Z_]+ = "([a-z_]+)"$`)
			spelled := readLines(
				t,
				specPath,
				`(?m)^\t`+tt.goType+`\w+ +`+tt.goType+` = "([a-z_]+)"$`,
			)

			assert.ElementsMatch(t, python, spelled)
		})
	}
}
