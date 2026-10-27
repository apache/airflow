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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enumsPath is the path, relative to this package, of the file that declares each enum.
const enumsPath = "enums.go"

func readMatches(t *testing.T, path, pattern string) [][]string {
	t.Helper()

	body, err := os.ReadFile(filepath.FromSlash(path))
	require.NoError(t, err)
	matches := regexp.MustCompile(pattern).FindAllStringSubmatch(string(body), -1)
	require.NotEmpty(t, matches, "%s matched nothing, so the file's shape has changed", path)
	return matches
}

// readPairs maps the first submatch of each match of pattern in the file at path to the second
// submatch, such as a constant name to its value.
func readPairs(t *testing.T, path, pattern string) map[string]string {
	t.Helper()

	pairs := make(map[string]string)
	for _, match := range readMatches(t, path, pattern) {
		pairs[match[1]] = match[2]
	}
	return pairs
}

// readPythonEnum maps the name of each member of the Python enum in the file at path to the
// value of the member. It fails the test if a line that assigns to an upper-case name at the
// indentation of a member is not written as NAME = "value". Without that check, a member written
// another way would go unread instead of failing the test.
func readPythonEnum(t *testing.T, path string) map[string]string {
	t.Helper()

	members := readPairs(t, path, `(?m)^    ([A-Z][A-Z0-9_]*) = "([^"]*)"`)
	var assigned []string
	for _, match := range readMatches(t, path, `(?m)^    ([A-Z][A-Z0-9_]*)\s*[:=]`) {
		assigned = append(assigned, match[1])
	}
	require.ElementsMatch(t, assigned, slices.Collect(maps.Keys(members)),
		"%s has a member that is not written as NAME = \"value\"", path)
	return members
}

// goConstantSuffix turns the name of a Python enum member, such as ALL_DONE_MIN_ONE_SUCCESS,
// into the part of a Go constant name after the type name, such as AllDoneMinOneSuccess.
func goConstantSuffix(pythonName string) string {
	var suffix strings.Builder
	for _, word := range strings.Split(strings.ToLower(pythonName), "_") {
		if word != "" {
			suffix.WriteString(strings.ToUpper(word[:1]) + word[1:])
		}
	}
	return suffix.String()
}

func toStrings[T ~string](values []T) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

// TestRuleConstantsMatchPython fails when the Python side adds or renames a trigger rule or a
// weight rule. The Dag serialization schema types trigger_rule and weight_rule as plain strings
// and does not list their values. The TriggerRule and WeightRule constants are therefore
// hand-written, outside the generated files that the check-go-sdk-generated-drift prek hook
// checks. The test also compares triggerRules and weightRules with the Python enums.
// checkTaskSpec accepts only the values in those lists, so an author cannot set a rule that the
// lists leave out.
func TestRuleConstantsMatchPython(t *testing.T) {
	for _, tt := range []struct {
		goType     string
		pythonPath string
		valid      []string
	}{
		{
			goType:     "TriggerRule",
			pythonPath: "../../airflow-core/src/airflow/task/trigger_rule.py",
			valid:      toStrings(triggerRules),
		},
		{
			goType:     "WeightRule",
			pythonPath: "../../airflow-core/src/airflow/task/weight_rule.py",
			valid:      toStrings(weightRules),
		},
	} {
		t.Run(tt.goType, func(t *testing.T) {
			python := readPythonEnum(t, tt.pythonPath)
			want := make(map[string]string, len(python))
			for name, value := range python {
				want[goConstantSuffix(name)] = value
			}
			spelled := readPairs(
				t,
				enumsPath,
				`(?m)^\t`+tt.goType+`(\w+) +`+tt.goType+` = "([^"]*)"$`,
			)

			assert.Equal(t, want, spelled)
			assert.ElementsMatch(t, slices.Collect(maps.Values(python)), tt.valid)
		})
	}
}

// TestDagRunStateMatchesGenmodels fails when regenerating genmodels from the supervisor schema
// adds, renames or removes a DagRunState constant. The DagRunState alias brings the type into
// this package but not the genmodels constants, so enums.go declares each of those constants
// again.
func TestDagRunStateMatchesGenmodels(t *testing.T) {
	generated := readPairs(
		t,
		"../pkg/execution/genmodels/models.gen.go",
		`(?m)^const DagRunState(\w+) DagRunState = "([^"]*)"$`,
	)
	declared := readPairs(t, enumsPath, `(?m)^\tDagRunState(\w+) +DagRunState = "([^"]*)"$`)

	assert.Equal(t, generated, declared)
	assert.ElementsMatch(t, slices.Collect(maps.Values(generated)), toStrings(dagRunStates))
}

func TestTaskRejectsAValueOutsideAnEnum(t *testing.T) {
	const (
		triggerRuleValues = `["all_success" "all_failed" "all_done" "all_done_min_one_success" ` +
			`"all_done_setup_success" "one_success" "one_failed" "one_done" "none_failed" ` +
			`"none_failed_min_one_success" "none_skipped" "all_skipped" "always"]`
		weightRuleValues = `["downstream" "upstream" "absolute"]`
	)
	tests := []struct {
		name   string
		fn     any
		spec   TaskSpec
		taskID string
		want   string
	}{
		{
			name:   "TriggerRule of a task named after its function",
			fn:     extract,
			spec:   TaskSpec{TriggerRule: "all_sucess"},
			taskID: "extract",
			want: `airflow.TaskSpec.TriggerRule is "all_sucess", which is not a trigger rule; ` +
				`use one of ` + triggerRuleValues,
		},
		{
			name: "WeightRule next to a valid TriggerRule",
			fn:   extract,
			spec: TaskSpec{
				TaskID: "extract_rows", TriggerRule: TriggerRuleAllDone, WeightRule: "Absolute",
			},
			taskID: "extract_rows",
			want: `airflow.TaskSpec.WeightRule is "Absolute", which is not a weight rule; ` +
				`use one of ` + weightRuleValues,
		},
		{
			name:   "WeightRule of a task from TriggerDagRun",
			fn:     TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
			spec:   TaskSpec{TaskID: "trigger_downstream", WeightRule: "Absolute"},
			taskID: "trigger_downstream",
			want: `airflow.TaskSpec.WeightRule is "Absolute", which is not a weight rule; ` +
				`use one of ` + weightRuleValues,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			assert.PanicsWithValue(t,
				fmt.Sprintf(`airflow.DagRef.Task: task %q of Dag "etl": %s`, tt.taskID, tt.want),
				func() { dag.Task(tt.fn, tt.spec) },
			)
			assert.NotPanics(t,
				func() { dag.Task(extract, TaskSpec{TaskID: tt.taskID}) },
				"a rejected task does not take its task_id",
			)
		})
	}
}

func TestTaskAcceptsEveryValueOfAnEnum(t *testing.T) {
	dag := Dag("etl")
	dag.Task(extract, TaskSpec{TaskID: "no_rules"})
	for _, rule := range triggerRules {
		dag.Task(extract, TaskSpec{TaskID: "trigger_" + string(rule), TriggerRule: rule})
	}
	for _, rule := range weightRules {
		dag.Task(extract, TaskSpec{TaskID: "weight_" + string(rule), WeightRule: rule})
	}

	assert.Len(t, dag.tasks, 1+len(triggerRules)+len(weightRules))
}
