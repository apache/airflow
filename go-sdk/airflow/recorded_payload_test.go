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
	"encoding/json"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedPayloadPath is the path of a fixture of the Airflow core tests. For each serialized Dag
// in the file, those tests run DagSerialization.validate_serialized_dag and put the Dag in a Dag
// bag. If a change to Airflow stops Airflow from loading what this SDK writes, those tests fail.
const recordedPayloadPath = "../../airflow-core/tests/unit/dag_processing/" +
	"lang_sdk_fixtures/go_native.json"

var updateRecordedPayload = flag.Bool(
	"update-recorded-payload", false, "rewrite "+recordedPayloadPath,
)

// recordedDags builds the Dags of the fixture. They also use constructs that the Dags of
// scripts/ci/lang_sdk_serialization/test_dags.yaml leave out, such as dag.If, TriggerDagRun and
// Label.
func recordedDags() []*DagRef {
	dag := Dag("go_native", DagSpec{
		Schedule:                    "@daily",
		StartDate:                   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndDate:                     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		Catchup:                     ptr(false),
		DagDisplayName:              "Go native",
		Description:                 "the constructs that the Go SDK serializes",
		DocMD:                       "# Go native",
		DagrunTimeout:               2 * time.Hour,
		MaxActiveRuns:               2,
		MaxActiveTasks:              8,
		MaxConsecutiveFailedDagRuns: ptr(3),
		IsPausedUponCreation:        ptr(true),
		Tags:                        []string{"native", "go"},
		Queue:                       "golang",
	})
	extracted := dag.Task(readRows, TaskSpec{
		Retries:          2,
		RetryDelay:       ptr(time.Minute),
		ExecutionTimeout: 10 * time.Minute,
		Owner:            "data-team",
		TaskDisplayName:  "Read the rows",
	})
	counted := dag.Task(countRows, Inputs(extracted), TaskSpec{
		PriorityWeight: ptr(5),
		WeightRule:     WeightRuleUpstream,
	})
	transform := dag.TaskGroup("transform", TaskGroupSpec{
		DocMD:            "# transform",
		GroupDisplayName: "Transform",
		Tooltip:          "cleans the rows",
		UIColor:          "#f0f0f0",
	})
	cleaned := transform.Task(cleanRows)
	checks := transform.TaskGroup("checks", TaskGroupSpec{PrefixGroupID: ptr(false)})
	cleaned.Before(checks.Task(ping, TaskSpec{TaskID: "nulls"}))
	publish := dag.TaskGroup("publish")
	publish.Task(ping, TaskSpec{TaskID: "push"})
	loaded := dag.Task(load)
	reported := dag.Task(extract, TaskSpec{TaskID: "report_empty"})
	dag.If(hasRows, Inputs(extracted)).Then(loaded).Else(reported)
	joined := dag.Task(ExtractRows, TaskSpec{
		TaskID:      "join",
		TriggerRule: TriggerRuleNoneFailedMinOneSuccess,
	})
	joined.After(loaded, reported)
	triggered := dag.Task(TriggerDagRun(TriggerDagRunSpec{
		DagID:                 "go_native_once",
		RunID:                 "triggered_{{ run_id }}",
		Conf:                  map[string]any{"rows": 2, "ratio": 0.5},
		LogicalDate:           "{{ ds }}",
		RunAfter:              time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		ResetDagRun:           true,
		WaitForCompletion:     true,
		PokeInterval:          ptr(30 * time.Second),
		AllowedStates:         []DagRunState{DagRunStateSuccess},
		FailedStates:          []DagRunState{},
		SkipWhenAlreadyExists: true,
		FailWhenDagIsPaused:   true,
		Note:                  "triggered by go_native",
		Deferrable:            ptr(true),
	}), TaskSpec{TaskID: "trigger_once"})
	untimed := dag.Task(
		TriggerDagRun(TriggerDagRunSpec{DagID: "go_native_manual"}),
		TaskSpec{TaskID: "trigger_manual"},
	)
	extracted.Before(Label(transform, "rows to clean"))
	transform.Before(Label(publish, "cleaned rows"))
	transform.Before(Label(joined, "cleaned"))
	counted.Before(Label(triggered, "counted"))
	joined.Before(triggered, untimed)

	once := Dag("go_native_once", DagSpec{Schedule: "@once"})
	once.Task(ping)
	continuous := Dag("go_native_continuous", DagSpec{Schedule: "@continuous", MaxActiveRuns: 1})
	continuous.Task(ping)
	manual := Dag("go_native_manual")
	manual.Task(ping)
	// The sixth field of the cron expression gives the seconds.
	seconds := Dag("go_native_seconds", DagSpec{Schedule: "30 3 * * MON-FRI 15"})
	seconds.Task(ping)
	return []*DagRef{dag, once, continuous, manual, seconds}
}

// TestSerializeMatchesTheRecordedPayload fails when the serializer writes something other than what
// the fixture holds. After a change to the serializer, rewrite the fixture with
//
//	go test ./airflow -run '^TestSerializeMatchesTheRecordedPayload$' -update-recorded-payload
//
// and the Airflow core tests check that Airflow loads the new payload.
func TestSerializeMatchesTheRecordedPayload(t *testing.T) {
	bundle := Bundle()
	serialized := make(map[string]any)
	for _, dag := range recordedDags() {
		bundle.Register(dag)
		serialized[dag.dagID] = dag.serialize("/bundles/app/etl", "etl")
	}
	want, err := json.MarshalIndent(serialized, "", "  ")
	require.NoError(t, err)
	want = append(want, '\n')
	if *updateRecordedPayload {
		require.NoError(t, os.WriteFile(recordedPayloadPath, want, 0o644))
	}

	got, err := os.ReadFile(recordedPayloadPath)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got),
		"the serializer no longer writes what %s holds; rewrite it with -update-recorded-payload",
		recordedPayloadPath,
	)
}
