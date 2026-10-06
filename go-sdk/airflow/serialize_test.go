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
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// serializedDag registers dag and returns the dag object of its serialized form, decoded from
// JSON so that a test can compare it with JSON.
func serializedDag(t *testing.T, dag *DagRef) map[string]any {
	t.Helper()
	Bundle().Register(dag)
	raw, err := json.Marshal(dag.serialize("/bundles/app/etl", "etl"))
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	return payload["dag"].(map[string]any)
}

// serializedTask returns the fields of the task taskID in a dag object from serializedDag.
func serializedTask(t *testing.T, dag map[string]any, taskID string) map[string]any {
	t.Helper()
	for _, task := range dag["tasks"].([]any) {
		fields := task.(map[string]any)["__var"].(map[string]any)
		if fields["task_id"] == taskID {
			return fields
		}
	}
	require.Failf(t, "no serialized task", "the serialized Dag has no task %q", taskID)
	return nil
}

// serializedGroup returns the task group groupID in a dag object from serializedDag.
func serializedGroup(t *testing.T, dag map[string]any, groupID string) map[string]any {
	t.Helper()
	var find func(group map[string]any) map[string]any
	find = func(group map[string]any) map[string]any {
		for id, child := range group["children"].(map[string]any) {
			kind, value := child.([]any)[0], child.([]any)[1]
			if kind != "taskgroup" {
				continue
			}
			if id == groupID {
				return value.(map[string]any)
			}
			if found := find(value.(map[string]any)); found != nil {
				return found
			}
		}
		return nil
	}
	found := find(dag["task_group"].(map[string]any))
	require.NotNil(t, found, "the serialized Dag has no task group %q", groupID)
	return found
}

func assertJSON(t *testing.T, want string, got any) {
	t.Helper()
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	assert.JSONEq(t, want, string(raw))
}

// withoutKeys returns a copy of fields without the given keys, to leave the fields that a test
// does not look at out of a comparison.
func withoutKeys(fields map[string]any, keys ...string) map[string]any {
	kept := maps.Clone(fields)
	for _, key := range keys {
		delete(kept, key)
	}
	return kept
}

// The fields that serializeTaskLocked writes for every Go task, whatever its TaskSpec sets.
var goTaskFields = []string{
	"task_id",
	"task_type",
	"_task_module",
	"language",
	"template_fields",
	"is_stub",
}

func TestSerializeWritesTheWholeDag(t *testing.T) {
	dag := Dag("etl")
	dag.Task(ping)
	Bundle().Register(dag)

	assertJSON(t, `{
		"__version": 3,
		"dag": {
			"dag_id": "etl",
			"fileloc": "/bundles/app/etl",
			"relative_fileloc": "etl",
			"timezone": "UTC",
			"timetable": {"__type": "airflow.timetables.simple.NullTimetable", "__var": {}},
			"tasks": [{"__type": "operator", "__var": {
				"task_id": "ping",
				"task_type": "GoOperator",
				"_task_module": "airflow.sdk.coordinators.executable",
				"language": "go",
				"template_fields": [],
				"is_stub": true
			}}],
			"dag_dependencies": [],
			"task_group": {
				"_group_id": null,
				"group_display_name": "",
				"prefix_group_id": true,
				"tooltip": "",
				"ui_color": "CornflowerBlue",
				"ui_fgcolor": "#000",
				"children": {"ping": ["operator", "ping"]},
				"upstream_group_ids": [],
				"downstream_group_ids": [],
				"upstream_task_ids": [],
				"downstream_task_ids": []
			},
			"edge_info": {},
			"params": [],
			"deadline": null,
			"allowed_run_types": null
		}
	}`, dag.serialize("/bundles/app/etl", "etl"))
}

func TestSerializePanicsForADagThatIsNotRegistered(t *testing.T) {
	dag := Dag("etl")
	dag.Task(ping)

	assert.PanicsWithValue(t,
		`airflow: Dag "etl" is not registered, so its group edges are not expanded yet`,
		func() { dag.serialize("/bundles/app/etl", "etl") },
	)
}

func TestSerializeWritesTheFieldsOfDagSpec(t *testing.T) {
	dag := Dag("etl", DagSpec{
		Catchup:                     ptr(true),
		DagDisplayName:              "ETL",
		DagrunTimeout:               90*time.Minute + 1500*time.Microsecond,
		Description:                 "loads rows",
		DisableBundleVersioning:     ptr(true),
		DocMD:                       "# ETL",
		EndDate:                     time.Date(2026, 12, 31, 23, 30, 15, 0, time.UTC),
		FailFast:                    true,
		IsPausedUponCreation:        ptr(true),
		MaxActiveRuns:               3,
		MaxActiveTasks:              8,
		MaxConsecutiveFailedDagRuns: ptr(2),
		Queue:                       "golang",
		RenderTemplateAsNativeObj:   true,
		Schedule:                    "0 3 * * *",
		StartDate: time.Date(
			2026,
			1,
			1,
			8,
			0,
			0,
			0,
			time.FixedZone("UTC+8", 8*3600),
		),
		Tags: []string{"gamma", "alpha", "gamma"},
	})
	dag.Task(ping)

	got := serializedDag(t, dag)

	assertJSON(t, `{
		"catchup": true,
		"dag_display_name": "ETL",
		"dagrun_timeout": 5400.0015,
		"description": "loads rows",
		"disable_bundle_versioning": true,
		"doc_md": "# ETL",
		"end_date": 1798759815,
		"fail_fast": true,
		"is_paused_upon_creation": true,
		"max_active_runs": 3,
		"max_active_tasks": 8,
		"max_consecutive_failed_dag_runs": 2,
		"render_template_as_native_obj": true,
		"start_date": 1767225600,
		"tags": ["alpha", "gamma"],
		"timetable": {
			"__type": "airflow.timetables.trigger.CronTriggerTimetable",
			"__var": {"expression": "0 3 * * *", "timezone": "UTC", "interval": 0, "run_immediately": false}
		}
	}`, withoutKeys(got,
		"dag_id", "fileloc", "relative_fileloc", "timezone", "tasks", "dag_dependencies",
		"task_group", "edge_info", "params", "deadline", "allowed_run_types",
	))
}

func TestSerializeWritesTheFieldsOfTaskSpec(t *testing.T) {
	dag := Dag("etl")
	dag.Task(ping, TaskSpec{
		TaskDisplayName:                  "Ping",
		DependsOnPast:                    true,
		DoXComPush:                       ptr(false),
		DocMD:                            "pings",
		EndDate:                          time.Date(2026, 11, 30, 0, 0, 0, 0, time.UTC),
		ExecutionTimeout:                 2 * time.Minute,
		Executor:                         "LocalExecutor",
		IgnoreFirstDependsOnPast:         true,
		MapIndexTemplate:                 "{{ task.task_id }}",
		MaxActiveTisPerDag:               3,
		MaxActiveTisPerDagrun:            4,
		MaxRetryDelay:                    15 * time.Minute,
		Owner:                            "data-team",
		Pool:                             "tiny",
		PoolSlots:                        ptr(2),
		PriorityWeight:                   ptr(5),
		Queue:                            "golang",
		Retries:                          2,
		RetryDelay:                       ptr(10 * time.Minute),
		RetryExponentialBackoff:          2.5,
		StartDate:                        time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		TaskID:                           "pinged",
		TriggerRule:                      TriggerRuleAllDone,
		WaitForDownstream:                true,
		WaitForPastDependsBeforeSkipping: true,
		WeightRule:                       WeightRuleUpstream,
	})

	got := serializedTask(t, serializedDag(t, dag), "pinged")

	assertJSON(t, `{
		"_task_display_name": "Ping",
		"depends_on_past": true,
		"do_xcom_push": false,
		"doc_md": "pings",
		"end_date": 1795996800,
		"execution_timeout": 120,
		"executor": "LocalExecutor",
		"ignore_first_depends_on_past": true,
		"map_index_template": "{{ task.task_id }}",
		"max_active_tis_per_dag": 3,
		"max_active_tis_per_dagrun": 4,
		"max_retry_delay": 900,
		"owner": "data-team",
		"pool": "tiny",
		"pool_slots": 2,
		"priority_weight": 5,
		"queue": "golang",
		"retries": 2,
		"retry_delay": 600,
		"retry_exponential_backoff": 2.5,
		"start_date": 1769904000,
		"trigger_rule": "all_done",
		"wait_for_downstream": true,
		"wait_for_past_depends_before_skipping": true,
		"weight_rule": "upstream"
	}`, withoutKeys(got, goTaskFields...))
}

func TestSerializeLeavesOutAFieldAtItsSchemaDefault(t *testing.T) {
	dag := Dag("etl", DagSpec{Queue: "default"})
	dag.Task(ping, TaskSpec{
		DoXComPush:     ptr(true),
		Owner:          "airflow",
		Pool:           "default_pool",
		PoolSlots:      ptr(1),
		PriorityWeight: ptr(1),
		Queue:          "default",
		RetryDelay:     ptr(300 * time.Second),
		TriggerRule:    TriggerRuleAllSuccess,
		WeightRule:     WeightRuleDownstream,
	})
	dag.Task(extract)

	got := serializedDag(t, dag)

	assert.Empty(t, withoutKeys(serializedTask(t, got, "ping"), goTaskFields...))
	assert.Empty(t, withoutKeys(serializedTask(t, got, "extract"), goTaskFields...))
}

func TestSerializeLeavesOutTheEmailFieldsOfTaskSpec(t *testing.T) {
	dag := Dag("etl")
	dag.Task(ping, TaskSpec{EmailOnFailure: ptr(false), EmailOnRetry: ptr(false)})

	got := serializedTask(t, serializedDag(t, dag), "ping")

	assert.NotContains(t, got, "email_on_failure")
	assert.NotContains(t, got, "email_on_retry")
}

func TestSerializeWritesAPointerFieldThatHoldsTheZeroValue(t *testing.T) {
	dag := Dag("etl", DagSpec{
		Catchup:                     ptr(false),
		DisableBundleVersioning:     ptr(false),
		IsPausedUponCreation:        ptr(false),
		MaxConsecutiveFailedDagRuns: ptr(0),
	})
	dag.Task(ping, TaskSpec{
		DoXComPush:     ptr(false),
		PriorityWeight: ptr(0),
		RetryDelay:     ptr(time.Duration(0)),
	})

	got := serializedDag(t, dag)

	assert.Equal(t, false, got["catchup"])
	assert.Equal(t, false, got["disable_bundle_versioning"])
	assert.Equal(t, false, got["is_paused_upon_creation"])
	assert.Equal(t, 0.0, got["max_consecutive_failed_dag_runs"])
	assertJSON(t,
		`{"do_xcom_push": false, "priority_weight": 0, "retry_delay": 0}`,
		withoutKeys(serializedTask(t, got, "ping"), goTaskFields...),
	)
}

func TestSerializeWritesTimesAndDurationsToTheMicrosecond(t *testing.T) {
	dag := Dag("etl")
	dag.Task(ping, TaskSpec{
		StartDate:        time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC),
		ExecutionTimeout: 2*time.Second + 1500*time.Nanosecond,
		// time.Time{}.In(...) is the zero Time, although it is not the zero value of time.Time.
		EndDate: time.Time{}.In(time.FixedZone("UTC+8", 8*3600)),
	})

	got := serializedTask(t, serializedDag(t, dag), "ping")

	assert.Equal(t, 1767225600.123456, got["start_date"])
	assert.Equal(t, 2.000001, got["execution_timeout"])
	assert.NotContains(t, got, "end_date")
}

func TestSerializeGivesEachGoTaskTheQueueOfTheDag(t *testing.T) {
	dag := Dag("etl", DagSpec{Queue: "golang"})
	dag.Task(extract)
	dag.Task(ping, TaskSpec{Queue: "golang_large"})
	dag.If(hasRows, Inputs(dag.Task(readRows))).Then(dag.Task(load))
	dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "reports"}), TaskSpec{TaskID: "trigger"})
	dag.Task(
		TriggerDagRun(TriggerDagRunSpec{DagID: "reports"}),
		TaskSpec{TaskID: "trigger_on_python", Queue: "python"},
	)

	got := serializedDag(t, dag)

	assert.Equal(t, "golang", serializedTask(t, got, "extract")["queue"])
	assert.Equal(t, "golang_large", serializedTask(t, got, "ping")["queue"])
	assert.Equal(t, "golang", serializedTask(t, got, "hasRows")["queue"])
	// A task from TriggerDagRun runs on a Python worker.
	assert.NotContains(t, serializedTask(t, got, "trigger"), "queue")
	assert.Equal(t, "python", serializedTask(t, got, "trigger_on_python")["queue"])
}

func TestSerializeWritesTheTimetableOfTheSchedule(t *testing.T) {
	cron := func(expression string) string {
		return fmt.Sprintf(`{
			"__type": "airflow.timetables.trigger.CronTriggerTimetable",
			"__var": {"expression": %q, "timezone": "UTC", "interval": 0, "run_immediately": false}
		}`, expression)
	}
	tests := []struct {
		schedule string
		want     string
	}{
		{"", `{"__type": "airflow.timetables.simple.NullTimetable", "__var": {}}`},
		{"@once", `{"__type": "airflow.timetables.simple.OnceTimetable", "__var": {}}`},
		{"@continuous", `{"__type": "airflow.timetables.simple.ContinuousTimetable", "__var": {}}`},
		{"@hourly", cron("0 * * * *")},
		{"@daily", cron("0 0 * * *")},
		{"@weekly", cron("0 0 * * 0")},
		{"@monthly", cron("0 0 1 * *")},
		{"@quarterly", cron("0 0 1 */3 *")},
		{"@yearly", cron("0 0 1 1 *")},
		// Python expands a preset only when it matches a key of its own table exactly, with the same
		// letter case. croniter reads any other preset when Airflow schedules the Dag.
		{"@Daily", cron("@Daily")},
		{"@annually", cron("@annually")},
		{"*/15 9-17 * * MON-FRI", cron("*/15 9-17 * * MON-FRI")},
	}
	for _, tt := range tests {
		t.Run(tt.schedule, func(t *testing.T) {
			// "@continuous" needs MaxActiveRuns to be 1.
			dag := Dag("etl", DagSpec{Schedule: tt.schedule, MaxActiveRuns: 1})
			dag.Task(ping)

			assertJSON(t, tt.want, serializedDag(t, dag)["timetable"])
		})
	}
}

func TestDagRejectsAScheduleThatIsNotACronExpressionOrAPreset(t *testing.T) {
	for _, schedule := range []string{
		"every day",
		"0 3 * *",
		"0 0 1 1 * 0 2027 2028",
		"0 0 LW * *",
		"0 3 * * every",
		"0 3 * * MON,TUESDAY",
		"@Quarterly",
		"@reboot",
		"H * * * *",
	} {
		t.Run(schedule, func(t *testing.T) {
			assert.PanicsWithValue(t,
				fmt.Sprintf(`airflow.Dag: Dag "etl": airflow.DagSpec.Schedule is %q, which is not `+
					`a cron expression or a preset; set a cron expression such as "0 3 * * *", a `+
					`preset such as "@daily", "@once" or "@continuous", or no Schedule for a Dag `+
					`that runs only when something triggers it`, schedule),
				func() { Dag("etl", DagSpec{Schedule: schedule}) },
			)
		})
	}
}

// Airflow's CronTriggerTimetable validates each schedule in this test.
func TestDagTakesTheCronExpressionsThatAirflowTakes(t *testing.T) {
	for _, schedule := range []string{
		"0 3 * * *",
		"*/5 * * * *",
		"0 0 * * * 30",
		"0 9-17/2 * * 1-5",
		"0 0 L * *",
		"0 0 15W * *",
		"0 0 * * 5#3",
		"0 0 ? * MON-FRI",
		"0 12 * JAN-MAR,DEC sun",
		"30 6 * * mon,wed,fri",
		"0 9 * * MON#2",
		"0 9 * * MON-5",
		"0 0 * * 1-FRI",
		"0 0 R * *",
		"0 0 * * * 0 *",
		"0 0 1 1 * 0 2020-2099",
		"0 0 * * L5",
		"0 0 W15 * *",
		"R(0-30) * * * *",
		"r(0-30)/5 * * * *",
		"0\t3 * * *",
		"0  3 * * *",
		"@annually",
		"@midnight",
		"@HOURLY",
		"@Daily",
	} {
		t.Run(schedule, func(t *testing.T) {
			assert.NotPanics(t, func() { Dag("etl", DagSpec{Schedule: schedule}) })
		})
	}
}

func TestDagRejectsWhatPythonsDagRejects(t *testing.T) {
	tests := []struct {
		name string
		spec DagSpec
		want string
	}{
		{
			name: "@continuous without MaxActiveRuns",
			spec: DagSpec{Schedule: "@continuous"},
			want: `airflow.DagSpec.Schedule is "@continuous", which allows one active Dag run at ` +
				`a time; set MaxActiveRuns to 1`,
		},
		{
			name: "@continuous with MaxActiveRuns of 2",
			spec: DagSpec{Schedule: "@continuous", MaxActiveRuns: 2},
			want: `airflow.DagSpec.Schedule is "@continuous", which allows one active Dag run at ` +
				`a time; set MaxActiveRuns to 1`,
		},
		{
			name: "@continuous with a negative MaxActiveRuns",
			spec: DagSpec{Schedule: "@continuous", MaxActiveRuns: -5},
			want: `airflow.DagSpec.Schedule is "@continuous", which allows one active Dag run at ` +
				`a time; set MaxActiveRuns to 1`,
		},
		{
			name: "Catchup without StartDate",
			spec: DagSpec{Schedule: "@daily", Catchup: ptr(true)},
			want: "airflow.DagSpec.Catchup is true, which needs a StartDate to catch up from; " +
				"set StartDate",
		},
		{
			name: "tag longer than 100 characters",
			spec: DagSpec{Tags: []string{"etl", strings.Repeat("ü", 101)}},
			want: fmt.Sprintf(
				"airflow.DagSpec.Tags has %q, which has 101 characters; a tag has at most 100",
				strings.Repeat("ü", 101),
			),
		},
		{
			name: "StartDate after year 9999 in UTC",
			spec: DagSpec{
				StartDate: time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("UTC-8", -8*3600)),
			},
			want: "airflow.DagSpec.StartDate is 10000-01-01T07:00:00Z in UTC; Airflow takes a time " +
				"only from year 1 to year 9999",
		},
		{
			name: "EndDate before year 1",
			spec: DagSpec{EndDate: time.Date(0, 12, 31, 0, 0, 0, 0, time.UTC)},
			want: "airflow.DagSpec.EndDate is 0000-12-31T00:00:00Z in UTC; Airflow takes a time " +
				"only from year 1 to year 9999",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				`airflow.Dag: Dag "etl": `+tt.want,
				func() { Dag("etl", tt.spec) },
			)
		})
	}
}

func TestDagTakesWhatPythonsDagTakes(t *testing.T) {
	for name, spec := range map[string]DagSpec{
		"@continuous with MaxActiveRuns of 1": {Schedule: "@continuous", MaxActiveRuns: 1},
		"Catchup with StartDate": {
			Schedule:  "@daily",
			Catchup:   ptr(true),
			StartDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		// A Dag with no Schedule has nothing to catch up on.
		"Catchup without Schedule":        {Catchup: ptr(true)},
		"Catchup false without StartDate": {Schedule: "@daily", Catchup: ptr(false)},
		"tag of 100 characters":           {Tags: []string{strings.Repeat("ü", 100)}},
		// 0001-01-01 00:00:00 UTC is the zero Time, which leaves StartDate unset, so this
		// StartDate is one second later.
		"StartDate in year 1":        {StartDate: time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)},
		"EndDate at the end of 9999": {EndDate: time.Date(9999, 12, 31, 23, 0, 0, 0, time.UTC)},
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotPanics(t, func() { Dag("etl", spec) })
		})
	}
}

func TestTaskRejectsATimeThatPythonCannotHold(t *testing.T) {
	tests := []struct {
		name string
		spec TaskSpec
		want string
	}{
		{
			name: "StartDate",
			spec: TaskSpec{StartDate: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
			want: "airflow.TaskSpec.StartDate is 10000-01-01T00:00:00Z in UTC",
		},
		{
			name: "EndDate",
			spec: TaskSpec{EndDate: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
			want: "airflow.TaskSpec.EndDate is 0000-01-01T00:00:00Z in UTC",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				`airflow.DagRef.Task: task "ping" of Dag "etl": `+tt.want+
					"; Airflow takes a time only from year 1 to year 9999",
				func() { Dag("etl").Task(ping, tt.spec) },
			)
		})
	}
}

func TestSerializeWritesInputsAsArgBindings(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))
	dag.Task(
		func(Context, int, rowSet, rowSet) error { return nil },
		Inputs(counted, read, read),
		TaskSpec{TaskID: "merge"},
	)

	got := serializedDag(t, dag)

	assertJSON(t, `[
		{"name": "arg0", "kind": "xcom", "task_id": "countRows"},
		{"name": "arg1", "kind": "xcom", "task_id": "readRows"},
		{"name": "arg2", "kind": "xcom", "task_id": "readRows"}
	]`, serializedTask(t, got, "merge")["_arg_bindings"])
	assertJSON(t, `[{"name": "arg0", "kind": "xcom", "task_id": "readRows"}]`,
		serializedTask(t, got, "countRows")["_arg_bindings"])
	// A task that takes no Inputs gets no arguments when it runs, but is still a stub task.
	assert.NotContains(t, serializedTask(t, got, "readRows"), "_arg_bindings")
	assert.Equal(t, true, serializedTask(t, got, "readRows")["is_stub"])
}

func TestSerializeWritesTheTasksInTheOrderTheyWereAdded(t *testing.T) {
	dag := Dag("etl")
	orderedTask(t, dag, "load")
	orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	groupTask(t, group, "clean")
	orderedTask(t, dag, "audit")

	got := serializedDag(t, dag)

	var taskIDs []any
	for _, task := range got["tasks"].([]any) {
		taskIDs = append(taskIDs, task.(map[string]any)["__var"].(map[string]any)["task_id"])
	}
	assert.Equal(t, []any{"load", "extract", "transform.clean", "audit"}, taskIDs)
}

func TestSerializeWritesTheDownstreamTasksOfEachTask(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")
	read.Before(notified)
	cleaned.After(read, counted)

	got := serializedDag(t, dag)

	assertJSON(t, `["cleanup", "countRows", "notify"]`,
		serializedTask(t, got, "readRows")["downstream_task_ids"])
	assertJSON(t, `["cleanup"]`, serializedTask(t, got, "countRows")["downstream_task_ids"])
	assert.NotContains(t, serializedTask(t, got, "cleanup"), "downstream_task_ids")
}

func TestSerializeWritesAConditionAsABranch(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	loaded := dag.Task(load)
	reported := dag.Task(ping)
	dag.If(hasRows, Inputs(read)).Then(loaded).Else(reported)
	dag.If(hasRows, Inputs(read), TaskSpec{TaskID: "hasRowsToo"}).Then(dag.Task(extract))

	got := serializedDag(t, dag)

	assertJSON(t, `{
		"_arg_bindings": [{"name": "arg0", "kind": "xcom", "task_id": "readRows"}],
		"_can_skip_downstream": true,
		"downstream_task_ids": ["load", "ping"]
	}`, withoutKeys(serializedTask(t, got, "hasRows"), goTaskFields...))
	// A condition with no task from Else is a branch with one task to take, not a short circuit.
	assertJSON(t, `{
		"_arg_bindings": [{"name": "arg0", "kind": "xcom", "task_id": "readRows"}],
		"_can_skip_downstream": true,
		"downstream_task_ids": ["extract"]
	}`, withoutKeys(serializedTask(t, got, "hasRowsToo"), goTaskFields...))
	assert.NotContains(t, serializedTask(t, got, "load"), "_can_skip_downstream")
}

func TestSerializeWritesASwitchAsABranch(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	loaded := dag.Task(load)
	reported := dag.Task(ping)
	dag.Switch(pickPathFromRows, Inputs(read)).Case(loaded).Case(reported)

	got := serializedDag(t, dag)

	assertJSON(t, `{
		"_arg_bindings": [{"name": "arg0", "kind": "xcom", "task_id": "readRows"}],
		"_can_skip_downstream": true,
		"downstream_task_ids": ["load", "ping"]
	}`, withoutKeys(serializedTask(t, got, "pickPathFromRows"), goTaskFields...))
	assert.NotContains(t, serializedTask(t, got, "load"), "_can_skip_downstream")
}

func TestSerializeWritesALiteralAsAnArgBinding(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(countRows, Inputs(dag.Task(readRows)))
	dag.Task(
		func(Context, int, string, any, *string, rowSet) error { return nil },
		Inputs(
			read,
			Literal("s3://bucket/out"),
			Literal(map[string]any{"limit": 10, "tags": []any{"a", nil}, "ratio": 0.5}),
			Literal(nil),
			Literal(map[string]any{"rows": []string{"a"}}),
		),
		TaskSpec{TaskID: "load"},
	)

	got := serializedDag(t, dag)

	assertJSON(t, `[
		{"name": "arg0", "kind": "xcom", "task_id": "countRows"},
		{"name": "arg1", "kind": "literal", "value": "s3://bucket/out"},
		{
			"name": "arg2",
			"kind": "literal",
			"value": {"limit": 10, "tags": ["a", null], "ratio": 0.5}
		},
		{"name": "arg3", "kind": "literal", "value": null},
		{"name": "arg4", "kind": "literal", "value": {"rows": ["a"]}}
	]`, serializedTask(t, got, "load")["_arg_bindings"])
	assertJSON(t, `["load"]`, serializedTask(t, got, "countRows")["downstream_task_ids"])
	assert.NotContains(t, serializedTask(t, got, "load"), "downstream_task_ids")
}

func TestSerializeWritesALiteralWithItsIntegersAsIntegers(t *testing.T) {
	dag := Dag("etl")
	dag.Task(keepAnything, Inputs(Literal(map[string]any{"big": int64(9007199254740993)})))

	Bundle().Register(dag)
	raw, err := msgpack.Marshal(dag.serialize("/bundles/app/etl", "etl"))
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, msgpack.Unmarshal(raw, &decoded))

	task := decoded["dag"].(map[string]any)["tasks"].([]any)[0].(map[string]any)["__var"].(map[string]any)
	binding := task["_arg_bindings"].([]any)[0].(map[string]any)
	assert.Equal(t, map[string]any{"big": int64(9007199254740993)}, binding["value"])
}

func TestSerializeSharesNothingWithALiteral(t *testing.T) {
	dag := Dag("etl")
	dag.Task(keepAnything, Inputs(Literal(map[string]any{"rows": []any{"a"}})))
	Bundle().Register(dag)

	first := dag.serialize("/bundles/app/etl", "etl")
	value := first["dag"].(map[string]any)["tasks"].([]any)[0].(map[string]any)["__var"].(map[string]any)["_arg_bindings"].([]any)[0].(map[string]any)["value"]
	value.(map[string]any)["rows"].([]any)[0] = "changed"

	assertJSON(t, `[{"name": "arg0", "kind": "literal", "value": {"rows": ["a"]}}]`,
		serializedTask(t, serializedDag(t, dag), "keepAnything")["_arg_bindings"])
}

func TestSerializeWritesTheLabelsOfEdges(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	notified := orderedTask(t, dag, "notify")
	transform := dag.TaskGroup("transform")
	groupTask(t, transform, "clean")
	publish := dag.TaskGroup("publish")
	groupTask(t, publish, "push")
	extracted.Before(Label(loaded, "rows"), notified)
	extracted.Before(Label(transform, "to transform"))
	transform.Before(Label(publish, "to publish"))
	publish.Before(Label(notified, "to notify"))

	got := serializedDag(t, dag)

	assertJSON(t, `{
		"extract": {
			"load": {"label": "rows"},
			"transform.upstream_join_id": {"label": "to transform"}
		},
		"transform.downstream_join_id": {"publish.upstream_join_id": {"label": "to publish"}},
		"publish.downstream_join_id": {"notify": {"label": "to notify"}}
	}`, got["edge_info"])
}

func TestSerializeNestsTheTaskGroups(t *testing.T) {
	dag := Dag("etl")
	orderedTask(t, dag, "extract")
	transform := dag.TaskGroup("transform", TaskGroupSpec{
		DocMD:            "# transform",
		GroupDisplayName: "Transform",
		Tooltip:          "cleans the rows",
		UIColor:          "#f0f0f0",
		UIFgColor:        "#111",
	})
	groupTask(t, transform, "clean")
	checks := transform.TaskGroup("checks", TaskGroupSpec{PrefixGroupID: ptr(false)})
	groupTask(t, checks, "nulls")

	got := serializedDag(t, dag)

	assertJSON(t, `{
		"_group_id": null,
		"group_display_name": "",
		"prefix_group_id": true,
		"tooltip": "",
		"ui_color": "CornflowerBlue",
		"ui_fgcolor": "#000",
		"children": {
			"extract": ["operator", "extract"],
			"transform": ["taskgroup", {
				"_group_id": "transform",
				"group_display_name": "Transform",
				"prefix_group_id": true,
				"tooltip": "cleans the rows",
				"ui_color": "#f0f0f0",
				"ui_fgcolor": "#111",
				"doc_md": "# transform",
				"children": {
					"transform.clean": ["operator", "transform.clean"],
					"transform.checks": ["taskgroup", {
						"_group_id": "checks",
						"group_display_name": "",
						"prefix_group_id": false,
						"tooltip": "",
						"ui_color": "CornflowerBlue",
						"ui_fgcolor": "#000",
						"children": {"nulls": ["operator", "nulls"]},
						"upstream_group_ids": [],
						"downstream_group_ids": [],
						"upstream_task_ids": [],
						"downstream_task_ids": []
					}]
				},
				"upstream_group_ids": [],
				"downstream_group_ids": [],
				"upstream_task_ids": [],
				"downstream_task_ids": []
			}]
		},
		"upstream_group_ids": [],
		"downstream_group_ids": [],
		"upstream_task_ids": [],
		"downstream_task_ids": []
	}`, got["task_group"])
}

func TestSerializeRecordsTheGroupEdgesOnTheGroups(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	staging := dag.TaskGroup("staging")
	staged := groupTask(t, staging, "stage")
	checks := staging.TaskGroup("checks")
	staged.Before(groupTask(t, checks, "nulls"))
	publish := dag.TaskGroup("publish")
	groupTask(t, publish, "push")
	loaded := orderedTask(t, dag, "load")
	extracted.Before(staging).Before(publish).Before(loaded)

	got := serializedDag(t, dag)

	edges := func(group map[string]any) map[string]any {
		return withoutKeys(group,
			"_group_id", "group_display_name", "prefix_group_id", "tooltip", "ui_color",
			"ui_fgcolor", "children",
		)
	}
	assertJSON(t, `{
		"upstream_group_ids": [],
		"downstream_group_ids": ["publish"],
		"upstream_task_ids": ["extract"],
		"downstream_task_ids": []
	}`, edges(serializedGroup(t, got, "staging")))
	assertJSON(t, `{
		"upstream_group_ids": ["staging"],
		"downstream_group_ids": [],
		"upstream_task_ids": ["staging.checks.nulls"],
		"downstream_task_ids": ["load"]
	}`, edges(serializedGroup(t, got, "publish")))
	assertJSON(t, `{
		"upstream_group_ids": [],
		"downstream_group_ids": [],
		"upstream_task_ids": [],
		"downstream_task_ids": []
	}`, edges(serializedGroup(t, got, "staging.checks")))
}

func TestSerializeRecordsTheTasksAGroupEdgeLeftFromWhenItExpanded(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	checks := transform.TaskGroup("checks")
	groupTask(t, checks, "nulls")
	cleaned := groupTask(t, transform, "clean")
	publish := dag.TaskGroup("publish")
	groupTask(t, publish, "push")
	transform.Before(publish)
	// transform.Before(publish) expands first. At that point no edge connects the two tasks of
	// transform yet, so both are last tasks of transform. Python works out a group edge when >> runs,
	// so it records the same two tasks.
	checks.Before(cleaned)

	got := serializedDag(t, dag)

	assertJSON(t, `["transform.checks.nulls", "transform.clean"]`,
		serializedGroup(t, got, "publish")["upstream_task_ids"])
	assertJSON(t, `["publish.push", "transform.clean"]`,
		serializedTask(t, got, "transform.checks.nulls")["downstream_task_ids"])
}

func TestSerializeWritesATriggerDagRunAsATriggerDagRunOperator(t *testing.T) {
	dag := Dag("etl", DagSpec{Queue: "golang"})
	dag.Task(TriggerDagRun(TriggerDagRunSpec{
		DagID:                 "downstream_etl",
		RunID:                 "{{ run_id }}",
		Conf:                  map[string]any{"rows": 2, "nested": map[string]any{"ok": true}},
		LogicalDate:           "{{ ds }}",
		RunAfter:              time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		ResetDagRun:           true,
		WaitForCompletion:     true,
		PokeInterval:          ptr(30 * time.Second),
		AllowedStates:         []DagRunState{DagRunStateSuccess, DagRunStateQueued},
		FailedStates:          []DagRunState{DagRunStateFailed},
		SkipWhenAlreadyExists: true,
		FailWhenDagIsPaused:   true,
		Note:                  "from etl",
		Deferrable:            ptr(false),
	}), TaskSpec{TaskID: "trigger", Retries: 1})

	got := serializedTask(t, serializedDag(t, dag), "trigger")

	assertJSON(t, `{
		"task_id": "trigger",
		"task_type": "TriggerDagRunOperator",
		"_task_module": "airflow.providers.standard.operators.trigger_dagrun",
		"ui_color": "#ffefeb",
		"template_fields": [
			"trigger_dag_id",
			"trigger_run_id",
			"logical_date",
			"conf",
			"wait_for_completion",
			"skip_when_already_exists"
		],
		"template_fields_renderers": {"conf": "py"},
		"_operator_extra_links": {"Triggered DAG": "_link_TriggerDagRunLink"},
		"trigger_dag_id": "downstream_etl",
		"trigger_run_id": "{{ run_id }}",
		"logical_date": "{{ ds }}",
		"conf": {"rows": 2, "nested": {"ok": true}},
		"wait_for_completion": true,
		"skip_when_already_exists": true,
		"run_after": {"__type": "datetime", "__var": 1769904000},
		"reset_dag_run": true,
		"poke_interval": 30,
		"allowed_states": ["success", "queued"],
		"failed_states": ["failed"],
		"fail_when_dag_is_paused": true,
		"note": "from etl",
		"deferrable": false,
		"retries": 1
	}`, got)
}

func TestSerializeWritesOnlyTheTriggerDagRunOptionsThatAreSet(t *testing.T) {
	dag := Dag("etl")
	dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "reports"}), TaskSpec{TaskID: "trigger"})
	dag.Task(
		TriggerDagRun(TriggerDagRunSpec{DagID: "reports", FailedStates: []DagRunState{}}),
		TaskSpec{TaskID: "trigger_never_fails"},
	)

	got := serializedDag(t, dag)

	// trigger_dag_id, logical_date, wait_for_completion and skip_when_already_exists are template
	// fields, which Python writes whatever their values.
	assertJSON(t, `{
		"trigger_dag_id": "reports",
		"logical_date": "NOTSET",
		"wait_for_completion": false,
		"skip_when_already_exists": false
	}`, withoutKeys(serializedTask(t, got, "trigger"),
		"task_id", "task_type", "_task_module", "ui_color", "template_fields",
		"template_fields_renderers", "_operator_extra_links",
	))
	// An empty FailedStates means that no Dag run state fails the task.
	assertJSON(t, `[]`, serializedTask(t, got, "trigger_never_fails")["failed_states"])
}

func TestSerializeSharesNoConfWithTheDag(t *testing.T) {
	dag := Dag("etl")
	dag.Task(TriggerDagRun(TriggerDagRunSpec{
		DagID: "reports",
		Conf:  map[string]any{"tables": []any{"rows"}, "target": map[string]any{"db": "warehouse"}},
	}), TaskSpec{TaskID: "trigger"})
	Bundle().Register(dag)
	conf := func(payload map[string]any) map[string]any {
		tasks := payload["dag"].(map[string]any)["tasks"].([]any)
		return tasks[0].(map[string]any)["__var"].(map[string]any)["conf"].(map[string]any)
	}

	first := conf(dag.serialize("/bundles/app/etl", "etl"))
	first["tables"].([]any)[0] = "changed"
	first["target"].(map[string]any)["db"] = "changed"
	first["added"] = true

	assert.Equal(t,
		map[string]any{"tables": []any{"rows"}, "target": map[string]any{"db": "warehouse"}},
		conf(dag.serialize("/bundles/app/etl", "etl")),
	)
}

func TestSerializeWritesADagDependencyForEachTriggeredDag(t *testing.T) {
	dag := Dag("etl")
	orderedTask(t, dag, "extract")
	dag.Task(
		TriggerDagRun(TriggerDagRunSpec{DagID: "reports"}),
		TaskSpec{TaskID: "trigger_reports", TaskDisplayName: "Trigger the reports"},
	)
	dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "audit"}), TaskSpec{TaskID: "trigger_audit_b"})
	dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "audit"}), TaskSpec{TaskID: "trigger_audit_a"})

	got := serializedDag(t, dag)

	assertJSON(t, `[
		{
			"source": "etl",
			"target": "audit",
			"label": "trigger_audit_a",
			"dependency_type": "trigger",
			"dependency_id": "trigger_audit_a"
		},
		{
			"source": "etl",
			"target": "audit",
			"label": "trigger_audit_b",
			"dependency_type": "trigger",
			"dependency_id": "trigger_audit_b"
		},
		{
			"source": "etl",
			"target": "reports",
			"label": "Trigger the reports",
			"dependency_type": "trigger",
			"dependency_id": "trigger_reports"
		}
	]`, got["dag_dependencies"])
}

// Airflow receives a serialized Dag from a bundle as msgpack, and msgpack writes a json.Number as a
// string.
func TestSerializeKeepsTheNumbersOfConfNumbersInMsgpack(t *testing.T) {
	dag := Dag("etl")
	dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "reports", Conf: map[string]any{
		"int":             2,
		"negative":        -3,
		"beyond_a_float":  uint64(1<<53 + 1),
		"beyond_an_int64": uint64(1<<63 + 1),
		"float":           1.5,
		"list":            []any{1, "two"},
		"nested":          map[string]any{"seven": 7},
	}}), TaskSpec{TaskID: "trigger"})
	Bundle().Register(dag)

	raw, err := msgpack.Marshal(dag.serialize("/bundles/app/etl", "etl"))
	require.NoError(t, err)
	dec := msgpack.NewDecoder(bytes.NewReader(raw))
	dec.UseLooseInterfaceDecoding(true)
	var payload map[string]any
	require.NoError(t, dec.Decode(&payload))

	tasks := payload["dag"].(map[string]any)["tasks"].([]any)
	conf := tasks[0].(map[string]any)["__var"].(map[string]any)["conf"].(map[string]any)
	for key, want := range map[string]string{
		"int":             "2",
		"negative":        "-3",
		"beyond_a_float":  "9007199254740993",
		"beyond_an_int64": "9223372036854775809",
		"float":           "1.5",
	} {
		assert.NotEqual(t, reflect.String, reflect.ValueOf(conf[key]).Kind(), key)
		assert.Equal(t, want, fmt.Sprint(conf[key]), key)
	}
	assert.NotEqual(t, reflect.String, reflect.ValueOf(conf["list"].([]any)[0]).Kind())
	assert.Equal(t, "two", conf["list"].([]any)[1])
	assert.NotEqual(t, reflect.String,
		reflect.ValueOf(conf["nested"].(map[string]any)["seven"]).Kind())
}

func TestGeneratedSpecFieldsNameEveryFieldOfTheirStruct(t *testing.T) {
	for _, tt := range []struct {
		spec   any
		fields map[string]schemaField
	}{
		{DagSpec{}, dagSpecFields},
		{TaskSpec{}, taskSpecFields},
		{TaskGroupSpec{}, taskGroupSpecFields},
	} {
		specType := reflect.TypeOf(tt.spec)
		names := make([]string, specType.NumField())
		for i := range names {
			names[i] = specType.Field(i).Name
		}
		assert.ElementsMatch(t, names, slices.Collect(maps.Keys(tt.fields)), specType.Name())
	}
}

func TestSpecRulesSkipAndSetOnlyNamedFields(t *testing.T) {
	for name, rules := range map[string]specRules{
		"dag":        dagSpecRules,
		"task":       taskSpecRules,
		"task group": taskGroupSpecRules,
	} {
		for field := range rules.skip {
			assert.Contains(t, rules.fields, field, "%s skip", name)
		}
		for field := range rules.set {
			assert.Contains(t, rules.fields, field, "%s set", name)
		}
	}
}

func TestWriteSpecFieldsPanicsForAFieldWithoutARule(t *testing.T) {
	fields := maps.Clone(taskGroupSpecFields)
	delete(fields, "Tooltip")

	assert.PanicsWithValue(t,
		"airflow: the serializer has no schema field for TaskGroupSpec.Tooltip",
		func() { writeSpecFields(map[string]any{}, TaskGroupSpec{}, specRules{fields: fields}) },
	)
}

func TestWriteSpecFieldsPanicsForAFieldOfATypeItCannotWrite(t *testing.T) {
	type spec struct{ Count uint }

	assert.PanicsWithValue(t,
		"airflow: the serializer cannot write a spec field of type uint",
		func() {
			writeSpecFields(
				map[string]any{},
				spec{Count: 1},
				specRules{fields: map[string]schemaField{"Count": {key: "count"}}},
			)
		},
	)
}
