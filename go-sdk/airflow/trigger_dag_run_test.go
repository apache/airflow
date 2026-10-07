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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

func TestTriggerDagRunIsATask(t *testing.T) {
	dag := Dag("etl")
	gate := dag.Task(extract)
	spec := TriggerDagRunSpec{
		DagID:                 "downstream_etl",
		RunID:                 "{{ run_id }}_downstream",
		Conf:                  map[string]any{"source": "etl"},
		LogicalDate:           "{{ ds }}",
		RunAfter:              time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
		ResetDagRun:           true,
		WaitForCompletion:     true,
		PokeInterval:          ptr(30 * time.Second),
		AllowedStates:         []DagRunState{DagRunStateSuccess, DagRunStateFailed},
		FailedStates:          []DagRunState{DagRunStateQueued, DagRunStateRunning},
		SkipWhenAlreadyExists: true,
		FailWhenDagIsPaused:   true,
		Note:                  "triggered by etl",
		Deferrable:            ptr(true),
	}

	task := dag.Task(TriggerDagRun(spec), TaskSpec{TaskID: "trigger_downstream"})

	assert.Equal(t, "trigger_downstream", task.taskID)
	assert.Equal(t, TaskSpec{TaskID: "trigger_downstream"}, task.spec)
	require.NotNil(t, task.triggerDagRun)
	assert.Equal(t, spec, *task.triggerDagRun)
	assert.Equal(t, []*TaskRef{gate, task}, dag.tasks)
	assert.Nil(t, gate.triggerDagRun, "a task that runs a Go function is not a TriggerDagRun task")
}

func TestTriggerDagRunKeepsNilApartFromZero(t *testing.T) {
	dag := Dag("etl")

	unset := TriggerDagRunSpec{DagID: "downstream_etl"}
	task := dag.Task(TriggerDagRun(unset), TaskSpec{TaskID: "unset"})
	assert.Equal(t, unset, *task.triggerDagRun)

	// nil keeps the Python default. An empty slice or a pointer to 0 or false is a value the task
	// uses instead. For example, an empty FailedStates means that no state fails the task.
	zero := TriggerDagRunSpec{
		DagID:         "downstream_etl",
		Conf:          map[string]any{},
		PokeInterval:  ptr(time.Duration(0)),
		AllowedStates: []DagRunState{},
		FailedStates:  []DagRunState{},
		Deferrable:    ptr(false),
	}
	task = dag.Task(TriggerDagRun(zero), TaskSpec{TaskID: "zero"})
	assert.Equal(t, zero, *task.triggerDagRun)
}

func TestTriggerDagRunNeedsATaskID(t *testing.T) {
	want := `airflow.DagRef.Task: Dag "etl": a task from airflow.TriggerDagRun with DagID ` +
		`"downstream_etl" has no Go function to take a task_id from; ` +
		`set one with airflow.TaskSpec{TaskID: ...}`
	trigger := TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"})

	assert.PanicsWithValue(t, want, func() { Dag("etl").Task(trigger) })
	assert.PanicsWithValue(t, want, func() { Dag("etl").Task(trigger, TaskSpec{}) })
	assert.PanicsWithValue(t, want, func() { Dag("etl").Task(trigger, Inputs(), Inputs()) })
}

// buildNestedConf returns a conf in which maps nest depth levels deep. encoding/json decodes at
// most 10000 levels of nesting.
func buildNestedConf(depth int) map[string]any {
	conf := map[string]any{}
	for range depth - 1 {
		conf = map[string]any{"nested": conf}
	}
	return conf
}

func TestTriggerDagRunRejectsAnInvalidSpec(t *testing.T) {
	tests := []struct {
		name    string
		trigger TriggerDagRunTask
		want    string
	}{
		{
			name:    "no DagID",
			trigger: TriggerDagRun(TriggerDagRunSpec{WaitForCompletion: true}),
			want:    "airflow.TriggerDagRunSpec has no DagID",
		},
		{
			name: "negative PokeInterval",
			trigger: TriggerDagRun(
				TriggerDagRunSpec{DagID: "downstream_etl", PokeInterval: ptr(-time.Second)},
			),
			want: "airflow.TriggerDagRunSpec.PokeInterval is -1s; " +
				"it must be a whole number of seconds and not negative",
		},
		{
			name: "PokeInterval with a fraction of a second",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl", PokeInterval: ptr(1500 * time.Millisecond),
			}),
			want: "airflow.TriggerDagRunSpec.PokeInterval is 1.5s; " +
				"it must be a whole number of seconds and not negative",
		},
		{
			name: "unknown state in AllowedStates",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl", AllowedStates: []DagRunState{DagRunStateSuccess, "SUCCESS"},
			}),
			want: `airflow.TriggerDagRunSpec.AllowedStates has "SUCCESS", which is not a Dag ` +
				`run state; use one of ["queued" "running" "success" "failed"]`,
		},
		{
			name: "unknown state in FailedStates",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl", FailedStates: []DagRunState{"skipped"},
			}),
			want: `airflow.TriggerDagRunSpec.FailedStates has "skipped", which is not a Dag ` +
				`run state; use one of ["queued" "running" "success" "failed"]`,
		},
		{
			name: "Conf that JSON cannot hold",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl", Conf: map[string]any{"done": make(chan struct{})},
			}),
			want: "airflow.TriggerDagRunSpec.Conf: json: unsupported type: chan struct {}",
		},
		{
			name: "RunAfter after year 9999 in UTC",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl", RunAfter: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
			}),
			want: "airflow.TriggerDagRunSpec.RunAfter is 10000-01-01T00:00:00Z in UTC; " +
				"Airflow takes a time only from year 1 to year 9999",
		},
		{
			name: "Conf with an integer that does not fit in 64 bits",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl",
				Conf: map[string]any{
					"rows": []any{1, json.Number("18446744073709551616")},
					"zeta": json.Number("-9223372036854775809"),
				},
			}),
			want: `airflow.TriggerDagRunSpec.Conf: the integer at ["rows"][1] is ` +
				"18446744073709551616, which does not fit in 64 bits",
		},
		{
			name: "Conf with a number past the range of a float64",
			trigger: TriggerDagRun(TriggerDagRunSpec{
				DagID: "downstream_etl",
				Conf:  map[string]any{"nested": map[string]any{"huge": json.Number("1e400")}},
			}),
			want: `airflow.TriggerDagRunSpec.Conf: the number at ["nested"]["huge"] is 1e400, ` +
				"which is past the range of a float64",
		},
		{
			name: "Conf nested deeper than encoding/json decodes",
			trigger: TriggerDagRun(
				TriggerDagRunSpec{DagID: "downstream_etl", Conf: buildNestedConf(10001)},
			),
			want: "airflow.TriggerDagRunSpec.Conf: invalid character '{' exceeded max depth",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			assert.PanicsWithValue(t,
				`airflow.DagRef.Task: task "trigger_downstream" of Dag "etl": `+tt.want,
				func() { dag.Task(tt.trigger, TaskSpec{TaskID: "trigger_downstream"}) },
			)
			assert.NotPanics(t,
				func() { dag.Task(extract, TaskSpec{TaskID: "trigger_downstream"}) },
				"a rejected task does not take its task_id",
			)
		})
	}
}

func TestTriggerDagRunHoldsEachNumberOfConfInATypeThatFits(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{"int", 2, int64(2)},
		{"negative int", -3, int64(-3)},
		{"uint64 past an int64", uint64(1<<63 + 1), uint64(1<<63 + 1)},
		{"float", 1.5, 1.5},
		// encoding/json writes each of these floats as digits with no point, the way it writes an
		// integer.
		{"float past a uint64", 1.5e20, 1.5e20},
		{"float past an int64", -1e19, -1e19},
		{"float32", float32(1e20), 1e20},
		{"exponent in upper case", json.Number("1E3"), 1000.0},
		{"number too small for a float64", json.Number("1e-400"), 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := Dag("etl").Task(
				TriggerDagRun(TriggerDagRunSpec{
					DagID: "downstream_etl",
					Conf:  map[string]any{"value": []any{tt.value}},
				}),
				TaskSpec{TaskID: "trigger_downstream"},
			)

			assert.Equal(t, []any{tt.want}, task.triggerDagRun.Conf["value"])
		})
	}
}

func TestTriggerDagRunReportsTheSameBadNumberEveryTime(t *testing.T) {
	conf := make(map[string]any)
	for _, key := range []string{"h", "c", "f", "a", "e", "b", "g", "d"} {
		conf[key] = json.Number("18446744073709551616")
	}
	// Go visits the keys of a map in a random order. Task still reports the bad number under the
	// first key in sorted order.
	for range 20 {
		assert.PanicsWithValue(t,
			`airflow.DagRef.Task: task "trigger_downstream" of Dag "etl": `+
				`airflow.TriggerDagRunSpec.Conf: the integer at ["a"] is 18446744073709551616, `+
				"which does not fit in 64 bits",
			func() {
				Dag("etl").Task(
					TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl", Conf: conf}),
					TaskSpec{TaskID: "trigger_downstream"},
				)
			},
		)
	}
}

func TestTriggerDagRunCopiesTheSpec(t *testing.T) {
	nested := map[string]any{"table": "rows"}
	allowed := []DagRunState{DagRunStateSuccess}
	failed := []DagRunState{DagRunStateFailed}
	poke := 30 * time.Second
	deferrable := true
	spec := TriggerDagRunSpec{
		DagID: "downstream_etl",
		// A float64 cannot hold 2^53 + 1 exactly.
		Conf:          map[string]any{"target": nested, "batch": int64(9007199254740993)},
		PokeInterval:  &poke,
		AllowedStates: allowed,
		FailedStates:  failed,
		Deferrable:    &deferrable,
	}
	task := Dag("etl").Task(TriggerDagRun(spec), TaskSpec{TaskID: "trigger_downstream"})

	nested["table"] = "changed"
	spec.Conf["added"] = true
	allowed[0] = DagRunStateRunning
	failed[0] = DagRunStateQueued
	poke = time.Minute
	deferrable = false

	stored := task.triggerDagRun
	assert.Equal(t,
		map[string]any{
			"target": map[string]any{"table": "rows"},
			"batch":  int64(9007199254740993),
		},
		stored.Conf,
	)
	assert.Equal(t, []DagRunState{DagRunStateSuccess}, stored.AllowedStates)
	assert.Equal(t, []DagRunState{DagRunStateFailed}, stored.FailedStates)
	assert.Equal(t, 30*time.Second, *stored.PokeInterval)
	assert.True(t, *stored.Deferrable)
}

// taskAdder adds a task to dag when encoding/json marshals it.
type taskAdder struct{ dag *DagRef }

func (a taskAdder) MarshalJSON() ([]byte, error) {
	a.dag.Task(extract)
	return []byte(`"added"`), nil
}

func TestTriggerDagRunMarshalsConfOutsideTheLock(t *testing.T) {
	dag := Dag("etl")
	spec := TriggerDagRunSpec{
		DagID: "downstream_etl",
		Conf:  map[string]any{"adder": taskAdder{dag: dag}},
	}
	added := make(chan *TaskRef, 1)
	go func() { added <- dag.Task(TriggerDagRun(spec), TaskSpec{TaskID: "trigger_downstream"}) }()

	select {
	case task := <-added:
		assert.Equal(t, map[string]any{"adder": "added"}, task.triggerDagRun.Conf)
		assert.Len(t, dag.tasks, 2)
	case <-time.After(5 * time.Second):
		t.Fatal("DagRef.Task held the lock of the Dag while it marshaled Conf")
	}
}

func TestTriggerDagRunTakesNoInputs(t *testing.T) {
	dag := Dag("etl")
	extracted := dag.Task(readRows)
	trigger := TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"})
	want := `airflow.DagRef.Task: task "trigger_downstream" of Dag "etl" comes from ` +
		`airflow.TriggerDagRun and takes no airflow.Inputs, because it has no Go function ` +
		`to pass the results to`

	assert.PanicsWithValue(t, want, func() {
		dag.Task(trigger, TaskSpec{TaskID: "trigger_downstream"}, Inputs(extracted))
	})
	assert.PanicsWithValue(t, want, func() {
		dag.Task(trigger, TaskSpec{TaskID: "trigger_downstream"}, Inputs())
	})
	assert.PanicsWithValue(t, want, func() {
		dag.Task(trigger, TaskSpec{TaskID: "trigger_downstream"}, Inputs(extracted), Inputs())
	})
	assert.NotPanics(t,
		func() { dag.Task(trigger, TaskSpec{TaskID: "trigger_downstream"}) },
		"a rejected task does not take its task_id",
	)
}

func TestTriggerDagRunHasNoResultForInputs(t *testing.T) {
	dag := Dag("etl")
	trigger := dag.Task(
		TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
		TaskSpec{TaskID: "trigger_downstream"},
	)

	assert.PanicsWithValue(t,
		`airflow.DagRef.Task: task "countRows" of Dag "etl" takes parameter 1 from task `+
			`"trigger_downstream", but that task comes from airflow.TriggerDagRun and returns `+
			`no result`,
		func() { dag.Task(countRows, Inputs(trigger)) },
	)
}
