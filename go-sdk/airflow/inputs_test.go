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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

// A function passed to TaskHandler binds the fields of a struct with an arg tag by name, and
// rejects such a struct next to another parameter. The tests use the tag to show that a task
// added with DagRef.Task takes the whole struct, whether or not the struct is the only parameter
// of the task function.
type rowSet struct {
	Rows []string `json:"rows" arg:"rows"`
}

type names []string

func readRows(Context) (rowSet, error)                 { return rowSet{}, nil }
func readRowsByPointer(Context) (*rowSet, error)       { return &rowSet{}, nil }
func readAnything(Context) (any, error)                { return nil, nil }
func readNames(Context) ([]string, error)              { return nil, nil }
func countRows(Context, rowSet) (int, error)           { return 0, nil }
func countRowsByPointer(Context, *rowSet) (int, error) { return 0, nil }
func compareRows(Context, rowSet, rowSet) error        { return nil }
func mergeRows(Context, rowSet, int) error             { return nil }
func report(Context, int) error                        { return nil }
func average(Context, float64) error                   { return nil }
func sumCounts(Context, []int) error                   { return nil }
func keepCounts(Context, map[string]int) error         { return nil }
func keepAnything(Context, any) error                  { return nil }
func keepNames(Context, names) error                   { return nil }
func ping(Context) error                               { return nil }

func assertInputs(t *testing.T, task *TaskRef, want ...*TaskRef) {
	t.Helper()
	require.Len(t, task.inputs, len(want))
	for i := range want {
		assert.Same(t, want[i], task.inputs[i], "input %d", i)
	}
}

func TestInputsRecordTheUpstreamTasks(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))
	merged := dag.Task(mergeRows, Inputs(read, counted))
	compared := dag.Task(compareRows, Inputs(read, read))

	assert.Empty(t, read.inputs)
	assertInputs(t, counted, read)
	assertInputs(t, merged, read, counted)
	assertInputs(t, compared, read, read)
}

func TestTaskCopiesTheInputs(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	tasks := []*TaskRef{read}
	counted := dag.Task(countRows, Inputs(tasks...))

	tasks[0] = counted
	assertInputs(t, counted, read)
}

type resultsByTask struct {
	sdk.Client
	results map[string]any
}

func (c resultsByTask) GetXCom(
	_ context.Context,
	_, _, taskID string,
	_ *int,
	_ string,
	_ any,
) (any, error) {
	return c.results[taskID], nil
}

// runWithInputs runs task through its Execute method, as the runtime runs a task with arguments.
// It passes one XCom binding per input, in the order that Inputs lists them. results maps the
// task_id of each upstream task to its result, and argNames holds the name of each binding.
func runWithInputs(t *testing.T, task *TaskRef, results map[string]any, argNames ...string) {
	t.Helper()
	require.Len(t, argNames, len(task.inputs))
	args := make([]binding.Arg, len(task.inputs))
	for i, upstream := range task.inputs {
		args[i] = binding.XComArg{Kind: "xcom", Name: argNames[i], TaskID: upstream.taskID}
	}
	ti := sdk.TaskInstance{DagID: "etl", RunID: "run1", TaskID: task.taskID}
	ctx := context.WithValue(
		context.Background(),
		sdkcontext.SdkClientContextKey,
		sdk.Client(resultsByTask{results: results}),
	)
	ctx = context.WithValue(
		ctx,
		sdkcontext.RuntimeContextKey,
		sdk.NewTIRunContext(context.Background(), ti, sdk.DagRun{DagID: "etl", RunID: "run1"}),
	)

	require.NoError(t, task.task.Execute(ctx, discardLogger(), args))
}

func TestInputsFillTheParametersInOrder(t *testing.T) {
	var gotRows rowSet
	var gotCount int
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))
	merged := dag.Task(
		func(_ Context, rows rowSet, count int) error {
			gotRows, gotCount = rows, count
			return nil
		},
		Inputs(read, counted),
		TaskSpec{TaskID: "merge"},
	)

	runWithInputs(t, merged, map[string]any{
		"readRows":  map[string]any{"rows": []any{"a", "b"}},
		"countRows": 2,
	}, "rows", "count")

	assert.Equal(t, rowSet{Rows: []string{"a", "b"}}, gotRows)
	assert.Equal(t, 2, gotCount)
}

func TestInputsFillASoleStructWithTheWholeResult(t *testing.T) {
	var got rowSet
	dag := Dag("etl")
	read := dag.Task(readRows)
	kept := dag.Task(
		func(_ Context, rows rowSet) error {
			got = rows
			return nil
		},
		Inputs(read),
		TaskSpec{TaskID: "keep"},
	)

	// The binding has the same name as the arg tag of rowSet.Rows. A function passed to
	// TaskHandler would decode the whole result into that one field.
	runWithInputs(t, kept, map[string]any{
		"readRows": map[string]any{"rows": []any{"a"}},
	}, "rows")

	assert.Equal(t, rowSet{Rows: []string{"a"}}, got)
}

func TestTaskPanicsWhenInputsDoNotMatchTheParameterCount(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))

	tests := []struct {
		name string
		add  func()
		want string
	}{
		{
			name: "no Inputs",
			add:  func() { dag.Task(report) },
			want: `task "report" of Dag "etl" has 1 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes no task`,
		},
		{
			name: "empty Inputs",
			add:  func() { dag.Task(report, Inputs()) },
			want: `task "report" of Dag "etl" has 1 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes no task`,
		},
		{
			name: "more tasks than parameters",
			add:  func() { dag.Task(report, Inputs(counted, read)) },
			want: `task "report" of Dag "etl" has 1 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes 2 task(s): "countRows", "readRows"`,
		},
		{
			name: "no parameter to fill",
			add:  func() { dag.Task(ping, Inputs(read)) },
			want: `task "ping" of Dag "etl" has 0 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes 1 task(s): "readRows"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t, "airflow.DagRef.Task: "+tt.want, tt.add)
		})
	}

	assert.Len(t, dag.tasks, 2, "a task that fails a check is not added")
	assert.NotPanics(t,
		func() { dag.Task(report, Inputs(counted)) },
		"a task that fails a check does not take its task_id",
	)
}

func TestTaskPanicsWhenAnInputHasTheWrongType(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	readByPointer := dag.Task(readRowsByPointer)
	anything := dag.Task(readAnything)
	pinged := dag.Task(ping)
	counted := dag.Task(countRows, Inputs(read), TaskSpec{TaskID: "count"})
	named := dag.Task(readNames)
	// Each function declares its own type row, so the two types have the same name.
	readRow := func() any {
		type row struct{ N int }
		return func(Context) (row, error) { return row{}, nil }
	}()
	keepRow := func() any {
		type row struct{ N int }
		return func(Context, row) error { return nil }
	}()
	rowRead := dag.Task(readRow, TaskSpec{TaskID: "read_row"})

	tests := []struct {
		name string
		add  func()
		want string
	}{
		{
			name: "result of another type",
			add:  func() { dag.Task(report, Inputs(read)) },
			want: `task "report" of Dag "etl" takes parameter 1 from task "readRows", ` +
				`but that task returns airflow.rowSet, which cannot be assigned to int`,
		},
		{
			name: "result that Go can only convert",
			add:  func() { dag.Task(average, Inputs(counted)) },
			want: `task "average" of Dag "etl" takes parameter 1 from task "count", ` +
				`but that task returns int, which cannot be assigned to float64`,
		},
		{
			name: "mismatch in a later parameter",
			add:  func() { dag.Task(mergeRows, Inputs(read, read)) },
			want: `task "mergeRows" of Dag "etl" takes parameter 2 from task "readRows", ` +
				`but that task returns airflow.rowSet, which cannot be assigned to int`,
		},
		{
			name: "pointer result for a value parameter",
			add:  func() { dag.Task(countRows, Inputs(readByPointer)) },
			want: `task "countRows" of Dag "etl" takes parameter 1 from task ` +
				`"readRowsByPointer", but that task returns *airflow.rowSet, ` +
				`which cannot be assigned to airflow.rowSet`,
		},
		{
			name: "value result for a pointer parameter",
			add:  func() { dag.Task(countRowsByPointer, Inputs(read)) },
			want: `task "countRowsByPointer" of Dag "etl" takes parameter 1 from task ` +
				`"readRows", but that task returns airflow.rowSet, ` +
				`which cannot be assigned to *airflow.rowSet`,
		},
		{
			name: "result of type any",
			add:  func() { dag.Task(countRows, Inputs(anything)) },
			want: `task "countRows" of Dag "etl" takes parameter 1 from task "readAnything", ` +
				`but that task returns interface {}, which cannot be assigned to airflow.rowSet`,
		},
		{
			name: "slice parameter",
			add:  func() { dag.Task(sumCounts, Inputs(named)) },
			want: `task "sumCounts" of Dag "etl" takes parameter 1 from task "readNames", ` +
				`but that task returns []string, which cannot be assigned to []int`,
		},
		{
			name: "map parameter",
			add:  func() { dag.Task(keepCounts, Inputs(read)) },
			want: `task "keepCounts" of Dag "etl" takes parameter 1 from task "readRows", ` +
				`but that task returns airflow.rowSet, which cannot be assigned to map[string]int`,
		},
		{
			name: "another type with the same name",
			add:  func() { dag.Task(keepRow, Inputs(rowRead), TaskSpec{TaskID: "keep_row"}) },
			want: `task "keep_row" of Dag "etl" takes parameter 1 from task "read_row", ` +
				`but that task returns airflow.row, which cannot be assigned to airflow.row, ` +
				`a different type with the same name`,
		},
		{
			name: "no result",
			add:  func() { dag.Task(report, Inputs(pinged)) },
			want: `task "report" of Dag "etl" takes parameter 1 from task "ping", ` +
				`but that task returns only an error`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t, "airflow.DagRef.Task: "+tt.want, tt.add)
		})
	}
}

func TestInputsTakeAResultThatGoCanAssign(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	named := dag.Task(readNames)

	assert.NotPanics(t, func() { dag.Task(keepAnything, Inputs(read)) })
	assert.NotPanics(t,
		func() { dag.Task(keepNames, Inputs(named)) },
		"a []string can be assigned to a named slice type",
	)
}

func TestTaskPanicsOnAnInputFromOutsideTheDag(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	readCopy := *read
	fromReports := Dag("reports").Task(readRows)
	fromAnotherEtl := Dag("etl").Task(readRows)

	tests := []struct {
		name   string
		inputs TaskOption
		want   string
	}{
		{
			name:   "nil",
			inputs: Inputs(read, nil),
			want: `task "mergeRows" of Dag "etl": ` +
				`airflow.Inputs got a nil *airflow.TaskRef at index 1`,
		},
		{
			name:   "zero TaskRef",
			inputs: Inputs(read, &TaskRef{}),
			want: `task "mergeRows" of Dag "etl": airflow.Inputs got a *airflow.TaskRef ` +
				`at index 1 that DagRef.Task did not return`,
		},
		{
			name:   "copy of a task of the Dag",
			inputs: Inputs(&readCopy),
			want: `task "mergeRows" of Dag "etl": airflow.Inputs got a *airflow.TaskRef ` +
				`at index 0 that DagRef.Task did not return`,
		},
		{
			name:   "task of another Dag",
			inputs: Inputs(fromReports),
			want: `task "mergeRows" of Dag "etl" cannot take an input from task "readRows" of ` +
				`another Dag, "reports"; pass tasks of the same Dag to airflow.Inputs`,
		},
		{
			name:   "task of another Dag with the same dag_id",
			inputs: Inputs(fromAnotherEtl),
			want: `task "mergeRows" of Dag "etl" cannot take an input from task "readRows" of ` +
				`another Dag, "etl"; pass tasks of the same Dag to airflow.Inputs`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				"airflow.DagRef.Task: "+tt.want,
				func() { dag.Task(mergeRows, tt.inputs) },
			)
		})
	}
}

func TestTaskRejectsASecondInputs(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)

	assert.PanicsWithValue(t,
		`airflow.DagRef.Task: task "countRows" of Dag "etl" got 2 airflow.Inputs values; `+
			`pass all of the task's inputs to one airflow.Inputs`,
		func() { dag.Task(countRows, Inputs(read), Inputs()) },
	)
}
