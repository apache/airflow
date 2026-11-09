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
	"encoding/json"
	"math"
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

// inputRefs returns the tasks among the inputs of task, in order.
func inputRefs(task *TaskRef) []*TaskRef {
	var refs []*TaskRef
	for _, input := range task.inputs {
		if !input.literal {
			refs = append(refs, input.ref)
		}
	}
	return refs
}

func assertInputs(t *testing.T, task *TaskRef, want ...*TaskRef) {
	t.Helper()
	require.Len(t, task.inputs, len(want))
	for i := range want {
		assert.Same(t, want[i], task.inputs[i].ref, "input %d", i)
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
	tasks := []Input{read}
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
		args[i] = binding.XComArg{Kind: "xcom", Name: argNames[i], TaskID: upstream.ref.taskID}
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
				`but airflow.Inputs passes no input`,
		},
		{
			name: "empty Inputs",
			add:  func() { dag.Task(report, Inputs()) },
			want: `task "report" of Dag "etl" has 1 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes no input`,
		},
		{
			name: "more tasks than parameters",
			add:  func() { dag.Task(report, Inputs(counted, read)) },
			want: `task "report" of Dag "etl" has 1 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes 2 input(s): "countRows", "readRows"`,
		},
		{
			name: "no parameter to fill",
			add:  func() { dag.Task(ping, Inputs(read)) },
			want: `task "ping" of Dag "etl" has 0 parameter(s) after airflow.Context, ` +
				`but airflow.Inputs passes 1 input(s): "readRows"`,
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
				`airflow.Inputs got a nil input at index 1`,
		},
		{
			name:   "nil TaskRef",
			inputs: Inputs(read, (*TaskRef)(nil)),
			want: `task "mergeRows" of Dag "etl": ` +
				`airflow.Inputs got a nil *airflow.TaskRef at index 1`,
		},
		{
			name:   "zero TaskRef",
			inputs: Inputs(read, &TaskRef{}),
			want: `task "mergeRows" of Dag "etl": airflow.Inputs got a *airflow.TaskRef ` +
				`at index 1 that DagRef.Task or TaskGroupRef.Task did not return`,
		},
		{
			name:   "copy of a task of the Dag",
			inputs: Inputs(&readCopy),
			want: `task "mergeRows" of Dag "etl": airflow.Inputs got a *airflow.TaskRef ` +
				`at index 0 that DagRef.Task or TaskGroupRef.Task did not return`,
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
	literal := func(Context, rowSet) (int, error) { return 0, nil }

	tests := []struct {
		name string
		fn   any
		opts []TaskOption
		// task is a regexp for how the panic message names the task.
		task string
	}{
		{
			name: "second Inputs is empty",
			fn:   countRows,
			opts: []TaskOption{Inputs(read), Inputs()},
			task: `countRows`,
		},
		{
			name: "first Inputs is empty",
			fn:   countRows,
			opts: []TaskOption{Inputs(), Inputs(read)},
			task: `countRows`,
		},
		{
			name: "TaskID in a TaskSpec after the Inputs",
			fn:   countRows,
			opts: []TaskOption{Inputs(read), Inputs(read), TaskSpec{TaskID: "count_rows"}},
			task: `count_rows`,
		},
		{
			name: "nil option after the second Inputs",
			fn:   countRows,
			opts: []TaskOption{Inputs(read), Inputs(read), nil},
			task: `countRows`,
		},
		{
			name: "no TaskID and no function name",
			fn:   literal,
			opts: []TaskOption{Inputs(read), Inputs(read)},
			task: `github\.com/apache/airflow/go-sdk/airflow\.` +
				`TestTaskRejectsASecondInputs\.func\d+`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { dag.Task(tt.fn, tt.opts...) })
			assert.Regexp(t,
				`^airflow\.DagRef\.Task: task "`+tt.task+`" of Dag "etl": got more than one `+
					`airflow\.Inputs; pass all of the task's inputs to one airflow\.Inputs$`,
				msg,
			)
		})
	}
}

func keepString(Context, string) error           { return nil }
func keepStringPointer(Context, *string) error   { return nil }
func keepRowSet(Context, rowSet) error           { return nil }
func loadInto(Context, int, string) error        { return nil }
func keepCount(Context, int) error               { return nil }
func keepSmallCount(Context, int8) error         { return nil }
func keepRowSetByPointer(Context, *rowSet) error { return nil }

func TestInputsTakeALiteralNextToATask(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(countRows, Inputs(dag.Task(readRows)))
	loaded := dag.Task(loadInto, Inputs(read, Literal("s3://bucket/out")))

	require.Len(t, loaded.inputs, 2)
	assert.Same(t, read, loaded.inputs[0].ref)
	assert.False(t, loaded.inputs[0].literal)
	assert.True(t, loaded.inputs[1].literal)
	assert.Equal(t, "s3://bucket/out", loaded.inputs[1].value)
}

func TestALiteralAddsNoEdge(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	alone := dag.Task(keepString, Inputs(Literal("x")))
	mixed := dag.Task(loadInto, Inputs(dag.Task(countRows, Inputs(read)), Literal("x")))
	Bundle().Register(dag)

	assert.Empty(t, alone.upstreams)
	assertTasks(t, mixed.upstreams, dag.tasksByID["countRows"])
	assertTasks(t, read.downstreams, dag.tasksByID["countRows"])
}

func TestALiteralTakesTheTypeOfItsParameter(t *testing.T) {
	dag := Dag("etl")

	assert.NotPanics(t, func() { dag.Task(keepString, Inputs(Literal("x"))) })
	assert.NotPanics(t, func() { dag.Task(keepStringPointer, Inputs(Literal(nil))) })
	assert.NotPanics(t, func() { dag.Task(keepAnything, Inputs(Literal(nil))) })
	assert.NotPanics(t, func() { dag.Task(keepCount, Inputs(Literal(3))) })
	assert.NotPanics(
		t,
		func() { dag.Task(average, Inputs(Literal(3))) },
		"a whole number fills a float64",
	)
	assert.NotPanics(t, func() {
		dag.Task(keepRowSet, Inputs(Literal(map[string]any{"rows": []string{"a"}})))
	}, "a map fills a struct")
	assert.NotPanics(t, func() {
		dag.Task(keepRowSetByPointer, Inputs(Literal(rowSet{Rows: []string{"a"}})))
	}, "a struct fills a pointer to a struct")
	assert.NotPanics(t, func() { dag.Task(keepCounts, Inputs(Literal(map[string]int{"a": 1}))) })
}

func TestTaskPanicsWhenALiteralDoesNotDecodeIntoItsParameter(t *testing.T) {
	dag := Dag("etl")

	tests := []struct {
		name string
		task string
		fn   any
		in   Input
		want string
	}{
		{
			name: "string into int",
			task: "keepCount",
			fn:   keepCount,
			in:   Literal("x"),
			want: `the airflow.Literal for parameter 1 cannot be decoded into int: ` +
				`json: cannot unmarshal string into Go value of type int`,
		},
		{
			name: "null into a parameter that is not nilable",
			task: "keepString",
			fn:   keepString,
			in:   Literal(nil),
			want: `the airflow.Literal for parameter 1 cannot be decoded into string: ` +
				`value is null but the parameter type string is not nilable`,
		},
		{
			name: "a number out of range for the parameter",
			task: "keepSmallCount",
			fn:   keepSmallCount,
			in:   Literal(300),
			want: `the airflow.Literal for parameter 1 cannot be decoded into int8: ` +
				`json: cannot unmarshal number 300 into Go value of type int8`,
		},
		{
			name: "a key the struct does not have",
			task: "keepRowSet",
			fn:   keepRowSet,
			in:   Literal(map[string]any{"other": 1}),
			want: `the airflow.Literal for parameter 1 cannot be decoded into airflow.rowSet: ` +
				`json: unknown field "other"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				`airflow.DagRef.Task: task "`+tt.task+`" of Dag "etl": `+tt.want,
				func() { dag.Task(tt.fn, Inputs(tt.in)) },
			)
		})
	}
}

func TestTaskPanicsOnALiteralThatJSONCannotHold(t *testing.T) {
	dag := Dag("etl")
	tooBig := json.Number("18446744073709551616")

	tests := []struct {
		name string
		in   Input
		want string
	}{
		{
			name: "a channel",
			in:   Literal(make(chan int)),
			want: "json: unsupported type: chan int",
		},
		{
			name: "NaN",
			in:   Literal(math.NaN()),
			want: "json: unsupported value: NaN",
		},
		{
			name: "an integer that does not fit in 64 bits",
			in:   Literal(map[string]any{"rows": []any{1, tooBig}}),
			want: `the integer at value["rows"][1] is 18446744073709551616, ` +
				`which does not fit in 64 bits`,
		},
		{
			name: "a number past the range of a float64",
			in:   Literal(json.Number("1e400")),
			want: "the number at value is 1e400, which is past the range of a float64",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				`airflow.DagRef.Task: task "keepAnything" of Dag "etl": `+
					`the airflow.Literal for parameter 1 is not valid: `+tt.want,
				func() { dag.Task(keepAnything, Inputs(tt.in)) },
			)
		})
	}
}

func TestTaskPanicsOnATaskRefInALiteral(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	type holder struct{ Upstream *TaskRef }

	tests := []struct {
		name string
		in   Input
		path string
	}{
		{"at the top level", Literal(read), "value"},
		{"a TaskRef value", Literal(*read), "value"},
		{"in a map", Literal(map[string]any{"a": read}), "value[a]"},
		{"in a slice", Literal([]any{1, []any{read}}), "value[1][0]"},
		{"in a struct", Literal(holder{Upstream: read}), "value.Upstream"},
		{"behind a pointer", Literal(&holder{Upstream: read}), "value.Upstream"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				`airflow.DagRef.Task: task "keepAnything" of Dag "etl": `+
					`the airflow.Literal for parameter 1 is not valid: `+tt.path+
					` is a *airflow.TaskRef, which is an edge and not data; `+
					`pass the TaskRef to airflow.Inputs itself`,
				func() { dag.Task(keepAnything, Inputs(tt.in)) },
			)
		})
	}
	assert.Empty(t, read.downstreams)
}

func TestALiteralMayHoldAValueThatPointsAtItself(t *testing.T) {
	type node struct {
		Name string `json:"name"`
		Next *node  `json:"-"`
	}
	loop := &node{Name: "a"}
	loop.Next = loop

	assert.NotPanics(t, func() { Dag("etl").Task(keepAnything, Inputs(Literal(loop))) })
}

func TestLiteralCopiesTheValue(t *testing.T) {
	dag := Dag("etl")
	value := map[string]any{"rows": []any{"a"}}
	copied := Literal(value)

	value["rows"].([]any)[0] = "changed"
	value["added"] = true
	task := dag.Task(keepAnything, Inputs(copied))

	assert.Equal(t, map[string]any{"rows": []any{"a"}}, task.inputs[0].value)
}

func TestLiteralKeepsAnIntegerAnInteger(t *testing.T) {
	task := Dag("etl").Task(keepAnything, Inputs(Literal(map[string]any{
		"small": 1, "big": int64(9007199254740993), "fraction": 1.5,
	})))

	assert.Equal(t,
		map[string]any{"small": int64(1), "big": int64(9007199254740993), "fraction": 1.5},
		task.inputs[0].value,
	)
}

func TestLiteralValuesReachTheTaskFunction(t *testing.T) {
	var got struct {
		anything any
		count    int
	}
	dag := Dag("etl")
	task := dag.Task(
		func(_ Context, anything any, count int) error {
			got.anything, got.count = anything, count
			return nil
		},
		Inputs(Literal(map[string]any{"a": []any{1, "x"}}), Literal(7)),
		TaskSpec{TaskID: "keep"},
	)
	args := make([]binding.Arg, len(task.inputs))
	for i, input := range task.inputs {
		args[i] = binding.LiteralArg{
			Kind:  "literal",
			Name:  "arg" + string(rune('0'+i)),
			Value: input.value,
		}
	}

	ctx := context.WithValue(
		context.Background(),
		sdkcontext.SdkClientContextKey,
		sdk.Client(resultsByTask{}),
	)
	require.NoError(t, task.task.Execute(ctx, discardLogger(), args))

	assert.Equal(t, map[string]any{"a": []any{float64(1), "x"}}, got.anything)
	assert.Equal(t, 7, got.count)
}
