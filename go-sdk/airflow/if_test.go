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
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/sdk"
)

type ready bool

type conditionError struct{}

func (*conditionError) Error() string { return "condition failed" }

func isReady(Context) (bool, error)                       { return true, nil }
func hasRows(_ Context, rows rowSet) (bool, error)        { return len(rows.Rows) > 0, nil }
func isReadyOnlyError(Context) error                      { return nil }
func isReadyAsInt(Context) (int, error)                   { return 0, nil }
func isReadyAsPointer(Context) (*bool, error)             { return nil, nil }
func isReadyAsNamedBool(Context) (ready, error)           { return false, nil }
func isReadyWithoutResults(Context)                       {}
func isReadyWithOwnError(Context) (bool, *conditionError) { return true, nil }
func load(Context) error                                  { return nil }
func reportEmpty(Context) error                           { return nil }

func TestIfAddsTheConditionAsATask(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)

	gate := dag.If(hasRows, Inputs(read))

	require.NotNil(t, gate.task)
	assert.Equal(t, "hasRows", gate.task.taskID)
	assert.Equal(t, []*TaskRef{read, gate.task}, dag.tasks)
	assert.Same(t, gate.task, dag.tasksByID["hasRows"])
	assertInputs(t, gate.task, read)
	assert.Equal(t, reflect.TypeFor[bool](), gate.task.resultType)
	assert.Same(t, gate, gate.task.ifRef)
	assert.Nil(t, read.ifRef, "a task from DagRef.Task is not a condition")

	named := dag.If(isReady, TaskSpec{TaskID: "is_ready"})
	assert.Equal(t, "is_ready", named.task.taskID)
	assert.Equal(t, TaskSpec{TaskID: "is_ready"}, named.task.spec)
}

func TestIfNeedsAFunctionThatReturnsABool(t *testing.T) {
	tests := []struct {
		name    string
		fn      any
		fnName  string
		returns string
	}{
		{"only an error", isReadyOnlyError, "isReadyOnlyError", "error"},
		{"another result type", isReadyAsInt, "isReadyAsInt", "(int, error)"},
		{"pointer to bool", isReadyAsPointer, "isReadyAsPointer", "(*bool, error)"},
		{"named bool type", isReadyAsNamedBool, "isReadyAsNamedBool", "(airflow.ready, error)"},
		{"no results", isReadyWithoutResults, "isReadyWithoutResults", "nothing"},
		{
			"another error type",
			isReadyWithOwnError,
			"isReadyWithOwnError",
			"(bool, *airflow.conditionError)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			assert.PanicsWithValue(t,
				`airflow.DagRef.If: Dag "etl": github.com/apache/airflow/go-sdk/airflow.`+
					tt.fnName+` returns `+tt.returns+
					`, but a condition function must return (bool, error)`,
				func() { dag.If(tt.fn) },
			)
			assert.Empty(t, dag.tasks)
		})
	}
}

func TestIfRejectsTriggerDagRun(t *testing.T) {
	dag := Dag("etl")
	assert.PanicsWithValue(t,
		`airflow.DagRef.If: Dag "etl": fn comes from airflow.TriggerDagRun, `+
			`but a condition function is a Go function that returns (bool, error)`,
		func() {
			dag.If(
				TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
				TaskSpec{TaskID: "trigger"},
			)
		},
	)
	assert.Empty(t, dag.tasks)
}

// If shares its checks with DagRef.Task, and each panic message has to name DagRef.If, the
// function that the caller called.
func TestIfPanicsUnderItsOwnName(t *testing.T) {
	read := func(dag *DagRef) *TaskRef { return dag.Task(readRows) }
	literal := func(Context) (bool, error) { return true, nil }

	tests := []struct {
		name string
		add  func(dag *DagRef)
		// want is a part of the panic message after "airflow.DagRef.If: ".
		want string
	}{
		{
			name: "registered Dag",
			add: func(dag *DagRef) {
				Bundle().Register(dag)
				dag.If(isReady)
			},
			want: "has already been registered",
		},
		{
			name: "not a function",
			add:  func(dag *DagRef) { dag.If("isReady") },
			want: "fn is string, not a function",
		},
		{
			name: "nil option",
			add:  func(dag *DagRef) { dag.If(isReady, nil) },
			want: "opts[0] is nil",
		},
		{
			name: "nil *TaskSpec",
			add:  func(dag *DagRef) { dag.If(isReady, (*TaskSpec)(nil)) },
			want: "opts[0] is a nil *airflow.TaskSpec",
		},
		{
			name: "option from a struct that embeds a TaskSpec",
			add:  func(dag *DagRef) { dag.If(isReady, wrappedSpec{}) },
			want: "opts[0] has type airflow.wrappedSpec",
		},
		{
			name: "second TaskSpec",
			add:  func(dag *DagRef) { dag.If(isReady, TaskSpec{}, TaskSpec{}) },
			want: "got more than one airflow.TaskSpec",
		},
		{
			name: "no name for the task_id",
			add:  func(dag *DagRef) { dag.If(literal) },
			want: "has no name to use as the task_id",
		},
		{
			name: "unknown trigger rule",
			add:  func(dag *DagRef) { dag.If(isReady, TaskSpec{TriggerRule: "bogus"}) },
			want: `airflow.TaskSpec.TriggerRule is "bogus", which is not a trigger rule`,
		},
		{
			name: "duplicate task_id",
			add: func(dag *DagRef) {
				dag.Task(extract, TaskSpec{TaskID: "isReady"})
				dag.If(isReady)
			},
			want: `already has a task "isReady"`,
		},
		{
			name: "second Inputs",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(read(dag)), Inputs()) },
			want: "got more than one airflow.Inputs",
		},
		{
			name: "nil input",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(nil)) },
			want: "airflow.Inputs got a nil *airflow.TaskRef",
		},
		{
			name: "input from another Dag",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(read(Dag("reports")))) },
			want: "cannot take an input from task",
		},
		{
			name: "input that DagRef.Task did not return",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(&TaskRef{})) },
			want: "that DagRef.Task did not return",
		},
		{
			name: "missing input",
			add:  func(dag *DagRef) { dag.If(hasRows) },
			want: "but airflow.Inputs passes no task",
		},
		{
			name: "input from TriggerDagRun",
			add: func(dag *DagRef) {
				trigger := dag.Task(
					TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
					TaskSpec{TaskID: "trigger"},
				)
				dag.If(hasRows, Inputs(trigger))
			},
			want: "comes from airflow.TriggerDagRun and returns no result",
		},
		{
			name: "input with no result",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(dag.Task(extract))) },
			want: "but that task returns only an error",
		},
		{
			name: "input of the wrong type",
			add:  func(dag *DagRef) { dag.If(hasRows, Inputs(dag.Task(readNames))) },
			want: "which cannot be assigned to airflow.rowSet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { tt.add(Dag("etl")) })
			assert.True(t, strings.HasPrefix(msg, "airflow.DagRef.If: "), msg)
			assert.Contains(t, msg, tt.want)
		})
	}
}

func TestThenAndElseNameTheTasksOfTheCondition(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	reported := dag.Task(reportEmpty)

	gate := dag.If(isReady)
	assert.Same(t, gate, gate.Then(loaded))
	assert.Same(t, gate, gate.Else(reported))

	assert.Same(t, loaded, gate.thenTask)
	assert.Same(t, reported, gate.elseTask)
	assert.Empty(t, loaded.inputs, "the condition passes no result to the task from Then")
	assert.Empty(t, reported.inputs, "the condition passes no result to the task from Else")

	reversed := dag.If(isReady, TaskSpec{TaskID: "reversed"}).Else(reported).Then(loaded)
	assert.Same(t, loaded, reversed.thenTask, "Else can come before Then")
	assert.Same(t, reported, reversed.elseTask)
}

// TestThenAndElseRecordTheEdgeFromTheCondition pins that naming a task orders it after the
// condition, which is what puts the condition in the serialized Dag as the task's upstream.
func TestThenAndElseRecordTheEdgeFromTheCondition(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	reported := dag.Task(reportEmpty)

	gate := dag.If(isReady)
	gate.Then(loaded)
	gate.Else(reported)

	assertTasks(t, gate.task.downstreams, loaded, reported)
	assertTasks(t, loaded.upstreams, gate.task)
	assertTasks(t, reported.upstreams, gate.task)
	assertEdgeLabel(t, dag, "isReady", "load", "")
	assertEdgeLabel(t, dag, "isReady", "reportEmpty", "")
}

// TestDeclaringTheEdgeOfAConditionAgainChangesNothing covers an author who also writes the edge
// that Then records, which is as idempotent as declaring any edge twice.
func TestDeclaringTheEdgeOfAConditionAgainChangesNothing(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)

	gate := dag.If(isReady)
	gate.Then(loaded)
	gate.task.Before(Label(loaded, "when ready"))

	assertTasks(t, gate.task.downstreams, loaded)
	assertTasks(t, loaded.upstreams, gate.task)
	assertEdgeLabel(t, dag, "isReady", "load", "when ready")
}

func TestThenAndElseRejectATaskOutsideTheDag(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	loadedCopy := *loaded
	fromReports := Dag("reports").Task(load)
	fromAnotherEtl := Dag("etl").Task(load)

	tests := []struct {
		name string
		task *TaskRef
		want string
	}{
		{name: "nil", task: nil, want: `got a nil *airflow.TaskRef`},
		{
			name: "zero TaskRef",
			task: &TaskRef{},
			want: `got a *airflow.TaskRef that DagRef.Task did not return`,
		},
		{
			name: "copy of a task of the Dag",
			task: &loadedCopy,
			want: `got a *airflow.TaskRef that DagRef.Task did not return`,
		},
		{
			name: "task of another Dag",
			task: fromReports,
			want: `cannot run task "load" of another Dag, "reports"; pass a task of the same Dag`,
		},
		{
			name: "task of another Dag with the same dag_id",
			task: fromAnotherEtl,
			want: `cannot run task "load" of another Dag, "etl"; pass a task of the same Dag`,
		},
	}
	for i, tt := range tests {
		for _, side := range []struct {
			name string
			call func(gate *IfRef, task *TaskRef)
		}{
			{"Then", func(gate *IfRef, task *TaskRef) { gate.Then(task) }},
			{"Else", func(gate *IfRef, task *TaskRef) { gate.Else(task) }},
		} {
			t.Run(tt.name+"/"+side.name, func(t *testing.T) {
				condition := fmt.Sprintf("ready_%d_%s", i, side.name)
				gate := dag.If(isReady, TaskSpec{TaskID: condition})
				assert.PanicsWithValue(t,
					"airflow.IfRef."+side.name+`: condition "`+condition+`" of Dag "etl" `+tt.want,
					func() { side.call(gate, tt.task) },
				)
				assert.Nil(t, gate.thenTask)
				assert.Nil(t, gate.elseTask)
			})
		}
	}
}

func TestThenAndElseNameOneTaskEach(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	reported := dag.Task(reportEmpty)

	gate := dag.If(isReady).Then(loaded)
	assert.PanicsWithValue(t,
		`airflow.IfRef.Then: condition "isReady" of Dag "etl" already has task "load" from Then; `+
			`call Then once`,
		func() { gate.Then(reported) },
	)
	assert.PanicsWithValue(t,
		`airflow.IfRef.Else: condition "isReady" of Dag "etl" already has task "load" from Then, `+
			`and a task cannot be on both sides of a condition`,
		func() { gate.Else(loaded) },
	)
	gate.Else(reported)
	assert.PanicsWithValue(t,
		`airflow.IfRef.Else: condition "isReady" of Dag "etl" already has task "reportEmpty" `+
			`from Else; call Else once`,
		func() { gate.Else(loaded) },
	)

	elseFirst := dag.If(isReady, TaskSpec{TaskID: "else_first"}).Else(reported)
	assert.PanicsWithValue(t,
		`airflow.IfRef.Then: condition "else_first" of Dag "etl" already has task "reportEmpty" `+
			`from Else, and a task cannot be on both sides of a condition`,
		func() { elseFirst.Then(reported) },
	)

	assert.Same(t, loaded, gate.thenTask)
	assert.Same(t, reported, gate.elseTask)
	assert.Nil(t, elseFirst.thenTask)
}

func TestThenAndElseAfterRegisterPanic(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	reported := dag.Task(reportEmpty)
	gate := dag.If(isReady).Then(loaded)
	Bundle().Register(dag)

	for _, side := range []struct {
		name string
		call func()
	}{
		{"Then", func() { gate.Then(reported) }},
		{"Else", func() { gate.Else(reported) }},
	} {
		assert.PanicsWithValue(t,
			"airflow.IfRef."+side.name+`: Dag "etl" has already been registered; `+
				`name the tasks of every condition before Register`,
			side.call,
		)
	}
	assert.Same(t, loaded, gate.thenTask)
	assert.Nil(t, gate.elseTask)
}

func TestIfRefThatDagRefIfDidNotReturnPanics(t *testing.T) {
	dag := Dag("etl")
	loaded := dag.Task(load)
	gate := dag.If(isReady)
	gateCopy := *gate
	var nilRef *IfRef

	for name, ref := range map[string]*IfRef{"nil": nilRef, "zero": {}, "copy": &gateCopy} {
		t.Run(name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				"airflow.IfRef.Then: DagRef.If did not return the *airflow.IfRef",
				func() { ref.Then(loaded) },
			)
			assert.PanicsWithValue(t,
				"airflow.IfRef.Else: DagRef.If did not return the *airflow.IfRef",
				func() { ref.Else(loaded) },
			)
		})
	}
	assert.Nil(t, gate.thenTask)
	assert.Nil(t, gate.elseTask)
}

func TestRegisterRejectsAConditionWithoutThen(t *testing.T) {
	tests := []struct {
		name string
		add  func(dag *DagRef)
	}{
		{name: "no Then or Else", add: func(dag *DagRef) { dag.If(isReady) }},
		{
			name: "only Else",
			add:  func(dag *DagRef) { dag.If(isReady).Else(dag.Task(reportEmpty)) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			dag.If(isReady, TaskSpec{TaskID: "complete"}).Then(dag.Task(load))
			tt.add(dag)
			b := Bundle()

			assert.PanicsWithValue(t,
				`airflow.BundleRef.Register: condition "isReady" of Dag "etl" has no task from `+
					`Then; name the task that runs when the condition is true with IfRef.Then`,
				func() { b.Register(dag) },
			)
			assert.False(t, dag.registered, "a Dag that Register rejects can still take tasks")
			assert.NotContains(t, b.dags.dags, "etl")
		})
	}
}

// conditionClient answers GetXCom from results, which maps a task_id to the result of that task.
// It records the XComs that a condition pushes. Its skip method stands in for the function that
// the runtime passes through bundle.WithSkipDownstreamTasks, and records the task_ids that the
// condition skips.
type conditionClient struct {
	sdk.Client
	results map[string]any
	xcoms   map[string]any
	skipped [][]string
}

func (c *conditionClient) GetXCom(
	_ context.Context,
	_, _, taskID string,
	_ *int,
	_ string,
	_ any,
) (any, error) {
	return c.results[taskID], nil
}

func (c *conditionClient) PushXCom(_ context.Context, _ sdk.TaskInstance, key string, v any) error {
	c.xcoms[key] = v
	return nil
}

func (c *conditionClient) skip(_ context.Context, taskIDs []string) error {
	c.skipped = append(c.skipped, taskIDs)
	return nil
}

// runCondition runs the task of gate through its Execute method, as the runtime runs a task. It
// passes one XCom binding per input, named after the arg tag of rowSet, so that a struct
// parameter shows whether it takes the whole result. results maps the task_id of each upstream
// task to its result. earlier maps each key to an XCom that an earlier try of the task left.
func runCondition(gate *IfRef, results, earlier map[string]any) (*conditionClient, error) {
	args := make([]binding.Arg, len(gate.task.inputs))
	for i, upstream := range gate.task.inputs {
		args[i] = binding.XComArg{Kind: "xcom", Name: "rows", TaskID: upstream.taskID}
	}
	client := &conditionClient{results: results, xcoms: maps.Clone(earlier)}
	if client.xcoms == nil {
		client.xcoms = map[string]any{}
	}
	ti := sdk.TaskInstance{DagID: "etl", RunID: "run1", TaskID: gate.task.taskID}
	ctx := context.WithValue(
		context.Background(),
		sdkcontext.SdkClientContextKey,
		sdk.Client(client),
	)
	ctx = context.WithValue(
		ctx,
		sdkcontext.RuntimeContextKey,
		sdk.NewTIRunContext(context.Background(), ti, sdk.DagRun{DagID: "etl", RunID: "run1"}),
	)
	ctx = bundle.WithSkipDownstreamTasks(ctx, client.skip)
	return client, gate.task.task.Execute(ctx, discardLogger(), args)
}

// TestRegisterRejectsACycleThroughACondition pins that the edge Then records is one the cycle
// check walks, so a cycle that runs through a condition is rejected like any other.
func TestRegisterRejectsACycleThroughACondition(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	loaded := dag.Task(load)
	dag.If(hasRows, Inputs(read)).Then(loaded)
	loaded.Before(read)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`readRows -> hasRows -> load -> readRows`,
		func() { Bundle().Register(dag) },
	)
	assert.False(t, dag.registered)
}

func TestConditionSkipsTheSideThatItDoesNotTake(t *testing.T) {
	tests := []struct {
		name     string
		rows     []any
		withElse bool
		want     bool
		skipped  []string
	}{
		{
			name:     "true skips the task from Else",
			rows:     []any{"a"},
			withElse: true,
			want:     true,
			skipped:  []string{"report_empty"},
		},
		{
			name:     "false skips the task from Then",
			rows:     []any{},
			withElse: true,
			want:     false,
			skipped:  []string{"load_rows"},
		},
		{
			name:    "true without Else skips nothing",
			rows:    []any{"a"},
			want:    true,
			skipped: []string{},
		},
		{
			name:    "false without Else skips the task from Then",
			rows:    []any{},
			want:    false,
			skipped: []string{"load_rows"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			read := dag.Task(readRows)
			// The TaskIDs differ from the function names, so the test shows that the condition
			// skips a task by its task_id.
			loaded := dag.Task(load, TaskSpec{TaskID: "load_rows"})
			reported := dag.Task(reportEmpty, TaskSpec{TaskID: "report_empty"})
			gate := dag.If(hasRows, Inputs(read)).Then(loaded)
			if tt.withElse {
				gate.Else(reported)
			}
			Bundle().Register(dag)

			client, err := runCondition(gate, map[string]any{
				"readRows": map[string]any{"rows": tt.rows},
			}, nil)
			require.NoError(t, err)

			assert.Equal(t, tt.want, client.xcoms["return_value"])
			if len(tt.skipped) == 0 {
				assert.Empty(t, client.skipped)
			} else {
				assert.Equal(t, [][]string{tt.skipped}, client.skipped)
			}
			assert.Equal(t,
				map[string][]string{"skipped": tt.skipped},
				client.xcoms["skipmixin_key"],
			)
		})
	}
}

func TestConditionThatFailsSkipsNothing(t *testing.T) {
	dag := Dag("etl")
	gate := dag.If(
		func(Context) (bool, error) { return false, errors.New("cannot reach the table") },
		TaskSpec{TaskID: "has_rows"},
	).Then(dag.Task(load)).Else(dag.Task(reportEmpty))
	Bundle().Register(dag)

	// An earlier try of the condition skipped load, and this try fails before it decides which
	// side to skip.
	client, err := runCondition(gate, nil, map[string]any{
		"skipmixin_key": map[string][]string{"skipped": {"load"}},
	})

	require.EqualError(t, err, "cannot reach the table")
	assert.Empty(t, client.skipped)
	assert.Equal(t,
		map[string][]string{"skipped": {}},
		client.xcoms["skipmixin_key"],
		"the list of the earlier try must not be left behind",
	)
}

// Else and Register both take the lock of the Dag, so each Else call either names its task before
// the Dag is registered or panics. If Else read the registered flag without the lock, only a run
// with -race would fail.
func TestRegisterWhileElseIsCalled(t *testing.T) {
	const conditions = 1000

	dag := Dag("etl")
	gates := make([]*IfRef, conditions)
	reported := make([]*TaskRef, conditions)
	for i := range conditions {
		gates[i] = dag.If(isReady, TaskSpec{TaskID: fmt.Sprintf("ready_%d", i)}).
			Then(dag.Task(load, TaskSpec{TaskID: fmt.Sprintf("load_%d", i)}))
		reported[i] = dag.Task(reportEmpty, TaskSpec{TaskID: fmt.Sprintf("report_%d", i)})
	}
	started := make(chan struct{})
	registered := make(chan struct{})
	type outcome struct {
		named     int
		recovered any
	}
	done := make(chan outcome)
	go func() {
		var o outcome
		defer func() {
			o.recovered = recover()
			done <- o
		}()
		close(started)
		for ; o.named < conditions-1; o.named++ {
			select {
			case <-registered:
				// Register has returned, so this Else call has to panic.
				gates[o.named].Else(reported[o.named])
				return
			default:
				gates[o.named].Else(reported[o.named])
			}
		}
		select {
		case <-registered:
		case <-time.After(10 * time.Second):
			return
		}
		gates[o.named].Else(reported[o.named])
	}()
	// Wait until the goroutine is running before calling Register, as TestRegisterWhileTasksAreAdded
	// explains.
	<-started
	Bundle().Register(dag)
	close(registered)
	o := <-done

	assert.Equal(t,
		`airflow.IfRef.Else: Dag "etl" has already been registered; `+
			`name the tasks of every condition before Register`,
		o.recovered,
	)
	for i, gate := range gates {
		if i < o.named {
			assert.Same(t, reported[i], gate.elseTask, "condition %d", i)
		} else {
			assert.Nil(t, gate.elseTask, "condition %d", i)
		}
	}
}
