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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deciderError struct{}

func (*deciderError) Error() string { return "decider failed" }

func pickPath(Context) (*TaskRef, error)                     { return nil, nil }
func pickPathFromRows(Context, rowSet) (*TaskRef, error)     { return nil, nil }
func pickPathOnlyError(Context) error                        { return nil }
func pickPathAsTaskRef(Context) (TaskRef, error)             { return TaskRef{}, nil }
func pickPathAsNode(Context) (Node, error)                   { return nil, nil }
func pickPathAsTaskID(Context) (string, error)               { return "", nil }
func pickPathWithoutError(Context) *TaskRef                  { return nil }
func pickPathWithoutResults(Context)                         {}
func pickPathWithOwnError(Context) (*TaskRef, *deciderError) { return nil, nil }
func handleLong(Context) error                               { return nil }
func handleShort(Context) error                              { return nil }
func handleOther(Context) error                              { return nil }

func TestSwitchAddsTheDeciderAsATask(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)

	pick := dag.Switch(pickPathFromRows, Inputs(read))

	require.NotNil(t, pick.task)
	assert.Equal(t, "pickPathFromRows", pick.task.taskID)
	assert.Equal(t, []*TaskRef{read, pick.task}, dag.tasks)
	assert.Same(t, pick.task, dag.tasksByID["pickPathFromRows"])
	assertInputs(t, pick.task, read)
	assert.Equal(t, reflect.TypeFor[*TaskRef](), pick.task.resultType)
	assert.Same(t, pick, pick.task.decider)
	assert.Nil(t, read.decider, "a task from DagRef.Task is not a switch")

	named := dag.Switch(pickPath, TaskSpec{TaskID: "pick_path"})
	assert.Equal(t, "pick_path", named.task.taskID)
	assert.Equal(t, TaskSpec{TaskID: "pick_path"}, named.task.spec)
}

func TestSwitchNeedsAFunctionThatReturnsATaskRef(t *testing.T) {
	tests := []struct {
		name    string
		fn      any
		fnName  string
		returns string
	}{
		{"only an error", pickPathOnlyError, "pickPathOnlyError", "error"},
		{"a TaskRef value", pickPathAsTaskRef, "pickPathAsTaskRef", "(airflow.TaskRef, error)"},
		{"a Node", pickPathAsNode, "pickPathAsNode", "(airflow.Node, error)"},
		{"a task_id", pickPathAsTaskID, "pickPathAsTaskID", "(string, error)"},
		{"no error", pickPathWithoutError, "pickPathWithoutError", "*airflow.TaskRef"},
		{"no results", pickPathWithoutResults, "pickPathWithoutResults", "nothing"},
		{
			"another error type",
			pickPathWithOwnError,
			"pickPathWithOwnError",
			"(*airflow.TaskRef, *airflow.deciderError)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag := Dag("etl")
			assert.PanicsWithValue(t,
				`airflow.DagRef.Switch: Dag "etl": github.com/apache/airflow/go-sdk/airflow.`+
					tt.fnName+` returns `+tt.returns+
					`, but a decider function must return (*airflow.TaskRef, error)`,
				func() { dag.Switch(tt.fn) },
			)
			assert.Empty(t, dag.tasks)
		})
	}
}

func TestSwitchRejectsTriggerDagRun(t *testing.T) {
	dag := Dag("etl")
	assert.PanicsWithValue(t,
		`airflow.DagRef.Switch: Dag "etl": fn comes from airflow.TriggerDagRun, `+
			`but a decider function is a Go function that returns (*airflow.TaskRef, error)`,
		func() {
			dag.Switch(
				TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
				TaskSpec{TaskID: "trigger"},
			)
		},
	)
	assert.Empty(t, dag.tasks)
}

// Switch shares its checks with DagRef.Task, and each panic message has to name DagRef.Switch, the
// function that the caller called.
func TestSwitchPanicsUnderItsOwnName(t *testing.T) {
	literal := func(Context) (*TaskRef, error) { return nil, nil }

	tests := []struct {
		name string
		add  func(dag *DagRef)
		// want is a part of the panic message after "airflow.DagRef.Switch: ".
		want string
	}{
		{
			name: "registered Dag",
			add: func(dag *DagRef) {
				Bundle().Register(dag)
				dag.Switch(pickPath)
			},
			want: "has already been registered",
		},
		{
			name: "not a function",
			add:  func(dag *DagRef) { dag.Switch("pickPath") },
			want: "fn is string, not a function",
		},
		{
			name: "no Context first",
			add: func(dag *DagRef) {
				dag.Switch(func(rowSet) (*TaskRef, error) { return nil, nil })
			},
			want: "but the first parameter must be airflow.Context",
		},
		{
			name: "nil option",
			add:  func(dag *DagRef) { dag.Switch(pickPath, nil) },
			want: "opts[0] is nil",
		},
		{
			name: "second TaskSpec",
			add:  func(dag *DagRef) { dag.Switch(pickPath, TaskSpec{}, TaskSpec{}) },
			want: "got more than one airflow.TaskSpec",
		},
		{
			name: "no name for the task_id",
			add:  func(dag *DagRef) { dag.Switch(literal) },
			want: "has no name to use as the task_id",
		},
		{
			name: "unknown trigger rule",
			add:  func(dag *DagRef) { dag.Switch(pickPath, TaskSpec{TriggerRule: "bogus"}) },
			want: `airflow.TaskSpec.TriggerRule is "bogus", which is not a trigger rule`,
		},
		{
			name: "duplicate task_id",
			add: func(dag *DagRef) {
				dag.Task(extract, TaskSpec{TaskID: "pickPath"})
				dag.Switch(pickPath)
			},
			want: `already has a task "pickPath"`,
		},
		{
			name: "missing input",
			add:  func(dag *DagRef) { dag.Switch(pickPathFromRows) },
			want: "but airflow.Inputs passes no input",
		},
		{
			name: "input of the wrong type",
			add:  func(dag *DagRef) { dag.Switch(pickPathFromRows, Inputs(dag.Task(readNames))) },
			want: "which cannot be assigned to airflow.rowSet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { tt.add(Dag("etl")) })
			assert.True(t, strings.HasPrefix(msg, "airflow.DagRef.Switch: "), msg)
			assert.Contains(t, msg, tt.want)
		})
	}
}

func TestCaseNamesTheTasksOfTheSwitch(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	short := dag.Task(handleShort)

	pick := dag.Switch(pickPath)
	assert.Same(t, pick, pick.Case(long))
	assert.Same(t, pick, pick.Case(short))

	assert.Equal(t, []*TaskRef{long, short}, pick.cases)
	assert.Empty(t, long.inputs, "the decider passes no result to a case")
	assert.Empty(t, short.inputs, "the decider passes no result to a case")
}

// TestCaseRecordsTheEdgeFromTheDecider pins that naming a case orders it after the decider. The
// edge puts the decider in the serialized Dag as the upstream of the case.
func TestCaseRecordsTheEdgeFromTheDecider(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	short := dag.Task(handleShort)

	pick := dag.Switch(pickPath).Case(long).Case(short)

	assertTasks(t, pick.task.downstreams, long, short)
	assertTasks(t, long.upstreams, pick.task)
	assertTasks(t, short.upstreams, pick.task)
	assertEdgeLabel(t, dag, "pickPath", "handleLong", "")
	assertEdgeLabel(t, dag, "pickPath", "handleShort", "")
}

func TestCaseRejectsATaskOutsideTheDag(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	longCopy := *long
	fromReports := Dag("reports").Task(handleLong)
	fromAnotherEtl := Dag("etl").Task(handleLong)

	tests := []struct {
		name string
		task *TaskRef
		want string
	}{
		{name: "nil", task: nil, want: `got a nil *airflow.TaskRef`},
		{
			name: "zero TaskRef",
			task: &TaskRef{},
			want: `got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task did not return`,
		},
		{
			name: "copy of a task of the Dag",
			task: &longCopy,
			want: `got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task did not return`,
		},
		{
			name: "task of another Dag",
			task: fromReports,
			want: `cannot choose task "handleLong" of another Dag, "reports"; ` +
				`pass a task of the same Dag`,
		},
		{
			name: "task of another Dag with the same dag_id",
			task: fromAnotherEtl,
			want: `cannot choose task "handleLong" of another Dag, "etl"; ` +
				`pass a task of the same Dag`,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decider := fmt.Sprintf("pick_%d", i)
			pick := dag.Switch(pickPath, TaskSpec{TaskID: decider})
			assert.PanicsWithValue(t,
				`airflow.SwitchRef.Case: switch "`+decider+`" of Dag "etl" `+tt.want,
				func() { pick.Case(tt.task) },
			)
			assert.Empty(t, pick.cases)
			assert.Empty(t, pick.task.downstreams)
		})
	}
}

func TestCaseRejectsATaskThatIsAlreadyACase(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	short := dag.Task(handleShort)
	pick := dag.Switch(pickPath).Case(long).Case(short)
	before := snapshot(dag)

	assert.PanicsWithValue(t,
		`airflow.SwitchRef.Case: switch "pickPath" of Dag "etl" already has task "handleLong" `+
			`as a case; name each case once`,
		func() { pick.Case(long) },
	)
	assert.Equal(t, before, snapshot(dag))
}

func TestCaseAfterRegisterPanics(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	short := dag.Task(handleShort)
	pick := dag.Switch(pickPath).Case(long)
	Bundle().Register(dag)

	assert.PanicsWithValue(t,
		`airflow.SwitchRef.Case: Dag "etl" has already been registered; `+
			`name the cases of every switch before Register`,
		func() { pick.Case(short) },
	)
	assert.Equal(t, []*TaskRef{long}, pick.cases)
}

func TestSwitchRefThatDagRefSwitchDidNotReturnPanics(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	pick := dag.Switch(pickPath)
	pickCopy := *pick
	var nilRef *SwitchRef

	for name, ref := range map[string]*SwitchRef{"nil": nilRef, "zero": {}, "copy": &pickCopy} {
		t.Run(name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				"airflow.SwitchRef.Case: DagRef.Switch or TaskGroupRef.Switch did not return "+
					"the *airflow.SwitchRef",
				func() { ref.Case(long) },
			)
		})
	}
	assert.Empty(t, pick.cases)
}

func TestRegisterRejectsASwitchWithoutACase(t *testing.T) {
	dag := Dag("etl")
	dag.Switch(pickPath, TaskSpec{TaskID: "complete"}).Case(dag.Task(handleLong))
	dag.Switch(pickPath)
	b := Bundle()

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: switch "pickPath" of Dag "etl" has no case; `+
			`name each task that the switch can choose with SwitchRef.Case`,
		func() { b.Register(dag) },
	)
	assert.False(t, dag.registered, "a Dag that Register rejects can still take tasks")
	assert.NotContains(t, b.dags.dags, "etl")
}

// TestRegisterTakesASwitchWithOneCase pins that one case is enough. The decider then has to
// return that case, and skips nothing.
func TestRegisterTakesASwitchWithOneCase(t *testing.T) {
	dag := Dag("etl")
	dag.Switch(pickPath).Case(dag.Task(handleLong))

	assert.NotPanics(t, func() { Bundle().Register(dag) })
	assert.True(t, dag.registered)
}

// TestRegisterRejectsACycleThroughASwitch pins that the cycle check walks the edge that Case
// records, so Register rejects a cycle that runs through a switch like any other cycle.
func TestRegisterRejectsACycleThroughASwitch(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	long := dag.Task(handleLong)
	dag.Switch(pickPathFromRows, Inputs(read)).Case(long)
	long.Before(read)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`readRows -> pickPathFromRows -> handleLong -> readRows`,
		func() { Bundle().Register(dag) },
	)
	assert.False(t, dag.registered)
}

// switchDag builds and registers a Dag whose switch pick_path has the given number of cases, taken
// in order from handle_long, handle_short and handle_other. The decider returns *choice. The
// task_ids differ from the function names, so that a test shows that the switch pushes and skips a
// case by its task_id.
func switchDag(t *testing.T, cases int, choice **TaskRef) (*SwitchRef, []*TaskRef) {
	t.Helper()
	dag := Dag("etl")
	tasks := []*TaskRef{
		dag.Task(handleLong, TaskSpec{TaskID: "handle_long"}),
		dag.Task(handleShort, TaskSpec{TaskID: "handle_short"}),
		dag.Task(handleOther, TaskSpec{TaskID: "handle_other"}),
	}
	pick := dag.Switch(
		func(Context) (*TaskRef, error) { return *choice, nil },
		TaskSpec{TaskID: "pick_path"},
	)
	for _, task := range tasks[:cases] {
		pick.Case(task)
	}
	Bundle().Register(dag)
	return pick, tasks
}

func TestSwitchSkipsTheCasesThatItDoesNotChoose(t *testing.T) {
	tests := []struct {
		name    string
		cases   int
		choose  int
		skipped []string
	}{
		{
			name:    "the first case skips the others",
			cases:   3,
			choose:  0,
			skipped: []string{"handle_short", "handle_other"},
		},
		{
			name:    "the last case skips the others",
			cases:   3,
			choose:  2,
			skipped: []string{"handle_long", "handle_short"},
		},
		{name: "the only case skips nothing", cases: 1, choose: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var choice *TaskRef
			pick, tasks := switchDag(t, tt.cases, &choice)
			choice = tasks[tt.choose]

			client, err := runDecider(pick.task, nil)
			require.NoError(t, err)

			assert.Equal(t, tasks[tt.choose].taskID, client.xcoms["return_value"],
				"the switch pushes the task_id of the case, not the *airflow.TaskRef")
			if len(tt.skipped) == 0 {
				assert.Empty(t, client.skipped)
				assert.NotContains(t, client.xcoms, "skipmixin_key")
			} else {
				assert.Equal(t, [][]string{tt.skipped}, client.skipped)
				assert.Equal(t,
					map[string][]string{"skipped": tt.skipped},
					client.xcoms["skipmixin_key"],
				)
			}
		})
	}
}

// TestSwitchSkipsACaseThatRunsAfterTheChosenCase pins the difference from a Python branch that the
// documentation of DagRef.Switch describes.
func TestSwitchSkipsACaseThatRunsAfterTheChosenCase(t *testing.T) {
	dag := Dag("etl")
	optional := dag.Task(handleLong, TaskSpec{TaskID: "optional"})
	join := dag.Task(handleShort, TaskSpec{TaskID: "join"})
	optional.Before(join)
	pick := dag.Switch(
		func(Context) (*TaskRef, error) { return optional, nil },
		TaskSpec{TaskID: "pick_path"},
	).Case(optional).Case(join)
	Bundle().Register(dag)

	client, err := runDecider(pick.task, nil)

	require.NoError(t, err)
	assert.Equal(t, [][]string{{"join"}}, client.skipped)
}

func TestSwitchFailsWhenTheDeciderReturnsATaskRefThatIsNotACase(t *testing.T) {
	var choice *TaskRef
	pick, tasks := switchDag(t, 2, &choice)
	longCopy := *tasks[0]
	fromReports := Dag("reports").Task(handleLong, TaskSpec{TaskID: "handle_long"})

	tests := []struct {
		name   string
		choice *TaskRef
		want   string
	}{
		{name: "a task of the Dag", choice: tasks[2], want: `task "handle_other"`},
		{name: "nil", choice: nil, want: "a nil *airflow.TaskRef"},
		{
			name:   "a task of another Dag",
			choice: fromReports,
			want:   `task "handle_long" of another Dag, "reports"`,
		},
		{
			name:   "a zero TaskRef",
			choice: &TaskRef{},
			want:   "a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task did not return",
		},
		{
			name:   "a copy of a case",
			choice: &longCopy,
			want:   `a copy of the *airflow.TaskRef of task "handle_long"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			choice = tt.choice

			client, err := runDecider(pick.task, nil)

			require.EqualError(t, err,
				`switch "pick_path" of Dag "etl" returned `+tt.want+
					`, which is not one of its cases: "handle_long", "handle_short"`,
			)
			assert.Empty(t, client.xcoms, "a switch that fails pushes no XCom")
			assert.Empty(t, client.skipped)
		})
	}
}

// TestSwitchThatFailsPushesAndSkipsNothing covers a decider that returns a case together with an
// error. The task fails without pushing the task_id of that case or skipping the other cases.
func TestSwitchThatFailsPushesAndSkipsNothing(t *testing.T) {
	dag := Dag("etl")
	long := dag.Task(handleLong)
	pick := dag.Switch(
		func(Context) (*TaskRef, error) { return long, errors.New("cannot reach the table") },
		TaskSpec{TaskID: "pick_path"},
	).Case(long).Case(dag.Task(handleShort))
	Bundle().Register(dag)

	client, err := runDecider(pick.task, nil)

	require.EqualError(t, err, "cannot reach the table")
	assert.Empty(t, client.xcoms)
	assert.Empty(t, client.skipped)
}

// Case and Register both take the lock of the Dag, so each Case call either names its case before
// the Dag is registered or panics. If Case read the registered flag without the lock, only a run
// with -race would fail.
func TestRegisterWhileCaseIsCalled(t *testing.T) {
	const switches = 1000

	dag := Dag("etl")
	picks := make([]*SwitchRef, switches)
	shorts := make([]*TaskRef, switches)
	for i := range switches {
		picks[i] = dag.Switch(pickPath, TaskSpec{TaskID: fmt.Sprintf("pick_%d", i)}).
			Case(dag.Task(handleLong, TaskSpec{TaskID: fmt.Sprintf("long_%d", i)}))
		shorts[i] = dag.Task(handleShort, TaskSpec{TaskID: fmt.Sprintf("short_%d", i)})
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
		for ; o.named < switches-1; o.named++ {
			select {
			case <-registered:
				// Register has returned, so this Case call has to panic.
				picks[o.named].Case(shorts[o.named])
				return
			default:
				picks[o.named].Case(shorts[o.named])
			}
		}
		select {
		case <-registered:
		case <-time.After(10 * time.Second):
			return
		}
		picks[o.named].Case(shorts[o.named])
	}()
	// Wait until the goroutine is running before calling Register, as
	// TestRegisterWhileTasksAreAdded explains.
	<-started
	Bundle().Register(dag)
	close(registered)
	o := <-done

	assert.Equal(t,
		`airflow.SwitchRef.Case: Dag "etl" has already been registered; `+
			`name the cases of every switch before Register`,
		o.recovered,
	)
	for i, pick := range picks {
		if i < o.named {
			assert.Len(t, pick.cases, 2, "switch %d", i)
		} else {
			assert.Len(t, pick.cases, 1, "switch %d", i)
		}
	}
}
