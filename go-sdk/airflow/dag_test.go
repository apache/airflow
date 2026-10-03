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
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extract(Context) error { return nil }

func ExtractRows(Context) error { return nil }

func extractAny[T any](Context) error { return nil }

type extractor struct{}

func (extractor) Extract(Context) error { return nil }

func (*extractor) ExtractByPointer(Context) error { return nil }

var packageLevelLiteral = func(Context) error { return nil }

func addExtract[T interface{ Extract(Context) error }](
	dag *DagRef,
	src T,
	opts ...TaskOption,
) *TaskRef {
	return dag.Task(src.Extract, opts...)
}

// Embedding promotes applyTask, so these structs compile as a TaskOption.
type (
	wrappedSpec struct {
		TaskSpec
		MaxRetries int
	}
	wrappedOption struct{ TaskOption }
)

func TestTaskIDIsTheFunctionName(t *testing.T) {
	tests := []struct {
		name string
		fn   any
		want string
	}{
		{name: "function", fn: extract, want: "extract"},
		{name: "spelling kept", fn: ExtractRows, want: "ExtractRows"},
		{name: "generic function", fn: extractAny[int], want: "extractAny"},
		{name: "method value", fn: extractor{}.Extract, want: "Extract"},
		{
			name: "pointer method value",
			fn:   (&extractor{}).ExtractByPointer,
			want: "ExtractByPointer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := Dag("etl").Task(tt.fn)
			assert.Equal(t, tt.want, task.taskID)
		})
	}
}

// The runtime name in each row has the form that runtime.FuncForPC reports for the kind of
// function that the row names.
func TestTaskIDFromFuncName(t *testing.T) {
	tests := []struct {
		name        string
		runtimeName string
		want        string
		ok          bool
	}{
		{"function in main", "main.extract", "extract", true},
		{
			"function in another package",
			"github.com/acme/etl/tasks.ExtractRows",
			"ExtractRows",
			true,
		},
		{"dots in the import path", "example.com/a.b/c%2ed.Extract", "Extract", true},
		{"generic function", "main.process[...]", "process", true},
		{"pointer-receiver method value", "main.(*Service).Extract-fm", "Extract", true},
		{"value-receiver method value", "main.Service.Extract-fm", "Extract", true},
		{"method value on a generic type", "example.com/etl.(*Box[...]).Run-fm", "Run", true},
		{
			"method value on an interface literal",
			"go:interface { Run(github.com/apache/airflow/go-sdk/airflow.Context) error }.Run-fm",
			"Run",
			true,
		},
		{"function literal", "main.main.func1", "", false},
		{"function literal in a package variable", "main.init.func1", "", false},
		{"inlined function literal", "main.main.makeClosure.func2", "", false},
		{"method value taken on a type parameter", "main.addSource[...].func1", "", false},
		{"method expression", "main.Service.Extract", "", false},
		{"reflect.MakeFunc", "reflect.makeFuncStub", "", false},
		{"method value from reflect", "reflect.methodValueCall", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := taskIDFromFuncName(tt.runtimeName)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDagTakesAtMostOneDagSpec(t *testing.T) {
	assert.NotPanics(t, func() { Dag("etl", DagSpec{}) })
	assert.PanicsWithValue(t,
		`airflow.Dag: Dag "etl" got 2 airflow.DagSpec values; `+
			`set all of the Dag's attributes in one DagSpec`,
		func() { Dag("etl", DagSpec{}, DagSpec{}) },
	)
}

func TestTaskSpecSetsTheTaskID(t *testing.T) {
	dag := Dag("etl")

	spec := TaskSpec{TaskID: "extract_rows"}
	task := dag.Task(ExtractRows, spec)
	assert.Equal(t, "extract_rows", task.taskID)
	assert.Equal(t, spec, task.spec)

	task = dag.Task(extract, TaskSpec{})
	assert.Equal(t, "extract", task.taskID, "an empty TaskID keeps the function name")
}

func TestTaskTakesAPointerToTaskSpec(t *testing.T) {
	task := Dag("etl").Task(extract, &TaskSpec{TaskID: "extract_rows"})
	assert.Equal(t, "extract_rows", task.taskID)
	assert.Equal(t, TaskSpec{TaskID: "extract_rows"}, task.spec)
}

func TestTaskNeedsTaskIDWhenFnHasNoName(t *testing.T) {
	const pkg = `github\.com/apache/airflow/go-sdk/airflow\.`
	literal := func(Context) error { return nil }
	madeByReflect := reflect.MakeFunc(
		reflect.TypeFor[func(Context) error](),
		func([]reflect.Value) []reflect.Value {
			return []reflect.Value{reflect.Zero(reflect.TypeFor[error]())}
		},
	).Interface()

	tests := []struct {
		name string
		add  func(dag *DagRef, opts ...TaskOption) *TaskRef
		// runtimeName is a regexp for the name the panic message shows.
		runtimeName string
	}{
		{
			name: "function literal",
			add: func(dag *DagRef, opts ...TaskOption) *TaskRef {
				return dag.Task(literal, opts...)
			},
			runtimeName: pkg + `TestTaskNeedsTaskIDWhenFnHasNoName\.func\d+`,
		},
		{
			name: "function literal in a package variable",
			add: func(dag *DagRef, opts ...TaskOption) *TaskRef {
				return dag.Task(packageLevelLiteral, opts...)
			},
			runtimeName: pkg + `init\.func\d+`,
		},
		{
			name: "reflect.MakeFunc",
			add: func(dag *DagRef, opts ...TaskOption) *TaskRef {
				return dag.Task(madeByReflect, opts...)
			},
			runtimeName: `reflect\.makeFuncStub`,
		},
		{
			name: "method value taken on a type parameter",
			add: func(dag *DagRef, opts ...TaskOption) *TaskRef {
				return addExtract(dag, extractor{}, opts...)
			},
			runtimeName: pkg + `addExtract\[\.\.\.\]\.func\d+`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { tt.add(Dag("etl")) })
			assert.Regexp(t,
				`^airflow\.DagRef\.Task: Dag "etl": `+tt.runtimeName+` has no name to use `+
					`as the task_id; set one with airflow\.TaskSpec\{TaskID: \.\.\.\}$`,
				msg,
			)

			task := tt.add(Dag("etl"), TaskSpec{TaskID: "transform"})
			assert.Equal(t, "transform", task.taskID)
		})
	}
}

func TestTaskRejectsASecondTaskSpec(t *testing.T) {
	literal := func(Context) error { return nil }
	var nilSpec *TaskSpec

	tests := []struct {
		name string
		fn   any
		opts []TaskOption
		// task is a regexp for how the panic message names the task.
		task string
	}{
		{
			name: "TaskID in the first spec",
			fn:   ExtractRows,
			opts: []TaskOption{TaskSpec{TaskID: "extract_rows"}, TaskSpec{}},
			task: `extract_rows`,
		},
		{
			name: "TaskID in the second spec",
			fn:   literal,
			opts: []TaskOption{TaskSpec{}, TaskSpec{TaskID: "given"}},
			task: `given`,
		},
		{
			name: "TaskID in a *TaskSpec",
			fn:   literal,
			opts: []TaskOption{TaskSpec{}, &TaskSpec{TaskID: "given"}},
			task: `given`,
		},
		{
			name: "no TaskID",
			fn:   ExtractRows,
			opts: []TaskOption{TaskSpec{}, TaskSpec{}},
			task: `ExtractRows`,
		},
		{
			name: "nil *TaskSpec after the second spec",
			fn:   ExtractRows,
			opts: []TaskOption{TaskSpec{}, TaskSpec{}, nilSpec},
			task: `ExtractRows`,
		},
		{
			name: "no TaskID and no function name",
			fn:   literal,
			opts: []TaskOption{TaskSpec{}, TaskSpec{}},
			task: `github\.com/apache/airflow/go-sdk/airflow\.` +
				`TestTaskRejectsASecondTaskSpec\.func\d+`,
		},
		{
			name: "TriggerDagRun and no TaskID",
			fn:   TriggerDagRun(TriggerDagRunSpec{DagID: "downstream_etl"}),
			opts: []TaskOption{TaskSpec{}, TaskSpec{}},
			task: `airflow\.TriggerDagRun`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { Dag("etl").Task(tt.fn, tt.opts...) })
			assert.Regexp(t,
				`^airflow\.DagRef\.Task: task "`+tt.task+`" of Dag "etl": got more than one `+
					`airflow\.TaskSpec; set all of the task's attributes in one TaskSpec$`,
				msg,
			)
		})
	}
}

func TestTaskRejectsADuplicateTaskID(t *testing.T) {
	dag := Dag("etl")
	dag.Task(extract)

	want := `airflow.DagRef.Task: Dag "etl" already has a task "extract"; ` +
		`set another task_id with airflow.TaskSpec{TaskID: ...}`
	assert.PanicsWithValue(t, want, func() { dag.Task(extract) })
	assert.PanicsWithValue(t, want, func() { dag.Task(ExtractRows, TaskSpec{TaskID: "extract"}) })
	assert.NotPanics(t,
		func() { Dag("reports").Task(extract) },
		"the same task_id in another Dag is a different task",
	)
}

func TestTaskPanicsOnBadFunction(t *testing.T) {
	var unassigned func(Context) error

	tests := []struct {
		name string
		fn   any
		want string
	}{
		{name: "not a function", fn: "extract", want: "fn is string, not a function"},
		{
			name: "nil function value",
			fn:   unassigned,
			want: "fn is a nil func(airflow.Context) error",
		},
		{
			name: "no leading Context",
			fn:   func(context.Context) error { return nil },
			want: "parameter 0 is context.Context, but the first parameter must be airflow.Context",
		},
		{
			// The name of a method expression keeps a dot, as a function literal's name does.
			// This row checks that the signature check rejects the method expression for its
			// receiver before Task looks at the name.
			name: "method expression",
			fn:   extractor.Extract,
			want: "parameter 0 is airflow.extractor, but the first parameter must be",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { Dag("etl").Task(tt.fn) })
			assert.Contains(t, msg, `airflow.DagRef.Task: Dag "etl": `)
			assert.Contains(t, msg, tt.want)
		})
	}
}

func TestTaskRejectsOptionsItDoesNotDefine(t *testing.T) {
	var unset TaskOption
	var nilSpec *TaskSpec
	const notDefined = ", which is not an option that package airflow defines"

	tests := []struct {
		name string
		opts []TaskOption
		want string
	}{
		{name: "nil", opts: []TaskOption{TaskSpec{}, unset}, want: "opts[1] is nil"},
		{
			name: "nil *TaskSpec",
			opts: []TaskOption{nilSpec},
			want: "opts[0] is a nil *airflow.TaskSpec",
		},
		{
			name: "struct that embeds a TaskSpec",
			opts: []TaskOption{wrappedSpec{TaskSpec: TaskSpec{TaskID: "x"}, MaxRetries: 5}},
			want: "opts[0] has type airflow.wrappedSpec" + notDefined,
		},
		{
			name: "struct that embeds a TaskOption",
			opts: []TaskOption{wrappedOption{TaskSpec{TaskID: "x"}}},
			want: "opts[0] has type airflow.wrappedOption" + notDefined,
		},
		{
			name: "struct that embeds a nil TaskOption",
			opts: []TaskOption{wrappedOption{}},
			want: "opts[0] has type airflow.wrappedOption" + notDefined,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t, `airflow.DagRef.Task: Dag "etl": `+tt.want, func() {
				Dag("etl").Task(extract, tt.opts...)
			})
		})
	}
}

func TestTaskAfterRegisterPanics(t *testing.T) {
	dag := Dag("etl")
	dag.Task(extract)
	Bundle().Register(dag)

	assert.PanicsWithValue(t,
		`airflow.DagRef.Task: Dag "etl" has already been registered; `+
			`add every task before Register`,
		func() { dag.Task(ExtractRows) },
	)
	assert.Len(t, dag.tasks, 1)
}

func TestTaskIsSafeForConcurrentUse(t *testing.T) {
	const workers, perWorker = 8, 100

	dag := Dag("etl")
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				dag.Task(extract, TaskSpec{TaskID: fmt.Sprintf("task_%d_%d", worker, i)})
			}
		}()
	}
	wg.Wait()

	assert.Len(t, dag.tasks, workers*perWorker)
}

// Task and Register both take the lock of the Dag, so each task is either added before the Dag
// is registered or not added at all. If Register marked the Dag registered without the lock,
// only a run with -race would fail.
func TestRegisterWhileTasksAreAdded(t *testing.T) {
	dag := Dag("etl")
	started := make(chan struct{})
	registered := make(chan struct{})
	type outcome struct {
		added     int
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
		for ; ; o.added++ {
			select {
			case <-registered:
				// Register has returned, so this Task call has to panic. If the call returns
				// instead, the goroutine stops after it, and the assertions below fail.
				dag.Task(extract, TaskSpec{TaskID: "after_register"})
				return
			default:
				dag.Task(extract, TaskSpec{TaskID: fmt.Sprintf("task_%d", o.added)})
			}
		}
	}()
	// Wait until the goroutine is running before calling Register. If the goroutine only started
	// after close(registered), that close would order every Task call after Register, and -race
	// could not catch a Register that skips the lock.
	<-started
	Bundle().Register(dag)
	close(registered)
	o := <-done

	assert.Equal(t,
		`airflow.DagRef.Task: Dag "etl" has already been registered; `+
			`add every task before Register`,
		o.recovered,
	)
	assert.Len(t, dag.tasks, o.added)
}

func TestTaskOptionIsSealed(t *testing.T) {
	typ := reflect.TypeFor[TaskOption]()
	require.Equal(t, 1, typ.NumMethod())
	// reflect reports a package path only for an unexported method.
	assert.Equal(t, "github.com/apache/airflow/go-sdk/airflow", typ.Method(0).PkgPath)
}

func TestTaskOptionRejectsForeignTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out, err := exec.Command("go", "build", "-o", os.DevNull, "./testdata/foreigntaskoption").
		CombinedOutput()

	require.Error(t, err, "a type defined outside package airflow must not compile as an option")
	// The test checks only the two type names, so that a change in the wording of the compiler
	// error does not break it.
	assert.Contains(t, string(out), "foreignOption")
	assert.Contains(t, string(out), "airflow.TaskOption")
}

// withStorage returns a value of type t that points at something, which is what makes a
// copy observable: a copy points at storage of its own to compare against.
func withStorage(t reflect.Type) reflect.Value {
	switch t.Kind() {
	case reflect.Pointer:
		return reflect.New(t.Elem())
	case reflect.Map:
		m := reflect.MakeMap(t)
		m.SetMapIndex(reflect.Zero(t.Key()), reflect.Zero(t.Elem()))
		return m
	default:
		return reflect.MakeSlice(t, 1, 1)
	}
}

// TestDagAndTaskCopyTheReferenceFieldsOfTheirSpecs pins the copy invariant DagRef
// documents. Assigning a spec copies a pointer, or a slice or map header, and not what it
// points at, so a caller that keeps what it passed could otherwise change a registered Dag
// through it. The fields are read from the spec types rather than named, so a field added to
// a generated spec is covered without this test being edited.
func TestDagAndTaskCopyTheReferenceFieldsOfTheirSpecs(t *testing.T) {
	for _, tt := range []struct {
		spec  any
		store func(spec reflect.Value) reflect.Value
	}{
		{
			spec: DagSpec{},
			store: func(spec reflect.Value) reflect.Value {
				return reflect.ValueOf(Dag("etl", spec.Interface().(DagSpec)).spec)
			},
		},
		{
			spec: TaskSpec{},
			store: func(spec reflect.Value) reflect.Value {
				return reflect.ValueOf(Dag("etl").Task(extract, spec.Interface().(TaskSpec)).spec)
			},
		},
	} {
		specType := reflect.TypeOf(tt.spec)
		for i := range specType.NumField() {
			field := specType.Field(i)
			switch field.Type.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map:
			default:
				continue
			}
			t.Run(specType.Name()+"."+field.Name, func(t *testing.T) {
				given := reflect.New(specType).Elem()
				given.Field(i).Set(withStorage(field.Type))

				stored := tt.store(given).Field(i)

				require.False(t, stored.IsNil(), "the field did not reach the registered spec")
				assert.NotEqual(
					t,
					given.Field(i).Pointer(),
					stored.Pointer(),
					"%s shares its contents with the caller; copy it in Dag or Task",
					field.Name,
				)
			})
		}
	}
}
