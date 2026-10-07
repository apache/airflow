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
	"io"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

func noop(Context) error { return nil }

func TestRegisterMakesTasksFindable(t *testing.T) {
	b := Bundle()
	b.Register(
		TaskHandler("py_etl", "extract", noop),
		TaskHandler("py_etl", "transform", noop),
		TaskHandler("reports", "extract", noop),
	)

	registered := [][2]string{
		{"py_etl", "extract"},
		{"py_etl", "transform"},
		{"reports", "extract"},
	}
	for _, id := range registered {
		task, ok := b.taskHandlers.LookupTask(id[0], id[1])
		assert.True(t, ok, "%s.%s must be registered", id[0], id[1])
		assert.NotNil(t, task)
	}

	_, ok := b.taskHandlers.LookupTask("reports", "transform")
	assert.False(t, ok, "transform is registered for py_etl only")
	_, ok = b.taskHandlers.LookupTask("unknown", "extract")
	assert.False(t, ok)
}

func TestRegisterKeepsRegistrationOrder(t *testing.T) {
	b := Bundle()
	assert.Empty(t, b.taskHandlers.ListTaskHandlers())

	// Out of alphabetical order, so the test fails if ListTaskHandlers sorts instead of keeping
	// registration order.
	b.Register(
		TaskHandler("zeta", "z2", noop),
		TaskHandler("alpha", "a1", noop),
		TaskHandler("zeta", "z1", noop),
	)

	assert.Equal(t, []bundle.TaskHandlerInfo{
		{DagID: "zeta", TaskID: "z2"},
		{DagID: "alpha", TaskID: "a1"},
		{DagID: "zeta", TaskID: "z1"},
	}, b.taskHandlers.ListTaskHandlers())

	listed := b.taskHandlers.ListTaskHandlers()
	listed[0].TaskID = "changed by the caller"
	assert.Equal(t, "z2", b.taskHandlers.ListTaskHandlers()[0].TaskID)
}

// A package that defines task handlers exports a function like etlHandlers, and main registers
// what it returns.
func etlHandlers() []Registerable {
	return []Registerable{
		TaskHandler("py_etl", "extract", noop),
		TaskHandler("py_etl", "transform", noop),
	}
}

func TestRegisterTakesASliceOfHandlers(t *testing.T) {
	b := Bundle()
	b.Register(etlHandlers()...)
	b.Register(TaskHandler("reports", "render", noop))

	assert.Equal(t, []bundle.TaskHandlerInfo{
		{DagID: "py_etl", TaskID: "extract"},
		{DagID: "py_etl", TaskID: "transform"},
		{DagID: "reports", TaskID: "render"},
	}, b.taskHandlers.ListTaskHandlers())
}

func TestRegisterRejectsDuplicateTask(t *testing.T) {
	b := Bundle()
	b.Register(TaskHandler("py_etl", "transform", noop))

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: task "transform" of Dag "py_etl" is already registered`,
		func() { b.Register(TaskHandler("py_etl", "transform", noop)) },
	)
	assert.NotPanics(t,
		func() { b.Register(TaskHandler("reports", "transform", noop)) },
		"the same task_id under another dag_id is a different task",
	)
}

func TestRegisterTakesTaskHandlersAndDagsTogether(t *testing.T) {
	etl := Dag("etl")
	etl.Task(extract)

	b := Bundle()
	b.Register(TaskHandler("py_etl", "transform", noop), etl)

	_, ok := b.taskHandlers.LookupTask("py_etl", "transform")
	assert.True(t, ok)
	assert.Same(t, etl, b.dags.dags["etl"])
	assert.True(t, etl.registered)
}

func TestRegisterRejectsDuplicateDag(t *testing.T) {
	etl := Dag("etl")
	b := Bundle()
	b.Register(etl)

	want := `airflow.BundleRef.Register: Dag "etl" is already registered`
	assert.PanicsWithValue(t, want, func() { b.Register(etl) })
	second := Dag("etl")
	assert.PanicsWithValue(t, want, func() { b.Register(second) })
	assert.Same(t, etl, b.dags.dags["etl"])
	assert.False(t, second.registered, "a Dag that Register rejects can still take tasks")
}

func TestRegisterRejectsADagWithTheDagIDOfATaskHandler(t *testing.T) {
	tests := []struct {
		name     string
		register func(b *BundleRef, handler Registerable, dag *DagRef)
	}{
		{
			name: "separate calls",
			register: func(b *BundleRef, handler Registerable, dag *DagRef) {
				b.Register(handler)
				b.Register(dag)
			},
		},
		{
			name: "one call",
			register: func(b *BundleRef, handler Registerable, dag *DagRef) {
				b.Register(handler, dag)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Bundle()
			dag := Dag("etl")

			want := `airflow.BundleRef.Register: Dag "etl" already has task handlers from ` +
				`airflow.TaskHandler, so it cannot also be registered as a Dag from airflow.Dag`
			assert.PanicsWithValue(t, want, func() {
				tt.register(b, TaskHandler("etl", "transform", noop), dag)
			})
			_, ok := b.taskHandlers.LookupTask("etl", "transform")
			assert.True(t, ok)
			assert.NotContains(t, b.dags.dags, "etl")
			assert.NotPanics(t, func() { dag.Task(extract) },
				"a Dag that Register rejects can still take tasks")
		})
	}
}

func TestRegisterRejectsATaskHandlerWithTheDagIDOfADag(t *testing.T) {
	tests := []struct {
		name     string
		register func(b *BundleRef, handler Registerable, dag *DagRef)
	}{
		{
			name: "separate calls",
			register: func(b *BundleRef, handler Registerable, dag *DagRef) {
				b.Register(dag)
				b.Register(handler)
			},
		},
		{
			name: "one call",
			register: func(b *BundleRef, handler Registerable, dag *DagRef) {
				b.Register(dag, handler)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Bundle()
			dag := Dag("etl")

			want := `airflow.BundleRef.Register: Dag "etl" is already registered as a Dag from ` +
				`airflow.Dag, so it cannot also have task handlers from airflow.TaskHandler`
			assert.PanicsWithValue(t, want, func() {
				tt.register(b, TaskHandler("etl", "transform", noop), dag)
			})
			assert.Same(t, dag, b.dags.dags["etl"])
			assert.NotContains(t, b.taskHandlers.handlers, "etl")
			assert.Empty(t, b.taskHandlers.ListTaskHandlers())
		})
	}
}

// TestRegisterRejectsACycle covers the check that reads a Dag's edges as a whole. Before and
// After record an edge without walking the graph, so a cycle between other tasks is Register's to
// find, and every edge of the call that closed it is recorded until then.
func TestRegisterRejectsACycle(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	notified := orderedTask(t, dag, "notify")
	extracted.Before(loaded)
	loaded.Before(notified, extracted)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`extract -> load -> extract`,
		func() { Bundle().Register(dag) },
	)
	// The verbs recorded what they were given, and the Dag can still be corrected.
	assertTasks(t, loaded.downstreams, notified, extracted)
	assert.False(t, dag.registered)
}

// TestRegisterRejectsACycleThroughAnInputsEdge pins that the check sees the edges Inputs
// declared, which is what DagRef.Task recording them is for. It is the case ADR-0008 names:
// b := dag.Task(B, Inputs(a)) followed by b.Before(a) is a genuine cycle in accepted syntax.
func TestRegisterRejectsACycleThroughAnInputsEdge(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))
	notified := orderedTask(t, dag, "notify")
	counted.Before(notified)
	counted.Before(read)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`readRows -> countRows -> readRows`,
		func() { Bundle().Register(dag) },
	)
}

// TestRegisterTakesADagWhoseTasksShareADownstream pins that the walk follows a diamond, where a
// task is reached twice without any cycle.
func TestRegisterTakesADagWhoseTasksShareADownstream(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")
	done := orderedTask(t, dag, "done")
	extracted.Before(notified, cleaned).Before(done)

	Bundle().Register(dag)

	assert.True(t, dag.registered)
}

func TestRegisterRejectsNilDag(t *testing.T) {
	var dag *DagRef
	assert.PanicsWithValue(t, "airflow.BundleRef.Register: cannot register a nil *airflow.DagRef",
		func() { Bundle().Register(dag) },
	)
}

func TestRegisterAfterServePanics(t *testing.T) {
	b := Bundle()
	b.Register(TaskHandler("py_etl", "transform", noop))
	require.NoError(t, b.serve([]string{"--airflow-metadata"}, io.Discard))

	want := "airflow.BundleRef.Register: Serve has already been called; " +
		"register everything before Serve"
	assert.PanicsWithValue(t, want, func() { b.Register(TaskHandler("py_etl", "load", noop)) })
	assert.PanicsWithValue(t, want, func() { b.Register(Dag("etl")) })
}

// The flag lives on the bundle, not on the task-handler map, so a kind of item added to
// Register later is covered without a flag of its own.
func TestRegisterAfterServeRejectsEveryKindOfItem(t *testing.T) {
	b := Bundle()
	require.NoError(t, b.serve([]string{"--airflow-metadata"}, io.Discard))

	var nilItem Registerable
	assert.Panics(t, func() { b.Register(nilItem) },
		"the closed check runs before Register looks at what the item is")
}

// Serve closes registration whatever the run does, so a bundle that only printed its usage
// still refuses a late Register.
func TestRegisterAfterAFailedServePanics(t *testing.T) {
	b := Bundle()
	require.NoError(t, b.serve([]string{"--help"}, io.Discard))

	assert.Panics(t, func() { b.Register(TaskHandler("py_etl", "transform", noop)) })
}

func TestRegisterIsSafeForConcurrentUse(t *testing.T) {
	const workers, perWorker = 8, 100

	b := Bundle()
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				b.Register(TaskHandler("py_etl", fmt.Sprintf("task_%d_%d", worker, i), noop))
				b.Register(Dag(fmt.Sprintf("dag_%d_%d", worker, i)))
				b.taskHandlers.LookupTask("py_etl", "task_0_0")
				b.taskHandlers.ListTaskHandlers()
			}
		}()
	}
	wg.Wait()

	assert.Len(t, b.taskHandlers.ListTaskHandlers(), workers*perWorker)
	assert.Len(t, b.dags.dags, workers*perWorker)
}

// For each dag_id, one goroutine registers a task handler and another registers a Dag at the
// same time, and exactly one of the two calls must succeed. Without BundleRef.mu both can
// succeed. A run with -race does not report that, because every map access still takes the
// map's lock.
func TestRegisterGivesEachDagIDToOneKindUnderConcurrentUse(t *testing.T) {
	const dagCount = 200

	b := Bundle()
	start := make(chan struct{})
	var wg sync.WaitGroup
	register := func(item Registerable) {
		defer wg.Done()
		defer func() { recover() }()
		<-start
		b.Register(item)
	}
	dags := make([]*DagRef, dagCount)
	for i := range dags {
		dags[i] = Dag(fmt.Sprintf("dag_%d", i))
		wg.Add(2)
		go register(TaskHandler(dags[i].dagID, "transform", noop))
		go register(dags[i])
	}
	close(start)
	wg.Wait()

	var wrong []string
	for _, dag := range dags {
		_, hasHandler := b.taskHandlers.LookupTask(dag.dagID, "transform")
		if hasHandler == dag.registered {
			wrong = append(wrong, dag.dagID)
		}
	}
	assert.Empty(t, wrong,
		"each dag_id must end up with a task handler or a Dag, not both or neither")
}

func TestRegisterRejectsNilItem(t *testing.T) {
	var item Registerable
	assert.PanicsWithValue(t, "airflow.BundleRef.Register: cannot register <nil>", func() {
		Bundle().Register(item)
	})
}

// Embedding promotes the unexported method, so this struct compiles as a Registerable.
type wrappedItem struct{ Registerable }

func TestRegisterRejectsEmbeddedItem(t *testing.T) {
	item := wrappedItem{TaskHandler("py_etl", "transform", noop)}
	assert.PanicsWithValue(
		t,
		"airflow.BundleRef.Register: cannot register airflow.wrappedItem",
		func() {
			Bundle().Register(item)
		},
	)
}

func TestRegisterableIsSealed(t *testing.T) {
	typ := reflect.TypeFor[Registerable]()
	require.Equal(t, 1, typ.NumMethod())
	// reflect reports a package path only for an unexported method.
	assert.Equal(t, "github.com/apache/airflow/go-sdk/airflow", typ.Method(0).PkgPath)
}

func TestRegisterableRejectsForeignTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out, err := exec.Command("go", "build", "-o", os.DevNull, "./testdata/foreignitem").
		CombinedOutput()

	require.Error(t, err, "a type defined outside package airflow must not compile as an item")
	assert.Contains(t, string(out), "foreignItem does not implement airflow.Registerable")
	assert.Contains(t, string(out), "unexported method registerable")
}

func TestSerializeDagsKeepsTheOtherDagsWhenADagCannotBeSerialized(t *testing.T) {
	b := Bundle()
	etl := Dag("etl")
	etl.Task(noop)
	b.Register(etl)
	// Register checks and expands every Dag it takes. A Dag that skipped Register therefore stands
	// in for a Dag that the serializer fails on.
	broken := Dag("broken")
	b.dags.dags["broken"] = broken
	b.dags.order = append(b.dags.order, broken)
	reports := Dag("reports")
	reports.Task(noop)
	b.Register(reports)

	serialized := coordinatorBundle{b}.SerializeDags("/bundles/go/etl", "etl")

	require.Len(t, serialized, 3)
	assert.Equal(t, bundle.SerializedDag{
		DagID: "etl",
		Data:  etl.serialize("/bundles/go/etl", "etl"),
	}, serialized[0])
	assert.Equal(t, "broken", serialized[1].DagID)
	assert.Nil(t, serialized[1].Data)
	assert.ErrorContains(t, serialized[1].Err, `Dag "broken" is not registered`)
	assert.Equal(t, bundle.SerializedDag{
		DagID: "reports",
		Data:  reports.serialize("/bundles/go/etl", "etl"),
	}, serialized[2])
}

func TestSerializeDagsLeavesOutADagThatRegisterRejected(t *testing.T) {
	b := Bundle()
	cyclic := Dag("cyclic")
	extracted := orderedTask(t, cyclic, "extract")
	loaded := orderedTask(t, cyclic, "load")
	extracted.Before(loaded)
	loaded.Before(extracted)
	require.Panics(t, func() { b.Register(cyclic) })

	assert.Empty(t, coordinatorBundle{b}.SerializeDags("/bundles/go/etl", "etl"))
}
