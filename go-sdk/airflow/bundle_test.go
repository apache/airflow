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
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundlev1"
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
		task, ok := b.tasks.LookupTask(id[0], id[1])
		assert.True(t, ok, "%s.%s must be registered", id[0], id[1])
		assert.NotNil(t, task)
	}

	_, ok := b.tasks.LookupTask("reports", "transform")
	assert.False(t, ok, "transform is registered for py_etl only")
	_, ok = b.tasks.LookupTask("unknown", "extract")
	assert.False(t, ok)
}

func TestRegisterKeepsRegistrationOrder(t *testing.T) {
	b := Bundle()
	assert.Empty(t, b.tasks.OrderedDags())

	// Out of alphabetical order, so the test fails if OrderedDags sorts instead of keeping
	// registration order.
	b.Register(
		TaskHandler("zeta", "z2", noop),
		TaskHandler("alpha", "a1", noop),
		TaskHandler("zeta", "z1", noop),
	)

	assert.Equal(t, []bundlev1.DagInfo{
		{DagID: "zeta", Tasks: []bundlev1.TaskInfo{{ID: "z2"}, {ID: "z1"}}},
		{DagID: "alpha", Tasks: []bundlev1.TaskInfo{{ID: "a1"}}},
	}, b.tasks.OrderedDags())
}

// A package that defines task handlers exports a function like etlHandlers, and main registers
// what it returns.
func etlHandlers() []Registraterable {
	return []Registraterable{
		TaskHandler("py_etl", "extract", noop),
		TaskHandler("py_etl", "transform", noop),
	}
}

func TestRegisterTakesASliceOfHandlers(t *testing.T) {
	b := Bundle()
	b.Register(etlHandlers()...)
	b.Register(TaskHandler("reports", "render", noop))

	assert.Equal(t, []bundlev1.DagInfo{
		{DagID: "py_etl", Tasks: []bundlev1.TaskInfo{{ID: "extract"}, {ID: "transform"}}},
		{DagID: "reports", Tasks: []bundlev1.TaskInfo{{ID: "render"}}},
	}, b.tasks.OrderedDags())
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
				b.tasks.LookupTask("py_etl", "task_0_0")
				b.tasks.OrderedDags()
			}
		}()
	}
	wg.Wait()

	dags := b.tasks.OrderedDags()
	require.Len(t, dags, 1)
	assert.Len(t, dags[0].Tasks, workers*perWorker)
}

func TestRegisterRejectsNilItem(t *testing.T) {
	var item Registraterable
	assert.PanicsWithValue(t, "airflow.BundleRef.Register: cannot register <nil>", func() {
		Bundle().Register(item)
	})
}

// Embedding promotes the unexported method, so this struct compiles as a Registraterable.
type wrappedItem struct{ Registraterable }

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

func TestRegistraterableIsSealed(t *testing.T) {
	typ := reflect.TypeFor[Registraterable]()
	require.Equal(t, 1, typ.NumMethod())
	// reflect reports a package path only for an unexported method.
	assert.Equal(t, "github.com/apache/airflow/go-sdk/airflow", typ.Method(0).PkgPath)
}

func TestRegistraterableRejectsForeignTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out, err := exec.Command("go", "build", "-o", os.DevNull, "./testdata/foreignitem").
		CombinedOutput()

	require.Error(t, err, "a type defined outside package airflow must not compile as an item")
	assert.Contains(t, string(out), "foreignItem does not implement airflow.Registraterable")
	assert.Contains(t, string(out), "unexported method registraterable")
}
