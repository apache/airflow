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

package bundlev1

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/apache/airflow/go-sdk/pkg/worker"
)

type (
	// Task is one registered task: something the runtime can Execute. Bundle
	// authors do not implement this directly; Dag.AddTask wraps a plain Go
	// function into a Task for you.
	Task = worker.Task

	// Bundle is the execution-time view of a registry: it looks up a task by
	// dag_id and task_id. Registry embeds it so the object built during
	// RegisterDags can also serve tasks when they run.
	Bundle = worker.Bundle

	// Dag is the handle returned by Registry.AddDag. Use it to attach the Go
	// functions that implement the dag's tasks.
	Dag interface {
		// AddTask registers fn as a task in this Dag using fn's Go name as
		// the task id (so it must match the @task.stub name in the Python
		// dag). spec carries optional per-task configuration (pass TaskSpec{}
		// for defaults). depends lists task ids in the same Dag that must run
		// before this one; each must already be registered. Pass nil for no
		// dependencies.
		//
		// fn is an ordinary Go function whose parameters are injected by type
		// and may appear in any order. Recognised parameters are:
		//   - context.Context: cancelled when the task is asked to stop
		//   - *slog.Logger: writes to the task's Airflow log
		//   - sdk.Client (or a narrower sdk.VariableClient / sdk.ConnectionClient /
		//     sdk.XComClient): access to Variables, Connections, and XCom
		//
		// fn must return either error or (result, error): a non-nil error fails
		// the task, and a non-nil first result is pushed as the task's
		// return-value XCom. Passing a non-function, or a function whose return
		// signature does not match, panics at registration time.
		AddTask(fn any, spec TaskSpec, depends []string)

		// AddTaskWithName is like AddTask but sets task_id explicitly instead
		// of deriving it from the function name. Use it when the Go function
		// name cannot match the Python @task.stub id, for example for an
		// anonymous function or a differing name.
		AddTaskWithName(taskId string, fn any, spec TaskSpec, depends []string)
	}

	// Registry is the recorder passed to BundleProvider.RegisterDags. Use it to
	// declare the dags this bundle can run; it also extends Bundle so the same
	// object serves task lookups at execution time.
	Registry interface {
		Bundle
		// AddDag registers a dag by its dag_id (matching the Python stub dag)
		// and returns a Dag handle for attaching tasks. An optional DagSpec
		// configures dag-level attributes such as schedule and tags; passing
		// more than one spec panics. Registering the same dag_id twice panics.
		AddDag(dagId string, spec ...DagSpec) Dag
	}

	// EnumerableBundle exposes the dag/task identity recorded by
	// RegisterDags. The default registry implements it; the coordinator-mode
	// runtime relies on it for the DAG-parse one-shot.
	EnumerableBundle interface {
		OrderedDags() []DagInfo
	}

	registry struct {
		sync.RWMutex
		taskFuncMap map[string]map[string]Task
		taskInfo    map[string]map[string]TaskInfo
		dagSpec     map[string]DagSpec
		dagOrder    []string
		taskOrder   map[string][]string
	}
)

type dagShim struct {
	dagId    string
	registry *registry
}

func (d dagShim) AddTask(fn any, spec TaskSpec, depends []string) {
	d.registry.registerTask(d.dagId, fn, spec, depends)
}

func (d dagShim) AddTaskWithName(taskId string, fn any, spec TaskSpec, depends []string) {
	d.registry.registerTaskWithName(d.dagId, taskId, fn, spec, depends)
}

func optionalSpec[T any](specs []T, caller string) T {
	switch len(specs) {
	case 0:
		var zero T
		return zero
	case 1:
		return specs[0]
	default:
		panic(fmt.Errorf("%s accepts at most one spec, got %d", caller, len(specs)))
	}
}

// New returns an empty Registry on which dags and tasks can be registered. The
// runtime creates one and hands it to BundleProvider.RegisterDags, so bundle
// authors rarely call this directly; it is handy for unit-testing a
// RegisterDags implementation.
func New() Registry {
	return &registry{
		taskFuncMap: make(map[string]map[string]Task),
		taskInfo:    make(map[string]map[string]TaskInfo),
		dagSpec:     make(map[string]DagSpec),
		taskOrder:   make(map[string][]string),
	}
}

func splitFullName(fullName string) (typeName, pkgPath string) {
	// fullName looks like "main.extract" or "github.com/x/y.MyTask"; method
	// values get a "-fm" suffix.
	lastDot := strings.LastIndex(fullName, ".")
	if lastDot < 0 {
		return strings.TrimSuffix(fullName, "-fm"), ""
	}
	return strings.TrimSuffix(fullName[lastDot+1:], "-fm"), fullName[:lastDot]
}

func getFnName(fn reflect.Value) string {
	fullName := runtime.FuncForPC(fn.Pointer()).Name()
	name, _ := splitFullName(fullName)
	return name
}

func (r *registry) AddDag(dagId string, spec ...DagSpec) Dag {
	dagSpec := optionalSpec(spec, "AddDag")
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	if _, exists := r.taskFuncMap[dagId]; exists {
		panic(fmt.Errorf("Dag %q already exists in bundle", dagId))
	}
	r.taskFuncMap[dagId] = make(map[string]Task)
	r.taskInfo[dagId] = make(map[string]TaskInfo)
	r.dagSpec[dagId] = dagSpec
	r.dagOrder = append(r.dagOrder, dagId)
	return dagShim{dagId, r}
}

func (r *registry) registerTask(dagId string, fn any, spec TaskSpec, depends []string) {
	val := reflect.ValueOf(fn)

	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}

	fnName := getFnName(val)

	r.registerTaskWithName(dagId, fnName, fn, spec, depends)
}

func (r *registry) registerTaskWithName(
	dagId, taskId string,
	fn any,
	spec TaskSpec,
	depends []string,
) {
	task, err := NewTaskFunction(fn)
	if err != nil {
		panic(fmt.Errorf("error registering task %q for DAG %q: %w", taskId, dagId, err))
	}

	val := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(val.Pointer()).Name()
	typeName, pkgPath := splitFullName(fullName)

	info := TaskInfo{ID: taskId, TypeName: typeName, PkgPath: pkgPath, Spec: spec}

	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()

	dagTasks, exists := r.taskFuncMap[dagId]
	if !exists {
		dagTasks = make(map[string]Task)
		r.taskFuncMap[dagId] = dagTasks
		r.taskInfo[dagId] = make(map[string]TaskInfo)
		r.dagOrder = append(r.dagOrder, dagId)
	}

	if _, exists := dagTasks[taskId]; exists {
		panic(fmt.Errorf("taskId %q is already registered for DAG %q", taskId, dagId))
	}

	// Resolve depends to upstream TaskInfo entries, validating each exists.
	// We dedupe so a repeated id in `depends` only records one downstream
	// edge on the parent.
	seen := make(map[string]bool, len(depends))
	for _, dep := range depends {
		if dep == taskId {
			panic(fmt.Errorf("task %q cannot depend on itself in DAG %q", taskId, dagId))
		}
		if seen[dep] {
			continue
		}
		seen[dep] = true
		parent, ok := r.taskInfo[dagId][dep]
		if !ok {
			panic(fmt.Errorf(
				"task %q depends on unknown task %q in DAG %q; register upstream tasks first",
				taskId, dep, dagId,
			))
		}
		parent.Downstream = append(parent.Downstream, taskId)
		r.taskInfo[dagId][dep] = parent
	}

	dagTasks[taskId] = task
	r.taskInfo[dagId][taskId] = info
	r.taskOrder[dagId] = append(r.taskOrder[dagId], taskId)
}

func (r *registry) LookupTask(dagId, taskId string) (task Task, exists bool) {
	r.RLock()
	defer r.RUnlock()

	dagTasks, exists := r.taskFuncMap[dagId]
	if !exists {
		return nil, false
	}
	task, exists = dagTasks[taskId]
	return task, exists
}

// OrderedDags returns the registered dags in the order AddDag was called,
// each with its tasks in the order AddTask / AddTaskWithName was called. The
// returned slice is freshly allocated; callers may mutate it freely.
func (r *registry) OrderedDags() []DagInfo {
	r.RLock()
	defer r.RUnlock()

	out := make([]DagInfo, 0, len(r.dagOrder))
	for _, dagID := range r.dagOrder {
		taskIDs := r.taskOrder[dagID]
		tasks := make([]TaskInfo, 0, len(taskIDs))
		for _, tid := range taskIDs {
			tasks = append(tasks, r.taskInfo[dagID][tid])
		}
		out = append(out, DagInfo{DagID: dagID, Spec: r.dagSpec[dagID], Tasks: tasks})
	}
	return out
}
