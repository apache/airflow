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
		// AddTask registers fn as a task, deriving the task_id from fn's own
		// name (so it must match the @task.stub name in the Python dag).
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
		AddTask(fn any)

		// AddTaskWithName is like AddTask but sets task_id explicitly instead of
		// deriving it from the function name. Use it when the Go function name
		// cannot match the Python @task.stub id, for example for an anonymous
		// function or a differing name.
		AddTaskWithName(taskId string, fn any)
	}

	// Registry is the recorder passed to BundleProvider.RegisterDags. Use it to
	// declare the dags this bundle can run; it also extends Bundle so the same
	// object serves task lookups at execution time.
	Registry interface {
		Bundle
		// AddDag registers a dag by its dag_id (matching the Python stub dag)
		// and returns a Dag handle for attaching tasks. Registering the same
		// dag_id twice panics.
		AddDag(dagId string) Dag
	}

	// TaskInfo describes a registered task by its user-visible id.
	TaskInfo struct {
		ID string
	}

	// DagInfo describes a registered dag together with its tasks in
	// registration order.
	DagInfo struct {
		DagID string
		Tasks []TaskInfo
	}

	// EnumerableBundle exposes the dag/task identity recorded by RegisterDags.
	// The default registry implements it; airflow-go-pack relies on it to read
	// a bundle's dag/task ids without executing any task.
	EnumerableBundle interface {
		OrderedDags() []DagInfo
	}

	registry struct {
		sync.RWMutex
		taskFuncMap map[string]map[string]Task
		dagOrder    []string
		taskOrder   map[string][]string
	}
)

type dagShim struct {
	dagId    string
	registry *registry
}

func (d dagShim) AddTask(fn any) {
	d.registry.registerTask(d.dagId, fn)
}

func (d dagShim) AddTaskWithName(taskId string, fn any) {
	d.registry.registerTaskWithName(d.dagId, taskId, fn)
}

// New returns an empty Registry on which dags and tasks can be registered. The
// runtime creates one and hands it to BundleProvider.RegisterDags, so bundle
// authors rarely call this directly; it is handy for unit-testing a
// RegisterDags implementation.
func New() Registry {
	return &registry{
		taskFuncMap: make(map[string]map[string]Task),
		taskOrder:   make(map[string][]string),
	}
}

func getFnName(fn reflect.Value) string {
	fullName := runtime.FuncForPC(fn.Pointer()).Name()
	parts := strings.Split(fullName, ".")
	fnName := parts[len(parts)-1]
	// Go adds `-fm` suffix to a method names
	return strings.TrimSuffix(fnName, "-fm")
}

func (r *registry) AddDag(dagId string) Dag {
	r.Lock()
	defer r.Unlock()

	if _, exists := r.taskFuncMap[dagId]; exists {
		panic(fmt.Errorf("Dag %q already exists in bundle", dagId))
	}
	r.taskFuncMap[dagId] = make(map[string]Task)
	r.dagOrder = append(r.dagOrder, dagId)
	return dagShim{dagId, r}
}

func (r *registry) registerTask(dagId string, fn any) {
	val := reflect.ValueOf(fn)

	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}

	fnName := getFnName(val)

	r.registerTaskWithName(dagId, fnName, fn)
}

func (r *registry) registerTaskWithName(dagId, taskId string, fn any) {
	task, err := NewTaskFunction(fn)
	if err != nil {
		panic(fmt.Errorf("error registering task %q for DAG %q: %w", taskId, dagId, err))
	}

	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()

	dagTasks, exists := r.taskFuncMap[dagId]

	if !exists {
		dagTasks = make(map[string]Task)
		r.taskFuncMap[dagId] = dagTasks
		r.dagOrder = append(r.dagOrder, dagId)
	}

	_, exists = dagTasks[taskId]
	if exists {
		panic(fmt.Errorf("taskId %q is already registered for DAG %q", taskId, dagId))
	}
	dagTasks[taskId] = task
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

// OrderedDags returns the registered dags in AddDag order, each with its tasks
// in registration order. The returned slice is freshly allocated; callers may
// mutate it freely.
func (r *registry) OrderedDags() []DagInfo {
	r.RLock()
	defer r.RUnlock()

	out := make([]DagInfo, 0, len(r.dagOrder))
	for _, dagID := range r.dagOrder {
		taskIDs := r.taskOrder[dagID]
		tasks := make([]TaskInfo, 0, len(taskIDs))
		for _, tid := range taskIDs {
			tasks = append(tasks, TaskInfo{ID: tid})
		}
		out = append(out, DagInfo{DagID: dagID, Tasks: tasks})
	}
	return out
}
