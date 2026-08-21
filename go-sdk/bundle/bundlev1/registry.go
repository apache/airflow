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
	"unicode"
	"unicode/utf8"
)

type (
	// Dag is the handle returned by Registry.AddDag. Use it to attach the Go
	// functions that implement the dag's tasks.
	Dag interface {
		// AddTask registers fn under its Go name. depends names
		// already-registered upstream tasks; the optional spec configures the
		// task.
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
		AddTask(fn any, depends []string, spec ...TaskSpec)

		// AddTaskWithName is like AddTask but sets task_id explicitly instead of
		// deriving it from the function name. Use it when the Go function name
		// cannot match the Python @task.stub id, for example for an anonymous
		// function or a differing name.
		AddTaskWithName(taskId string, fn any, depends []string, spec ...TaskSpec)
	}

	// Registry is the recorder passed to BundleProvider.RegisterDags. Use it to
	// declare the dags this bundle can run; it also extends Bundle so the same
	// object serves task lookups at execution time.
	Registry interface {
		Bundle
		// AddDag registers dagId with one optional spec. Duplicates and extra specs panic.
		AddDag(dagId string, spec ...DagSpec) Dag
	}

	// EnumerableBundle exposes registered Dag and task metadata.
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

func (d dagShim) AddTask(fn any, depends []string, spec ...TaskSpec) {
	d.registry.registerTask(d.dagId, fn, depends, optionalSpec(spec, "AddTask"))
}

func (d dagShim) AddTaskWithName(taskId string, fn any, depends []string, spec ...TaskSpec) {
	d.registry.registerTaskWithName(
		d.dagId,
		taskId,
		fn,
		depends,
		optionalSpec(spec, "AddTaskWithName"),
	)
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
	// Method values end in "-fm".
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

const maxIDLength = 250

func validateID(kind, id string) {
	if length := utf8.RuneCountInString(id); length > maxIDLength {
		panic(
			fmt.Errorf(
				"%s %q must be at most %d characters, got %d",
				kind,
				id,
				maxIDLength,
				length,
			),
		)
	}
	for _, r := range id {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r == '-' || r == '.' {
			continue
		}
		panic(
			fmt.Errorf(
				"%s %q may contain only letters, numbers, dashes, dots, and underscores",
				kind,
				id,
			),
		)
	}
	if id == "" {
		panic(fmt.Errorf("%s must not be empty", kind))
	}
}

func normalizeDagSpec(dagID string, spec DagSpec) DagSpec {
	if spec.Schedule != "@continuous" {
		return spec
	}
	if spec.MaxActiveRuns > 1 {
		panic(fmt.Errorf(
			"Dag %q uses @continuous and requires MaxActiveRuns <= 1, got %d",
			dagID,
			spec.MaxActiveRuns,
		))
	}
	if spec.MaxActiveRuns == 0 {
		spec.MaxActiveRuns = 1
	}
	return spec
}

func (r *registry) AddDag(dagId string, spec ...DagSpec) Dag {
	validateID("Dag ID", dagId)
	dagSpec := normalizeDagSpec(dagId, optionalSpec(spec, "AddDag"))
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

func (r *registry) registerTask(dagId string, fn any, depends []string, spec TaskSpec) {
	val := reflect.ValueOf(fn)

	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}

	fnName := getFnName(val)

	r.registerTaskWithName(dagId, fnName, fn, depends, spec)
}

func (r *registry) registerTaskWithName(
	dagId, taskId string,
	fn any,
	depends []string,
	spec TaskSpec,
) {
	validateID("task ID", taskId)
	doXComPush := spec.DoXComPush == nil || *spec.DoXComPush
	task, err := newTaskFunction(fn, doXComPush)
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

	// Record one downstream edge per dependency.
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

// OrderedDags returns a mutable copy of Dags and tasks in registration order.
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
