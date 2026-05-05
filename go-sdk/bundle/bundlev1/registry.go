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
	Task   = worker.Task
	Bundle = worker.Bundle

	Dag interface {
		AddTask(fn any)
		AddTaskWithName(taskId string, fn any)
	}

	// Registry defines the interface that lets user code add dags and tasks, and extends Bundle for execution
	// time
	Registry interface {
		Bundle
		AddDag(dagId string) Dag
	}

	// TaskInfo describes a registered task. Coordinator-mode DAG parsing uses
	// it to render the per-task block of a DagFileParsingResult.
	TaskInfo struct {
		// ID is the user-visible task id (the function name unless overridden
		// via AddTaskWithName).
		ID string
		// TypeName is the unqualified Go function name (e.g. "extract").
		TypeName string
		// PkgPath is the Go package path (e.g. "main", "github.com/x/y").
		PkgPath string
	}

	// DagInfo describes a registered dag together with its tasks in
	// registration order.
	DagInfo struct {
		DagID string
		Tasks []TaskInfo
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

// Function New creates a new bundle on which Dag and Tasks can be registered
func New() Registry {
	return &registry{
		taskFuncMap: make(map[string]map[string]Task),
		taskInfo:    make(map[string]map[string]TaskInfo),
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

func (r *registry) AddDag(dagId string) Dag {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	if _, exists := r.taskFuncMap[dagId]; exists {
		panic(fmt.Errorf("Dag %q already exists in bundle", dagId))
	}
	r.taskFuncMap[dagId] = make(map[string]Task)
	r.taskInfo[dagId] = make(map[string]TaskInfo)
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

	val := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(val.Pointer()).Name()
	typeName, pkgPath := splitFullName(fullName)

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

	dagTasks[taskId] = task
	r.taskInfo[dagId][taskId] = TaskInfo{ID: taskId, TypeName: typeName, PkgPath: pkgPath}
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
		out = append(out, DagInfo{DagID: dagID, Tasks: tasks})
	}
	return out
}
