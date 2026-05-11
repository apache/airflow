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
	"time"

	"github.com/apache/airflow/go-sdk/pkg/worker"
)

type (
	Task   = worker.Task
	Bundle = worker.Bundle

	Dag interface {
		AddTask(fn any, spec ...TaskSpec)
		AddTaskWithName(taskId string, fn any, spec ...TaskSpec)
	}

	// Registry defines the interface that lets user code add dags and tasks, and extends Bundle for execution
	// time
	Registry interface {
		Bundle
		AddDag(dagId string, spec ...DagSpec) Dag
	}

	// TaskSpec is the optional configuration applied to a task at registration
	// time. Every field is optional: a zero value means "unset" and the
	// scheduler falls back to its serialization-schema default. The field
	// names mirror the keys defined under "operator" in
	// airflow-core/src/airflow/serialization/schema.json.
	TaskSpec struct {
		Queue                   string
		Pool                    string
		PoolSlots               int
		Retries                 int
		RetryDelay              time.Duration
		MaxRetryDelay           time.Duration
		RetryExponentialBackoff float64
		PriorityWeight          int
		WeightRule              string
		TriggerRule             string
		Owner                   string
		ExecutionTimeout        time.Duration
		Executor                string
		StartDate               time.Time
		EndDate                 time.Time
		DependsOnPast           bool
		WaitForDownstream       bool
		// DoXComPush, EmailOnFailure, and EmailOnRetry default to true in the
		// scheduler. A nil pointer means "unset" so the field is omitted from
		// the serialized payload; pass Bool(false) to explicitly opt out.
		DoXComPush            *bool
		EmailOnFailure        *bool
		EmailOnRetry          *bool
		DocMD                 string
		MapIndexTemplate      string
		MaxActiveTisPerDag    int
		MaxActiveTisPerDagrun int
	}

	// DagSpec is the optional configuration applied to a DAG at registration
	// time. Every field is optional: a zero value means "unset" and the
	// scheduler falls back to its serialization-schema default. The field
	// names mirror the keys defined under "dag" in
	// airflow-core/src/airflow/serialization/schema.json.
	DagSpec struct {
		// Schedule is "@once", "@continuous", a cron expression, or "" for
		// NullTimetable (no schedule).
		Schedule                    string
		Description                 string
		StartDate                   time.Time
		EndDate                     time.Time
		Tags                        []string
		DagDisplayName              string
		DocMD                       string
		MaxActiveTasks              int
		MaxActiveRuns               int
		MaxConsecutiveFailedDagRuns int
		DagrunTimeout               time.Duration
		Catchup                     bool
		FailFast                    bool
		RenderTemplateAsNativeObj   bool
		DisableBundleVersioning     bool
		// IsPausedUponCreation has no schema default. nil means "unset"; pass
		// Bool(true) or Bool(false) to set it explicitly.
		IsPausedUponCreation *bool
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
		// Spec carries the optional per-task configuration supplied at
		// registration. The zero value means "no overrides".
		Spec TaskSpec
	}

	// DagInfo describes a registered dag together with its tasks in
	// registration order.
	DagInfo struct {
		DagID string
		// Spec carries the optional per-dag configuration supplied at
		// registration. The zero value means "no overrides".
		Spec  DagSpec
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
		dagSpec     map[string]DagSpec
		dagOrder    []string
		taskOrder   map[string][]string
	}
)

type dagShim struct {
	dagId    string
	registry *registry
}

func (d dagShim) AddTask(fn any, spec ...TaskSpec) {
	d.registry.registerTask(d.dagId, fn, optionalSpec(spec, "AddTask"))
}

func (d dagShim) AddTaskWithName(taskId string, fn any, spec ...TaskSpec) {
	d.registry.registerTaskWithName(d.dagId, taskId, fn, optionalSpec(spec, "AddTaskWithName"))
}

// Bool returns a pointer to b. Use it for the *bool fields on TaskSpec /
// DagSpec where nil means "leave at schema default":
//
//	v1.TaskSpec{DoXComPush: v1.Bool(false)}
func Bool(b bool) *bool {
	return &b
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

// Function New creates a new bundle on which Dag and Tasks can be registered
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

func (r *registry) registerTask(dagId string, fn any, spec TaskSpec) {
	val := reflect.ValueOf(fn)

	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}

	fnName := getFnName(val)

	r.registerTaskWithName(dagId, fnName, fn, spec)
}

func (r *registry) registerTaskWithName(dagId, taskId string, fn any, spec TaskSpec) {
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
