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
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/apache/airflow/go-sdk/pkg/worker"
)

type (
	// Task is one registered task: something the runtime can Execute. Bundle
	// authors do not implement this directly; Dag.Task wraps a plain Go
	// function into a Task for you.
	Task = worker.Task

	// Bundle is the execution-time view of a registry: it looks up a task by
	// dag_id and task_id. Registry embeds it so the object built during
	// RegisterDags can also serve tasks when they run.
	Bundle = worker.Bundle

	// Dag is the handle returned by Registry.AddDag. Use it to attach the Go
	// functions that implement the dag's tasks.
	Dag interface {
		// Task registers fn as a task and returns its TaskRef, the handle
		// downstream registrations pass to Inputs or After. The task id is
		// TaskSpec.TaskId when a spec sets it, otherwise fn's own Go name.
		//
		// fn is an ordinary Go function whose parameters are filled by kind:
		//
		//   - Injected runtime values, matched by type in any order:
		//     context.Context (or sdk.TIRunContext), *slog.Logger, and
		//     sdk.Client (or a narrower sdk.VariableClient /
		//     sdk.ConnectionClient / sdk.XComClient).
		//   - Data parameters: every other parameter, bound positionally to
		//     the Inputs refs. At run time each receives the return value of
		//     its upstream task, pulled from that task's return-value XCom
		//     and decoded into the parameter's type.
		//
		// fn must return either error or (result, error): a non-nil error
		// fails the task, and a non-nil first result is pushed as the task's
		// return-value XCom (feeding any downstream Inputs). Passing a
		// non-function, a function whose signature does not match, a spec /
		// Inputs mismatch, or a ref from another dag panics at registration
		// (i.e. dag-parse) time.
		Task(fn any, opts ...TaskOption) *TaskRef
	}

	// Registry is the recorder passed to BundleProvider.RegisterDags. Use it to
	// declare the dags this bundle can run; it also extends Bundle so the same
	// object serves task lookups at execution time.
	Registry interface {
		Bundle
		// AddDag registers a dag described by spec (spec.DagId is required)
		// and returns a Dag handle for attaching tasks. A missing DagId or a
		// duplicate registration panics.
		AddDag(spec DagSpec) Dag
	}

	// EnumerableBundle exposes the dag/task identity recorded by
	// RegisterDags. The default registry implements it; the coordinator-mode
	// runtime relies on it for the DAG-parse one-shot.
	EnumerableBundle interface {
		OrderedDags() []DagInfo
	}

	// TaskOption configures one Dag.Task registration. A TaskSpec value,
	// Inputs, and After all implement it.
	TaskOption interface {
		applyTask(*taskConfig)
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

// TaskRef is the handle Dag.Task returns for a registered task. Pass it to
// Inputs (data dependency: the upstream's return value feeds a data
// parameter) or After (ordering-only dependency) when registering downstream
// tasks in the same dag.
type TaskRef struct {
	id    string
	out   reflect.Type // fn's first return type; nil when fn returns only error
	reg   *registry
	dagID string
}

// ID returns the registered task id.
func (r *TaskRef) ID() string { return r.id }

// taskConfig accumulates the options of one Dag.Task call.
type taskConfig struct {
	spec    TaskSpec
	specSet bool
	inputs  []*TaskRef
	after   []*TaskRef
}

type inputsOption []*TaskRef

func (o inputsOption) applyTask(c *taskConfig) { c.inputs = append(c.inputs, o...) }

// Inputs declares data dependencies: each ref's task must run first, and its
// return value is pulled from XCom and passed to the matching data parameter
// of the task function, in order. The number of refs must equal the number of
// data parameters, and each upstream's return type must fit the parameter it
// feeds (checked at registration).
func Inputs(refs ...*TaskRef) TaskOption { return inputsOption(refs) }

type afterOption []*TaskRef

func (o afterOption) applyTask(c *taskConfig) { c.after = append(c.after, o...) }

// After declares ordering-only dependencies: each ref's task must complete
// before this one runs, without feeding it any value.
func After(refs ...*TaskRef) TaskOption { return afterOption(refs) }

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

type dagShim struct {
	dagId    string
	registry *registry
}

func (d dagShim) Task(fn any, opts ...TaskOption) *TaskRef {
	return d.registry.registerTask(d.dagId, fn, opts)
}

// Bool returns a pointer to b. Use it for the *bool fields on TaskSpec /
// DagSpec where nil means "leave at schema default":
//
//	v1.TaskSpec{DoXComPush: v1.Bool(false)}
func Bool(b bool) *bool {
	return &b
}

// anonFnName matches the names the Go runtime gives anonymous functions once
// splitFullName has taken the segment after the last dot: "funcN" for a
// top-level closure, or a bare number for a nested one ("pkg.fn.func1.2").
var anonFnName = regexp.MustCompile(`^(func)?\d+$`)

func splitFullName(fullName string) (typeName, pkgPath string) {
	// fullName looks like "main.extract" or "github.com/x/y.MyTask"; method
	// values get a "-fm" suffix.
	lastDot := strings.LastIndex(fullName, ".")
	if lastDot < 0 {
		return strings.TrimSuffix(fullName, "-fm"), ""
	}
	return strings.TrimSuffix(fullName[lastDot+1:], "-fm"), fullName[:lastDot]
}

func (r *registry) AddDag(spec DagSpec) Dag {
	if spec.DagId == "" {
		panic(fmt.Errorf("AddDag requires DagSpec.DagId to be set"))
	}
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	if _, exists := r.taskFuncMap[spec.DagId]; exists {
		panic(fmt.Errorf("Dag %q already exists in bundle", spec.DagId))
	}
	r.taskFuncMap[spec.DagId] = make(map[string]Task)
	r.taskInfo[spec.DagId] = make(map[string]TaskInfo)
	r.dagSpec[spec.DagId] = spec
	r.dagOrder = append(r.dagOrder, spec.DagId)
	return dagShim{spec.DagId, r}
}

func (r *registry) registerTask(dagId string, fn any, opts []TaskOption) *TaskRef {
	var cfg taskConfig
	for _, o := range opts {
		o.applyTask(&cfg)
	}

	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}
	fullName := runtime.FuncForPC(val.Pointer()).Name()
	typeName, pkgPath := splitFullName(fullName)

	taskId := cfg.spec.TaskId
	if taskId == "" {
		if anonFnName.MatchString(typeName) {
			panic(fmt.Errorf(
				"task function in DAG %q is anonymous (its derived name would be %q); set TaskSpec.TaskId explicitly",
				dagId,
				typeName,
			))
		}
		taskId = typeName
	}

	// Pair the fn's data parameters with the Inputs refs before wrapping, so
	// every mismatch is a registration (dag-parse) error, not a runtime one.
	dataParams := dataParamTypes(val.Type())
	if len(cfg.inputs) != len(dataParams) {
		panic(fmt.Errorf(
			"task %q in DAG %q declares %d data parameter(s) but Inputs supplies %d",
			taskId, dagId, len(dataParams), len(cfg.inputs),
		))
	}
	bindings := make([]InputBinding, len(cfg.inputs))
	for i, ref := range cfg.inputs {
		r.validateRef(ref, dagId, taskId, "Inputs")
		if ref.out == nil {
			panic(fmt.Errorf(
				"task %q in DAG %q: input task %q returns no value; use After for an ordering-only dependency",
				taskId,
				dagId,
				ref.id,
			))
		}
		if !inputCompatible(ref.out, dataParams[i]) {
			panic(fmt.Errorf(
				"task %q in DAG %q: input %d: task %q returns %s, which cannot fill parameter type %s",
				taskId,
				dagId,
				i,
				ref.id,
				ref.out,
				dataParams[i],
			))
		}
		bindings[i] = InputBinding{TaskID: ref.id}
	}
	for _, ref := range cfg.after {
		r.validateRef(ref, dagId, taskId, "After")
	}

	task, err := NewTaskFunction(fn, bindings...)
	if err != nil {
		panic(fmt.Errorf("error registering task %q for DAG %q: %w", taskId, dagId, err))
	}

	var outType reflect.Type
	if fnType := val.Type(); fnType.NumOut() > 1 {
		outType = fnType.Out(0)
	}

	var inputIDs []string
	if len(cfg.inputs) > 0 {
		inputIDs = make([]string, len(cfg.inputs))
		for i, ref := range cfg.inputs {
			inputIDs[i] = ref.id
		}
	}

	info := TaskInfo{
		ID:       taskId,
		TypeName: typeName,
		PkgPath:  pkgPath,
		Spec:     cfg.spec,
		Inputs:   inputIDs,
	}

	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()

	dagTasks, exists := r.taskFuncMap[dagId]
	if !exists {
		panic(fmt.Errorf("DAG %q is not registered", dagId))
	}
	if _, exists := dagTasks[taskId]; exists {
		panic(fmt.Errorf("taskId %q is already registered for DAG %q", taskId, dagId))
	}

	// Record one downstream edge on each distinct upstream, whether the
	// dependency carries data (Inputs) or only ordering (After).
	seen := make(map[string]bool, len(cfg.inputs)+len(cfg.after))
	for _, ref := range append(append([]*TaskRef{}, cfg.inputs...), cfg.after...) {
		if seen[ref.id] {
			continue
		}
		seen[ref.id] = true
		parent := r.taskInfo[dagId][ref.id]
		parent.Downstream = append(parent.Downstream, taskId)
		r.taskInfo[dagId][ref.id] = parent
	}

	dagTasks[taskId] = task
	r.taskInfo[dagId][taskId] = info
	r.taskOrder[dagId] = append(r.taskOrder[dagId], taskId)

	return &TaskRef{id: taskId, out: outType, reg: r, dagID: dagId}
}

// validateRef panics unless ref was returned by this registry for the same
// dag. A nil ref, a ref from another registry, or a ref from a different dag
// is a registration error.
func (r *registry) validateRef(ref *TaskRef, dagId, taskId, opt string) {
	switch {
	case ref == nil:
		panic(fmt.Errorf("task %q in DAG %q: %s got a nil TaskRef", taskId, dagId, opt))
	case ref.reg != r:
		panic(fmt.Errorf(
			"task %q in DAG %q: %s ref %q belongs to a different registry",
			taskId, dagId, opt, ref.id,
		))
	case ref.dagID != dagId:
		panic(fmt.Errorf(
			"task %q in DAG %q: %s ref %q belongs to DAG %q; dependencies cannot cross dags",
			taskId, dagId, opt, ref.id, ref.dagID,
		))
	}
}

// inputCompatible reports whether an upstream return type can fill a data
// parameter. Identical or assignable types always fit; a map or empty
// interface parameter opts into loose decoding of any shape; an upstream that
// returns any is accepted and checked structurally when the value is decoded
// at run time. Pointers are stripped from both sides: the value crosses XCom
// as its JSON shape, so *T and T are interchangeable in either position.
func inputCompatible(out, param reflect.Type) bool {
	if out.Kind() == reflect.Pointer {
		out = out.Elem()
	}
	if param.Kind() == reflect.Pointer {
		param = param.Elem()
	}
	switch {
	case out.AssignableTo(param):
		return true
	case param.Kind() == reflect.Interface && param.NumMethod() == 0:
		return true
	case param.Kind() == reflect.Map:
		return true
	case out.Kind() == reflect.Interface && out.NumMethod() == 0:
		return true
	}
	return false
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
// each with its tasks in the order Dag.Task was called. The returned slice is
// freshly allocated; callers may mutate it freely.
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
