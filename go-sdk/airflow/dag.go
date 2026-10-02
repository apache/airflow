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
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// DagRef is a Dag authored in Go. [Dag] returns a new one.
type DagRef struct {
	dagID string
	// Dag and Task copy the specs they are given with copySpec, so a caller cannot change a
	// registered Dag through a spec it still holds.
	spec DagSpec

	mu         sync.Mutex
	registered bool
	tasks      []*TaskRef
	tasksByID  map[string]*TaskRef
}

// Dag returns an empty Dag with the given dag_id. An optional [DagSpec] holds the rest of the
// Dag's attributes, and Dag panics if it gets more than one DagSpec. Add the tasks with
// [DagRef.Task], then pass the Dag to [BundleRef.Register]:
//
//	dag := airflow.Dag("etl")
//	dag.Task(extract)
//	dag.Task(load, airflow.TaskSpec{TaskID: "load_rows"})
//
//	bundle.Register(dag)
//
// Add every task before Register. [DagRef.Task] panics once the Dag is registered.
//
// [BundleRef.Serve] does not yet serve the Dags that Dag returns. It leaves them out of the
// --airflow-metadata manifest and cannot run their tasks.
func Dag(dagID string, spec ...DagSpec) *DagRef {
	if len(spec) > 1 {
		panic(fmt.Sprintf(
			"airflow.Dag: Dag %q got %d airflow.DagSpec values; "+
				"set all of the Dag's attributes in one DagSpec",
			dagID, len(spec),
		))
	}
	d := &DagRef{dagID: dagID}
	if len(spec) == 1 {
		d.spec = copySpec(spec[0])
	}
	return d
}

func (*DagRef) registerable() {}

// TaskRef is a task that [DagRef.Task] added to a Dag. Pass it to [Inputs] to give its result
// to a task that DagRef.Task adds later.
type TaskRef struct {
	dag    *DagRef
	taskID string
	spec   TaskSpec
	// resultType is the type of the result that the task function returns with its error. It is
	// nil when the function returns only an error.
	resultType reflect.Type
	// inputs holds the tasks that Inputs passed, in the order of the parameters they fill. Each
	// of them is an upstream task of this one.
	inputs []*TaskRef
	task   bundle.Task
	// triggerDagRun is the checked copy of the TriggerDagRunSpec of a task from TriggerDagRun.
	// It is nil for a task that runs a Go function. A task from TriggerDagRun runs no Go
	// function, so its resultType, inputs and task are nil.
	triggerDagRun *TriggerDagRunSpec
}

// Task adds a task that runs fn to the Dag and returns the new task.
//
// fn takes a [Context] first and returns either error or (result, error), like a function
// passed to [TaskHandler]. The parameters after the Context take the results of the tasks
// passed to [Inputs], in order. fn can also be the value that [TriggerDagRun] returns. The task
// then runs no Go code and takes no Inputs.
//
// The task_id is the name of fn, spelled exactly as it is in Go. dag.Task(extractRows) adds the
// task extractRows, and dag.Task(svc.Extract), which passes a method value, adds the task
// Extract. A task needs a TaskID when Task cannot read a name from fn. That is the case for a
// function literal, for a function that package reflect made, such as one from
// reflect.MakeFunc, and for a method value that a generic function takes on a value whose type
// uses a type parameter.
//
// A [TaskSpec] sets the task_id and the other attributes of a task. Pass at most one, so that
// each attribute is set in one place:
//
//	dag.Task(extractRows, airflow.TaskSpec{TaskID: "extract_rows"})
//
// Renaming fn renames a task that takes its task_id from fn. Airflow identifies a task by its
// task_id in the run history, in clears and in the UI, so to Airflow the renamed function runs
// a new task. Set TaskID when the task_id has to stay the same through such a rename.
//
// Task panics if:
//   - fn is not a valid task function
//   - fn has no name that Task can read and no TaskSpec sets a TaskID
//   - fn comes from TriggerDagRun and no TaskSpec sets a TaskID
//   - fn comes from TriggerDagRun and its TriggerDagRunSpec is not valid
//   - fn comes from TriggerDagRun and opts holds an Inputs
//   - an option is nil or is not one that package airflow defines
//   - opts holds more than one TaskSpec or more than one Inputs
//   - the tasks passed to Inputs do not match the parameters of fn after the Context
//   - the Dag already has a task with the same task_id
//   - the Dag is already registered
func (d *DagRef) Task(fn any, opts ...TaskOption) *TaskRef {
	trigger, isTrigger := fn.(TriggerDagRunTask)
	var triggerSpec *TriggerDagRunSpec
	var triggerErr error
	if isTrigger {
		// Copying the spec marshals Conf and so runs the MarshalJSON methods of the caller's
		// values. Task copies before it locks d.mu. Otherwise such a method would deadlock if it
		// added a task to this Dag, and a slow one would hold up Register.
		copied, err := copyTriggerDagRunSpec(trigger.spec)
		triggerSpec, triggerErr = &copied, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.registered {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: Dag %q has already been registered; "+
				"add every task before Register",
			d.dagID,
		))
	}
	var wrapped bundle.Task
	if !isTrigger {
		var err error
		if wrapped, err = newTaskFunction(fn, bundle.NewPositionalTaskFunction); err != nil {
			panic(fmt.Sprintf("airflow.DagRef.Task: Dag %q: %v", d.dagID, err))
		}
	}
	var cfg taskConfig
	for i, opt := range opts {
		switch opt := opt.(type) {
		case nil:
			panic(fmt.Sprintf("airflow.DagRef.Task: Dag %q: opts[%d] is nil", d.dagID, i))
		case *TaskSpec:
			if opt == nil {
				panic(fmt.Sprintf(
					"airflow.DagRef.Task: Dag %q: opts[%d] is a nil *airflow.TaskSpec", d.dagID, i,
				))
			}
		case TaskSpec, inputs:
		default:
			// Only a struct that embeds a TaskSpec or a TaskOption gets here.
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: Dag %q: opts[%d] has type %T, "+
					"which is not an option that package airflow defines",
				d.dagID, i, opt,
			))
		}
		opt.applyTask(&cfg)
	}
	if len(cfg.specs) > 1 {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: task %q of Dag %q got %d airflow.TaskSpec values; "+
				"set all of the task's attributes in one TaskSpec",
			findTaskName(fn, cfg.specs), d.dagID, len(cfg.specs),
		))
	}

	var spec TaskSpec
	if len(cfg.specs) == 1 {
		spec = cfg.specs[0]
	}
	taskID := spec.TaskID
	if taskID == "" && isTrigger {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: Dag %q: a task from airflow.TriggerDagRun with DagID %q has no "+
				"Go function to take a task_id from; set one with airflow.TaskSpec{TaskID: ...}",
			d.dagID, trigger.spec.DagID,
		))
	}
	if taskID == "" {
		var ok bool
		if taskID, ok = taskIDFromFuncName(funcName(fn)); !ok {
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: Dag %q: %s has no name to use as the task_id; "+
					"set one with airflow.TaskSpec{TaskID: ...}",
				d.dagID, funcName(fn),
			))
		}
	}
	if triggerErr != nil {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: task %q of Dag %q: %v", taskID, d.dagID, triggerErr,
		))
	}
	if _, exists := d.tasksByID[taskID]; exists {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: Dag %q already has a task %q; "+
				"set another task_id with airflow.TaskSpec{TaskID: ...}",
			d.dagID, taskID,
		))
	}
	var upstreams []*TaskRef
	var resultType reflect.Type
	if isTrigger {
		if len(cfg.inputs) > 0 {
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q comes from airflow.TriggerDagRun and "+
					"takes no airflow.Inputs, because it has no Go function to pass the results to",
				taskID, d.dagID,
			))
		}
	} else {
		fnType := reflect.TypeOf(fn)
		upstreams = d.checkInputs(taskID, fnType, cfg.inputs)
		// newTaskFunction has checked that fn returns either error or (result, error).
		if fnType.NumOut() == 2 {
			resultType = fnType.Out(0)
		}
	}

	task := &TaskRef{
		dag:           d,
		taskID:        taskID,
		spec:          copySpec(spec),
		resultType:    resultType,
		inputs:        upstreams,
		task:          wrapped,
		triggerDagRun: triggerSpec,
	}
	if d.tasksByID == nil {
		d.tasksByID = make(map[string]*TaskRef)
	}
	d.tasksByID[taskID] = task
	d.tasks = append(d.tasks, task)
	return task
}

func (d *DagRef) markRegistered() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.registered = true
}

func funcName(fn any) string { return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name() }

// findTaskName names a task in an error that Task raises before it settles the task_id.
func findTaskName(fn any, specs []TaskSpec) string {
	for _, spec := range specs {
		if spec.TaskID != "" {
			return spec.TaskID
		}
	}
	if _, ok := fn.(TriggerDagRunTask); ok {
		return "airflow.TriggerDagRun"
	}
	if taskID, ok := taskIDFromFuncName(funcName(fn)); ok {
		return taskID
	}
	return funcName(fn)
}

// taskIDFromFuncName takes the runtime name of a function and returns the name that the
// function is declared with. It reports false when the runtime name does not carry one.
func taskIDFromFuncName(name string) (string, bool) {
	// Package reflect runs every function it makes through a stub of its own, such as
	// reflect.makeFuncStub, so the name is the stub's.
	if strings.HasPrefix(name, "reflect.") {
		return "", false
	}
	// A method value such as svc.Extract is named after the receiver type of the method, as in
	// (*Service).Extract-fm, and the last segment is the method name.
	if method, ok := strings.CutSuffix(name, "-fm"); ok {
		return method[strings.LastIndex(method, ".")+1:], true
	}
	// The runtime prefixes the name with the import path, and it escapes a dot in the last
	// element of that path as %2e. So the first dot after the last slash ends the package name.
	name = name[strings.LastIndex(name, "/")+1:]
	name = name[strings.Index(name, ".")+1:]
	// The runtime writes the type arguments of a generic function as [...].
	name = strings.ReplaceAll(name, "[...]", "")
	// Any dot left comes from a function literal, which the runtime names after the function it
	// is declared in, as in main.func1. The compiler builds a function literal for a method value
	// that a generic function takes on a value whose type uses a type parameter, so that case
	// ends up here too. A method expression such as Service.Extract leaves a dot as well, but
	// Task rejects most method expressions earlier, because the receiver is their first
	// parameter.
	if strings.Contains(name, ".") {
		return "", false
	}
	return name, true
}
