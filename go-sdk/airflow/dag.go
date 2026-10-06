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
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// DagRef is a Dag authored in Go. [Dag] returns a new one.
type DagRef struct {
	dagID string
	// file is the source file that called Dag, as the compiler recorded it. It is empty when the
	// runtime cannot report a caller.
	file string
	// Dag and Task copy the specs they are given with copySpec, so a caller cannot change a
	// registered Dag through a spec it still holds.
	spec DagSpec

	mu         sync.Mutex
	registered bool
	// tasks holds every task of the Dag, inside a task group or not, in the order they were added.
	tasks     []*TaskRef
	tasksByID map[string]*TaskRef
	// groups holds every task group of the Dag, nested or not, in the order they were added.
	// Tasks and task groups share one namespace of IDs, so tasksByID and groupsByID hold no key
	// in common.
	groups     []*TaskGroupRef
	groupsByID map[string]*TaskGroupRef
	// edgeLabels holds every edge between two tasks of the Dag, whether Inputs, Before or After
	// declared it, and the label that Label put on it. An edge with no label maps to the empty
	// string, so a lookup reports whether the edge has been declared.
	edgeLabels map[edgeKey]string
	// groupEdges holds the edges that have a task group at one end or both, in the order they
	// were first declared, and groupEdgeLabels holds their labels as edgeLabels does. Registration
	// adds the edges between tasks that they stand for to edgeLabels.
	groupEdges      []groupEdge
	groupEdgeLabels map[edgeKey]string
}

// Dag returns an empty Dag with the given dag_id. An optional [DagSpec] holds the rest of the
// Dag's attributes. Add the tasks with [DagRef.Task], then pass the Dag to [BundleRef.Register]:
//
//	dag := airflow.Dag("etl")
//	dag.Task(extract)
//	dag.Task(load, airflow.TaskSpec{TaskID: "load_rows"})
//
//	bundle.Register(dag)
//
// Add every task before Register. [DagRef.Task], [DagRef.If], [DagRef.Switch], [DagRef.TaskGroup],
// [IfRef.Then], [IfRef.Else], [SwitchRef.Case] and the methods of [TaskGroupRef] panic once the Dag
// is registered.
//
// The bundle embeds the source file that calls Dag, so call it from the file that declares the Dag.
// A Dag built in a factory function belongs to the file of the factory.
//
// [BundleRef.Serve] sends the registered Dags to the Dag processor, but does not yet list them in
// the --airflow-metadata manifest or run their tasks.
//
// Dag panics if it gets more than one DagSpec, or if the DagSpec has a value that Python rejects
// when it builds or validates a Dag:
//   - Schedule is something other than an empty string, a preset or a cron expression of five to
//     seven fields
//   - Schedule is "@continuous" and MaxActiveRuns is not 1
//   - Catchup is true and StartDate is the zero Time, for a Dag that has a Schedule
//   - a tag in Tags is longer than 100 characters
//   - the year of StartDate or EndDate in UTC is not from 1 to 9999
func Dag(dagID string, spec ...DagSpec) *DagRef {
	if len(spec) > 1 {
		panic(fmt.Sprintf(
			"airflow.Dag: Dag %q got %d airflow.DagSpec values; "+
				"set all of the Dag's attributes in one DagSpec",
			dagID, len(spec),
		))
	}
	d := &DagRef{dagID: dagID}
	_, d.file, _, _ = runtime.Caller(1)
	if len(spec) == 1 {
		if err := checkDagSpec(spec[0]); err != nil {
			panic(fmt.Sprintf("airflow.Dag: Dag %q: %v", dagID, err))
		}
		d.spec = copySpec(spec[0])
	}
	return d
}

// tagMaxLength is the longest tag that Python's DAG accepts, counted in characters. Airflow stores
// a tag in a column of that length.
const tagMaxLength = 100

// TODO: run this validation only at build time (airflow-go-pack), not on every Dag call.
//
// checkDagSpec rejects a DagSpec with a value that Python rejects when it builds or validates a
// Dag. Depending on the value, Airflow would otherwise fail to load the serialized Dag, fail to
// store the Dag, or never schedule the Dag.
func checkDagSpec(spec DagSpec) error {
	if err := checkSchedule(spec.Schedule); err != nil {
		return err
	}
	// An unset MaxActiveRuns takes [core] max_active_runs_per_dag, which is 16 by default.
	if spec.Schedule == "@continuous" && spec.MaxActiveRuns != 1 {
		return errors.New(
			`airflow.DagSpec.Schedule is "@continuous", which allows one active Dag run at a ` +
				"time; set MaxActiveRuns to 1",
		)
	}
	if spec.Catchup != nil && *spec.Catchup && spec.Schedule != "" && spec.StartDate.IsZero() {
		return errors.New(
			"airflow.DagSpec.Catchup is true, which needs a StartDate to catch up from; " +
				"set StartDate",
		)
	}
	for _, tag := range spec.Tags {
		if n := utf8.RuneCountInString(tag); n > tagMaxLength {
			return fmt.Errorf(
				"airflow.DagSpec.Tags has %q, which has %d characters; a tag has at most %d",
				tag, n, tagMaxLength,
			)
		}
	}
	if err := checkTime("airflow.DagSpec.StartDate", spec.StartDate); err != nil {
		return err
	}
	return checkTime("airflow.DagSpec.EndDate", spec.EndDate)
}

func (*DagRef) registerable() {}

// TaskRef is a task that [DagRef.Task] or [TaskGroupRef.Task] added to a Dag. Pass it to [Inputs]
// to give its result to a task added later. Pass it to [IfRef.Then] or [IfRef.Else] to run it on
// one side of a condition. Pass it to [SwitchRef.Case] to make it a case of a switch. A TaskRef is
// a [Node], so [TaskRef.Before] and [TaskRef.After] order it against another task or a task group.
type TaskRef struct {
	dag *DagRef
	// group is the task group that the task was added through. It is nil for a task that
	// DagRef.Task, DagRef.If or DagRef.Switch added.
	group  *TaskGroupRef
	taskID string
	spec   TaskSpec
	// resultType is the type of the result that the task function returns with its error. It is
	// nil when the function returns only an error.
	resultType reflect.Type
	// inputs holds what Inputs passed, in the order of the parameters it fills. The task of each ref
	// is an upstream task of this one, and a literal adds no edge.
	inputs []taskInput
	// upstreams and downstreams hold the edges of the task, in the order they were declared and
	// without a repeat, so that an edge is recorded in both directions. Inputs, Before and After
	// all record an edge here.
	upstreams   []*TaskRef
	downstreams []*TaskRef
	task        bundle.Task
	// triggerDagRun is the checked copy of the TriggerDagRunSpec of a task from TriggerDagRun.
	// It is nil for a task that runs a Go function. A task from TriggerDagRun runs no Go
	// function, so its resultType, inputs and task are nil.
	triggerDagRun *TriggerDagRunSpec
	// decider is the IfRef or the SwitchRef that an If or a Switch method returned for the task. It
	// is nil for a task from DagRef.Task or TaskGroupRef.Task.
	decider decider
}

// Task adds a task that runs fn to the Dag and returns the new task.
//
// fn takes a [Context] first and returns either error or (result, error), like a function
// passed to [TaskHandler]. The parameters after the Context take the results of the tasks
// and the [Literal] values passed to [Inputs], in order. fn can also be the value that
// [TriggerDagRun] returns. The task then runs no Go code and takes no Inputs.
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
//   - the TaskSpec sets TriggerRule to a value that is not a TriggerRule constant
//   - the TaskSpec sets WeightRule to a value that is not a WeightRule constant
//   - the year of the StartDate or the EndDate of the TaskSpec in UTC is not from 1 to 9999
//   - the inputs passed to Inputs do not match the parameters of fn after the Context
//   - the task_id, with the group_ids that prefix it, is longer than 250 characters, or holds a
//     character other than a letter, a digit, an underscore, a dash or a dot, as Python's
//     validate_key requires
//   - the Dag already has a task with the same task_id, or a task group or the join node of one
//     takes it
//   - the Dag is already registered
func (d *DagRef) Task(fn any, opts ...TaskOption) *TaskRef {
	return d.addTask("airflow.DagRef.Task", nil, fn, opts, nil)
}

// decider is the IfRef that If returns or the SwitchRef that Switch returns. addTask uses it to
// add the task that decides which tasks after it to skip.
type decider interface {
	// wrap checks the result types of fn and wraps fn as a bundle.Task that skips the tasks that fn
	// does not choose.
	wrap(fn any) (bundle.Task, error)
	// bind records task as the task that runs the function of the decider.
	bind(task *TaskRef)
}

// addTask adds a task for Task, If and Switch, of the Dag or of a task group. method names the
// caller in panic messages. group is the task group that the task is added through, and nil for a
// task of the Dag itself. decider is the IfRef or the SwitchRef of the task, and nil when Task
// calls addTask.
func (d *DagRef) addTask(
	method string, group *TaskGroupRef, fn any, opts []TaskOption, decider decider,
) *TaskRef {
	trigger, isTrigger := fn.(TriggerDagRunTask)
	var triggerSpec *TriggerDagRunSpec
	var triggerErr error
	if isTrigger {
		// Copying the spec marshals Conf and so runs the MarshalJSON methods of the caller's
		// values. addTask copies before it locks d.mu. Otherwise such a method would deadlock if it
		// added a task to this Dag, and a slow one would hold up Register.
		copied, err := copyTriggerDagRunSpec(trigger.spec)
		triggerSpec, triggerErr = &copied, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.registered {
		panic(fmt.Sprintf(
			"%s: Dag %q has already been registered; add every task before Register",
			method, d.dagID,
		))
	}
	d.checkGroupLocked(method, group)
	var wrapped bundle.Task
	if !isTrigger {
		wrap := bundle.NewPositionalTaskFunction
		if decider != nil {
			wrap = decider.wrap
		}
		var err error
		if wrapped, err = newTaskFunction(fn, wrap); err != nil {
			panic(fmt.Sprintf("%s: Dag %q: %v", method, d.dagID, err))
		}
	}
	var cfg taskConfig
	for i, opt := range opts {
		switch opt := opt.(type) {
		case nil:
			panic(fmt.Sprintf("%s: Dag %q: opts[%d] is nil", method, d.dagID, i))
		case *TaskSpec:
			if opt == nil {
				panic(fmt.Sprintf(
					"%s: Dag %q: opts[%d] is a nil *airflow.TaskSpec", method, d.dagID, i,
				))
			}
		case TaskSpec, inputs:
		default:
			// Only a struct that embeds a TaskSpec or a TaskOption gets here.
			panic(fmt.Sprintf(
				"%s: Dag %q: opts[%d] has type %T, "+
					"which is not an option that package airflow defines",
				method, d.dagID, i, opt,
			))
		}
		if err := opt.applyTask(&cfg); err != nil {
			// A task from TriggerDagRun takes no Inputs at all, and the check after this loop
			// reports that. err says to merge the Inputs into one, which would not fix a task from
			// TriggerDagRun.
			if _, ok := opt.(inputs); ok && isTrigger {
				continue
			}
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q: %v", method, findTaskName(group, fn, opts), d.dagID, err,
			))
		}
	}

	taskID := cfg.spec.TaskID
	if taskID == "" && isTrigger {
		panic(fmt.Sprintf(
			"%s: Dag %q: a task from airflow.TriggerDagRun with DagID %q has no "+
				"Go function to take a task_id from; set one with airflow.TaskSpec{TaskID: ...}",
			method, d.dagID, trigger.spec.DagID,
		))
	}
	if taskID == "" {
		var ok bool
		if taskID, ok = taskIDFromFuncName(funcName(fn)); !ok {
			panic(fmt.Sprintf(
				"%s: Dag %q: %s has no name to use as the task_id; "+
					"set one with airflow.TaskSpec{TaskID: ...}",
				method, d.dagID, funcName(fn),
			))
		}
	}
	unprefixed := taskID
	taskID = group.childID(taskID)
	if err := checkTaskID(taskID, taskID != unprefixed); err != nil {
		panic(fmt.Sprintf("%s: Dag %q: %v", method, d.dagID, err))
	}
	if triggerErr != nil {
		panic(fmt.Sprintf("%s: task %q of Dag %q: %v", method, taskID, d.dagID, triggerErr))
	}
	if err := checkTaskSpec(cfg.spec); err != nil {
		panic(fmt.Sprintf("%s: task %q of Dag %q: %v", method, taskID, d.dagID, err))
	}
	if _, exists := d.tasksByID[taskID]; exists {
		panic(fmt.Sprintf(
			"%s: Dag %q already has a task %q; "+
				"set another task_id with airflow.TaskSpec{TaskID: ...}",
			method, d.dagID, taskID,
		))
	}
	if taken := d.describeIDLocked(taskID); taken != "" {
		panic(fmt.Sprintf(
			"%s: Dag %q cannot add task %q, because %s already takes the ID; "+
				"set another task_id with airflow.TaskSpec{TaskID: ...}",
			method, d.dagID, taskID, taken,
		))
	}
	var taskInputs []taskInput
	var resultType reflect.Type
	if isTrigger {
		if cfg.hasInputs {
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q comes from airflow.TriggerDagRun and "+
					"takes no airflow.Inputs, because it has no Go function to pass the results to",
				method, taskID, d.dagID,
			))
		}
	} else {
		fnType := reflect.TypeOf(fn)
		taskInputs = d.checkInputs(method, taskID, fnType, cfg.inputs)
		// newTaskFunction has checked that fn returns either error or (result, error).
		if fnType.NumOut() == 2 {
			resultType = fnType.Out(0)
		}
	}

	task := &TaskRef{
		dag:           d,
		group:         group,
		taskID:        taskID,
		spec:          copySpec(cfg.spec),
		resultType:    resultType,
		inputs:        taskInputs,
		task:          wrapped,
		triggerDagRun: triggerSpec,
		decider:       decider,
	}
	if decider != nil {
		decider.bind(task)
	}
	if d.tasksByID == nil {
		d.tasksByID = make(map[string]*TaskRef)
	}
	d.tasksByID[taskID] = task
	d.tasks = append(d.tasks, task)
	if group != nil {
		group.children = append(group.children, task)
	}
	// Inputs passes a task once per parameter it fills, so the same task can arrive twice. The
	// edge is one either way, and the task is new, so no edge to it carries a label to settle.
	for _, input := range taskInputs {
		if !input.literal {
			d.addEdgeLocked(input.ref, task, "")
		}
	}
	return task
}

// markRegistered marks d as registered, which stops any further change to d. It panics instead
// when a condition from If has no task from Then, when a switch from Switch has no case, or when
// the edges of d close a cycle. The Dag is whole by then, so markRegistered can expand the group
// edges in the order they were first declared, and a walk of the whole graph answers for every
// edge. It walks the graph before the expansion too, so that a cycle between the edges the author
// declared is reported as declared. A Dag that fails a check stays unregistered and holds only the
// edges its author declared. The author can still complete the Dag with IfRef.Then or
// SwitchRef.Case, but cannot undo a cycle, since a Dag only ever gains edges.
func (d *DagRef) markRegistered() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// A Dag that another bundle registered has been checked and expanded already.
	if d.registered {
		return
	}
	for _, task := range d.tasks {
		switch decider := task.decider.(type) {
		case *IfRef:
			if decider.thenTask == nil {
				panic(fmt.Sprintf(
					"airflow.BundleRef.Register: condition %q of Dag %q has no task from Then; "+
						"name the task that runs when the condition is true with IfRef.Then",
					task.taskID, d.dagID,
				))
			}
		case *SwitchRef:
			if len(decider.cases) == 0 {
				panic(fmt.Sprintf(
					"airflow.BundleRef.Register: switch %q of Dag %q has no case; "+
						"name each task that the switch can choose with SwitchRef.Case",
					task.taskID, d.dagID,
				))
			}
		}
	}
	if cycle := d.cycleLocked(); cycle != nil {
		panic(cycleMessage(d.dagID, cycle, nil))
	}
	expanded := d.expandGroupEdgesLocked()
	if cycle := d.cycleLocked(); cycle != nil {
		through := groupEdgesOn(cycle, expanded)
		d.removeEdgesLocked(expanded)
		panic(cycleMessage(d.dagID, cycle, through))
	}
	d.registered = true
}

// cycleMessage reports a cycle in the task dependencies of Dag dagID. through names the group
// edges that edges on the cycle stand for, so that a cycle that only group edges close points at
// them.
func cycleMessage(dagID string, cycle, through []string) string {
	message := fmt.Sprintf(
		"airflow.BundleRef.Register: the task dependencies of Dag %q contain a cycle: %s",
		dagID, strings.Join(cycle, " -> "),
	)
	switch len(through) {
	case 0:
		return message
	case 1:
		return message + ", through the group edge " + through[0]
	default:
		return message + ", through the group edges " + strings.Join(through, ", ")
	}
}

func funcName(fn any) string { return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name() }

// findTaskName names a task in an error that addTask raises before it settles the task_id. group
// is the task group that the task is added through, and prefixes the task_id that findTaskName
// finds.
func findTaskName(group *TaskGroupRef, fn any, opts []TaskOption) string {
	for _, opt := range opts {
		var spec TaskSpec
		switch opt := opt.(type) {
		case TaskSpec:
			spec = opt
		case *TaskSpec:
			if opt != nil {
				spec = *opt
			}
		}
		if spec.TaskID != "" {
			return group.childID(spec.TaskID)
		}
	}
	if _, ok := fn.(TriggerDagRunTask); ok {
		return "airflow.TriggerDagRun"
	}
	if taskID, ok := taskIDFromFuncName(funcName(fn)); ok {
		return group.childID(taskID)
	}
	return funcName(fn)
}

// taskIDMaxLength is the longest task_id that Python's validate_key accepts, counted in
// characters.
const taskIDMaxLength = 250

// checkTaskID checks a task_id as Python's validate_key does when an operator is constructed,
// which is after the group_ids of its task groups prefix it. Python matches the ID against
// ^[\w.-]+$, whose $ also lets a trailing newline through, and checkTaskID does not. prefixed
// reports whether a group_id prefixes the task_id, so that the error can say what to shorten.
func checkTaskID(taskID string, prefixed bool) error {
	if !utf8.ValidString(taskID) {
		return fmt.Errorf(
			"task_id %q is not valid UTF-8; set another one with airflow.TaskSpec{TaskID: ...}",
			taskID,
		)
	}
	if n := utf8.RuneCountInString(taskID); n > taskIDMaxLength {
		if prefixed {
			return fmt.Errorf(
				"task_id %q has %d characters, counting the group_ids that prefix it, and a "+
					"task_id has at most %d; shorten a group_id, or set a shorter task_id with "+
					"airflow.TaskSpec{TaskID: ...}",
				taskID, n, taskIDMaxLength,
			)
		}
		return fmt.Errorf(
			"task_id %q has %d characters, and a task_id has at most %d; "+
				"set a shorter one with airflow.TaskSpec{TaskID: ...}",
			taskID, n, taskIDMaxLength,
		)
	}
	for _, r := range taskID {
		if !isWordRune(r) && r != '-' && r != '.' {
			return fmt.Errorf(
				"task_id %q holds %q, and a task_id holds only letters, digits, underscores, "+
					"dashes and dots; set another one with airflow.TaskSpec{TaskID: ...}",
				taskID, r,
			)
		}
	}
	return nil
}

// isWordRune reports whether r matches \w in a Python regular expression: a character for which
// str.isalnum is true, or the underscore.
func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' }

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
