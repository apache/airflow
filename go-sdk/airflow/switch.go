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
	"slices"
	"strconv"
	"strings"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// SwitchRef is a switch that [DagRef.Switch] or [TaskGroupRef.Switch] added to a Dag. Each call to
// [SwitchRef.Case] names a task that the switch can choose.
type SwitchRef struct {
	// task is the task that runs the decider function. The cases run after task, but none of them
	// takes the result of task.
	task *TaskRef
	// cases holds the tasks that Case named, in the order Case named them.
	cases []*TaskRef
}

// Switch adds a task that runs the decider function fn, and returns the switch. Use
// [SwitchRef.Case] to name each task that fn can choose. fn returns (*airflow.TaskRef, error), and
// the TaskRef that it returns is the case that runs. So fn needs the TaskRefs of the cases, for
// example through the receiver of a method value:
//
//	type paths struct{ long, short *airflow.TaskRef }
//
//	func (p *paths) pickPath(actx airflow.Context, rows []string) (*airflow.TaskRef, error) {
//		if len(rows) > 1000 {
//			return p.long, nil
//		}
//		return p.short, nil
//	}
//
// Where the Dag is built:
//
//	extracted := dag.Task(extract)
//	p := &paths{long: dag.Task(handleLong), short: dag.Task(handleShort)}
//
//	dag.Switch(p.pickPath, airflow.Inputs(extracted)).Case(p.long).Case(p.short)
//
// The task that Switch adds then gets the task_id pickPath, the name of the method. fn can also be
// a function literal that closes over the TaskRefs. A function literal has no name to take the
// task_id from, so pass Switch a [TaskSpec] that sets TaskID. A function that reads the TaskRefs
// from package-level variables works only when the code that builds the Dag runs once: a second
// Dag built by the same code would overwrite the variables.
//
// In every other way fn follows the rules of a function passed to [DagRef.Task]: it takes a
// [Context] first, and [Inputs] fills the parameters after the Context. The task that Switch adds
// gets its task_id in the same way as a task from DagRef.Task, and a TaskSpec sets its attributes.
//
// When the task runs, the result of fn decides what it skips:
//   - a case skips every other case
//   - a TaskRef that is not a case, or a nil one, fails the task with an error that names the
//     TaskRef and the cases
//   - an error fails the task
//
// When the task fails, it pushes no XCom and skips nothing. Otherwise its return_value XCom holds
// the task_id of the case that fn chose.
//
// fn chooses exactly one case. A Python branch can choose several tasks by returning a list of
// task_ids, but a switch cannot. To run several tasks together, order them after one task and make
// that task a case, or give each of them a condition of its own with [DagRef.If].
//
// A switch has no default case, because a Python branch operator has none. To run a task when no
// other case fits, make it a case and have fn return it.
//
// A case runs after the switch, so naming it records an edge from the switch to it, as
// [TaskRef.Before] would. When fn chooses a case, the switch skips only the cases that fn did not
// choose. Whether a task after the cases runs is then up to its trigger rule. With the default
// trigger rule, a task is skipped when one of its upstream tasks is skipped. So a task after both
// cases of the example above would never run. Give that task a trigger rule like
// [TriggerRuleNoneFailedMinOneSuccess], which runs a task when at least one of its upstream tasks
// succeeds and none fails:
//
//	report := dag.Task(writeReport, airflow.TaskSpec{
//		TriggerRule: airflow.TriggerRuleNoneFailedMinOneSuccess,
//	})
//	report.After(p.long, p.short)
//
// Do not make report a case as well. The switch skips every case that fn does not choose, so when
// fn chooses p.short, the switch would skip report too. A Python branch works differently here: it
// does not skip a task that runs after the task it chooses. So the Python pattern
// branch >> [optional, join] with optional >> join needs a change in Go. Make join a task like
// report, and give the path without optional a case of its own, such as a task that does nothing.
//
// [BundleRef.Register] panics if a switch has no case.
//
// Switch panics for the same reasons as DagRef.Task does for a Go function. It also panics if fn
// comes from [TriggerDagRun] or does not return (*airflow.TaskRef, error).
func (d *DagRef) Switch(fn any, opts ...TaskOption) *SwitchRef {
	return d.addSwitch("airflow.DagRef.Switch", nil, fn, opts)
}

// addSwitch adds a switch for DagRef.Switch and TaskGroupRef.Switch. group is the task group that
// the switch is added through, and nil for DagRef.Switch.
func (d *DagRef) addSwitch(
	method string, group *TaskGroupRef, fn any, opts []TaskOption,
) *SwitchRef {
	if _, ok := fn.(TriggerDagRunTask); ok {
		panic(fmt.Sprintf(
			"%s: Dag %q: fn comes from airflow.TriggerDagRun, "+
				"but a decider function is a Go function that returns (*airflow.TaskRef, error)",
			method, d.dagID,
		))
	}
	switchRef := &SwitchRef{}
	d.addTask(method, group, fn, opts, switchRef)
	return switchRef
}

// Case names task as a task that the switch s can choose. When the decider function of s chooses
// another case, the task is skipped. Case returns s, so that a switch and all of its cases fit in
// one statement, as in the example of [DagRef.Switch]:
//
//	dag.Switch(p.pickPath, airflow.Inputs(extracted)).Case(p.long).Case(p.short)
//
// The task runs after the decider, but its function does not take the result of the decider as a
// parameter.
//
// Case panics if:
//   - s is not the SwitchRef that DagRef.Switch or TaskGroupRef.Switch returned, for example a
//     copy of it
//   - task is nil, or neither DagRef.Task nor TaskGroupRef.Task added task to the Dag of the
//     switch
//   - task is already a case of s
//   - the Dag is already registered
func (s *SwitchRef) Case(task *TaskRef) *SwitchRef {
	const method = "airflow.SwitchRef.Case"
	// When the decider runs, it reads the cases from the SwitchRef that Switch returned. A case
	// given to a copy of that SwitchRef would never reach the decider.
	if s == nil || s.task == nil || s.task.decider != s {
		panic(method + ": DagRef.Switch or TaskGroupRef.Switch did not return the " +
			"*airflow.SwitchRef")
	}
	decider, d := s.task.taskID, s.task.dag
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.registered {
		panic(fmt.Sprintf(
			"%s: Dag %q has already been registered; "+
				"name the cases of every switch before Register",
			method, d.dagID,
		))
	}
	switch {
	case task == nil:
		panic(fmt.Sprintf(
			"%s: switch %q of Dag %q got a nil *airflow.TaskRef", method, decider, d.dagID,
		))
	case task.dag != nil && task.dag != d:
		panic(fmt.Sprintf(
			"%s: switch %q of Dag %q cannot choose task %q of another Dag, %q; "+
				"pass a task of the same Dag",
			method, decider, d.dagID, task.taskID, task.dag.dagID,
		))
	// A zero TaskRef and a copy of a TaskRef get here.
	case d.tasksByID[task.taskID] != task:
		panic(fmt.Sprintf(
			"%s: switch %q of Dag %q got a *airflow.TaskRef that DagRef.Task or "+
				"TaskGroupRef.Task did not return",
			method, decider, d.dagID,
		))
	case slices.Contains(s.cases, task):
		panic(fmt.Sprintf(
			"%s: switch %q of Dag %q already has task %q as a case; name each case once",
			method, decider, d.dagID, task.taskID,
		))
	}
	s.cases = append(s.cases, task)
	// The case runs after the decider, so the case is a downstream task of the decider. Recording
	// the edge puts the decider in the serialized Dag as the upstream of the case, and lets
	// registration see a cycle that runs through a switch.
	d.addEdgeLocked(s.task, task, "")
	return s
}

// wrap wraps fn as the task of s. The task skips the cases of s that fn does not choose.
func (s *SwitchRef) wrap(fn any) (bundle.Task, error) {
	fnType := reflect.TypeOf(fn)
	if fnType.NumOut() != 2 ||
		fnType.Out(0) != reflect.TypeFor[*TaskRef]() ||
		fnType.Out(1) != reflect.TypeFor[error]() {
		return nil, fmt.Errorf(
			"%s returns %s, but a decider function must return (*airflow.TaskRef, error)",
			funcName(fn), describeResults(fnType),
		)
	}
	return bundle.NewPositionalBranchFunction(fn, s.decide)
}

func (s *SwitchRef) bind(task *TaskRef) { s.task = task }

// decide returns the task_id of result, the case that the decider chose, as the value to push, and
// the task_ids of the other cases of s to skip. It returns an error when result is not a case of s.
func (s *SwitchRef) decide(result any) (any, []string, error) {
	chosen := result.(*TaskRef)
	if !slices.Contains(s.cases, chosen) {
		cases := make([]string, len(s.cases))
		for i, task := range s.cases {
			cases[i] = strconv.Quote(task.taskID)
		}
		return nil, nil, fmt.Errorf(
			"switch %q of Dag %q returned %s, which is not one of its cases: %s",
			s.task.taskID, s.task.dag.dagID, s.describeChoice(chosen), strings.Join(cases, ", "),
		)
	}
	skipped := make([]string, 0, len(s.cases)-1)
	for _, task := range s.cases {
		if task != chosen {
			skipped = append(skipped, task.taskID)
		}
	}
	return chosen.taskID, skipped, nil
}

// describeChoice names chosen, which the decider of s returned and which is not a case of s.
func (s *SwitchRef) describeChoice(chosen *TaskRef) string {
	d := s.task.dag
	switch {
	case chosen == nil:
		return "a nil *airflow.TaskRef"
	// A zero TaskRef gets here.
	case chosen.dag == nil:
		return "a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task did not return"
	case chosen.dag != d:
		return fmt.Sprintf("task %q of another Dag, %q", chosen.taskID, chosen.dag.dagID)
	case d.tasksByID[chosen.taskID] != chosen:
		return fmt.Sprintf("a copy of the *airflow.TaskRef of task %q", chosen.taskID)
	default:
		return fmt.Sprintf("task %q", chosen.taskID)
	}
}
