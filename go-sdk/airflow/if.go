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
	"strings"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// IfRef is a condition that [DagRef.If] added to a Dag. [IfRef.Then] names the task that runs
// when the condition is true, and [IfRef.Else] names the task that runs when it is false.
type IfRef struct {
	// task is the task that runs the condition function. thenTask and elseTask run after task,
	// but neither takes the result of task.
	task     *TaskRef
	thenTask *TaskRef
	elseTask *TaskRef
}

// If adds a task that runs the condition function fn, and returns the condition. Use
// [IfRef.Then] to name the task that runs when fn returns true, and [IfRef.Else] to name the task
// that runs when fn returns false:
//
//	extracted := dag.Task(extract)
//	loaded := dag.Task(load)
//	reportedEmpty := dag.Task(reportEmpty)
//
//	gate := dag.If(hasRows, airflow.Inputs(extracted))
//	gate.Then(loaded)
//	gate.Else(reportedEmpty)
//
// fn returns (bool, error). In every other way it follows the rules of a function passed to
// [DagRef.Task]: it takes a [Context] first, and [Inputs] fills the parameters after the Context.
// For the Dag above, the functions could be:
//
//	func extract(actx airflow.Context) ([]string, error)
//	func hasRows(actx airflow.Context, rows []string) (bool, error)
//	func load(actx airflow.Context) error
//	func reportEmpty(actx airflow.Context) error
//
// The task that If adds gets its task_id in the same way as a task from DagRef.Task, and a
// [TaskSpec] sets its attributes.
//
// When the task runs, the result of fn decides what it skips:
//   - true skips the task from Else, or nothing if there is no task from Else
//   - false skips the task from Then
//   - an error fails the task, which then skips nothing
//
// The condition skips only that one task. In Python, a branch skips every task directly after it
// that it does not follow, and a short circuit skips every task after it. After the skip, the
// trigger rule of each task after the skipped task decides whether that task runs. A task that
// runs after both the task from Then and the task from Else needs a trigger rule that runs it when
// one of the two is skipped, such as [TriggerRuleNoneFailedMinOneSuccess].
//
// [BundleRef.Register] panics if a condition has no task from Then.
//
// If panics for the same reasons as DagRef.Task does for a Go function. It also panics if fn
// comes from [TriggerDagRun] or does not return (bool, error).
func (d *DagRef) If(fn any, opts ...TaskOption) *IfRef {
	if _, ok := fn.(TriggerDagRunTask); ok {
		panic(fmt.Sprintf(
			"airflow.DagRef.If: Dag %q: fn comes from airflow.TriggerDagRun, "+
				"but a condition function is a Go function that returns (bool, error)",
			d.dagID,
		))
	}
	ifRef := &IfRef{}
	d.addTask("airflow.DagRef.If", fn, opts, ifRef)
	return ifRef
}

// Then names the task that runs when the condition of g is true. When the condition is false,
// the task is skipped. Then returns g, so that a call to Else can follow. With the tasks from the
// example of [DagRef.If], the condition fits in one statement:
//
//	dag.If(hasRows, airflow.Inputs(extracted)).Then(loaded).Else(reportedEmpty)
//
// The task runs after the condition, but its function does not take the result of the
// condition as a parameter.
//
// Then panics if:
//   - g is not the IfRef that DagRef.If returned, for example a copy of that IfRef
//   - task is nil, or DagRef.Task did not add task to the Dag of the condition
//   - the condition already has a task from Then
//   - task is the task from Else
//   - the Dag is already registered
func (g *IfRef) Then(task *TaskRef) *IfRef {
	g.setTask("Then", task)
	return g
}

// Else names the task that runs when the condition of g is false. When the condition is true,
// the task is skipped. Like Then, Else returns g. A condition does not need a task from Else.
//
// Else panics for the reasons that Then lists, with Then and Else swapped.
func (g *IfRef) Else(task *TaskRef) *IfRef {
	g.setTask("Else", task)
	return g
}

// setTask makes task the task from Then or from Else of g. side is "Then" or "Else".
func (g *IfRef) setTask(side string, task *TaskRef) {
	method := "airflow.IfRef." + side
	// When the condition task runs, it reads the tasks from Then and Else from the IfRef that If
	// returned. A task given to a copy of that IfRef would never reach the condition task.
	if g == nil || g.task == nil || g.task.ifRef != g {
		panic(method + ": DagRef.If did not return the *airflow.IfRef")
	}
	condition, d := g.task.taskID, g.task.dag
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.registered {
		panic(fmt.Sprintf(
			"%s: Dag %q has already been registered; "+
				"name the tasks of every condition before Register",
			method, d.dagID,
		))
	}
	switch {
	case task == nil:
		panic(fmt.Sprintf(
			"%s: condition %q of Dag %q got a nil *airflow.TaskRef", method, condition, d.dagID,
		))
	case task.dag != nil && task.dag != d:
		panic(fmt.Sprintf(
			"%s: condition %q of Dag %q cannot run task %q of another Dag, %q; "+
				"pass a task of the same Dag",
			method, condition, d.dagID, task.taskID, task.dag.dagID,
		))
	// A zero TaskRef and a copy of a TaskRef get here.
	case d.tasksByID[task.taskID] != task:
		panic(fmt.Sprintf(
			"%s: condition %q of Dag %q got a *airflow.TaskRef that DagRef.Task did not return",
			method, condition, d.dagID,
		))
	}

	slot, other, otherSide := &g.thenTask, g.elseTask, "Else"
	if side == "Else" {
		slot, other, otherSide = &g.elseTask, g.thenTask, "Then"
	}
	switch {
	case *slot != nil:
		panic(fmt.Sprintf(
			"%s: condition %q of Dag %q already has task %q from %s; call %s once",
			method, condition, d.dagID, (*slot).taskID, side, side,
		))
	case task == other:
		panic(fmt.Sprintf(
			"%s: condition %q of Dag %q already has task %q from %s, "+
				"and a task cannot be on both sides of a condition",
			method, condition, d.dagID, task.taskID, otherSide,
		))
	}
	*slot = task
}

// wrapCondition wraps fn as the task of g. The task skips the side of g that the result of fn
// does not take.
func (g *IfRef) wrapCondition(fn any) (bundle.Task, error) {
	fnType := reflect.TypeOf(fn)
	// DagRef.Task also takes a function whose last result has a concrete type that implements
	// error. A nil value of that type becomes a non-nil error when the runtime reads it, so the
	// condition task would always fail.
	if fnType.NumOut() != 2 ||
		fnType.Out(0) != reflect.TypeFor[bool]() ||
		fnType.Out(1) != reflect.TypeFor[error]() {
		return nil, fmt.Errorf(
			"%s returns %s, but a condition function must return (bool, error)",
			funcName(fn), describeResults(fnType),
		)
	}
	return bundle.NewPositionalBranchFunction(fn, g.findSkipped)
}

// findSkipped returns the task_id of the task on the side of g that result does not take. It
// returns nil when that side has no task.
func (g *IfRef) findSkipped(result any) []string {
	d := g.task.dag
	d.mu.Lock()
	defer d.mu.Unlock()

	notTaken := g.thenTask
	if result.(bool) {
		notTaken = g.elseTask
	}
	if notTaken == nil {
		return nil
	}
	return []string{notTaken.taskID}
}

func describeResults(fnType reflect.Type) string {
	results := make([]string, fnType.NumOut())
	for i := range results {
		results[i] = fnType.Out(i).String()
	}
	switch len(results) {
	case 0:
		return "nothing"
	case 1:
		return results[0]
	default:
		return "(" + strings.Join(results, ", ") + ")"
	}
}
