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
	"slices"
	"strconv"
	"strings"
)

type inputs []*TaskRef

func (in inputs) applyTask(c *taskConfig) error {
	if c.hasInputs {
		return errors.New(
			"got more than one airflow.Inputs; pass all of the task's inputs to one airflow.Inputs",
		)
	}
	c.inputs, c.hasInputs = in, true
	return nil
}

// Inputs passes the results of tasks to the task that [DagRef.Task] adds, and makes each of
// those tasks an upstream task of the new one. It is the Go form of a Python TaskFlow call such
// as transform(extract()):
//
//	extracted := dag.Task(extract)
//	transformed := dag.Task(transform, airflow.Inputs(extracted))
//	dag.Task(load, airflow.Inputs(transformed))
//
// The result of the first task fills the first parameter after the [Context], the result of the
// second task fills the second parameter, and so on. For the Dag above, the task functions could
// be:
//
//	func extract(actx airflow.Context) ([]string, error)
//	func transform(actx airflow.Context, rows []string) (int, error)
//	func load(actx airflow.Context, count int) error
//
// The SDK decodes the result of a task from JSON into the type of the parameter that the result
// fills. So each field of a struct parameter comes from the matching JSON key. A parameter of
// type any gets a map[string]any when the result is a struct.
//
// When [DagRef.Task] adds the task, it panics unless each parameter after the Context gets
// exactly one task of the same Dag, and the result type of that task is assignable to the
// parameter type, as in a Go function call. Pass at most one Inputs to a task.
func Inputs(refs ...*TaskRef) TaskOption { return inputs(refs) }

// checkInputs returns the tasks that task taskID got through Inputs, in a new slice. It panics if
// any of those tasks was not added to d by DagRef.Task, or if the tasks do not match the
// parameters that a function of type fnType takes after the Context.
func (d *DagRef) checkInputs(taskID string, fnType reflect.Type, tasks []*TaskRef) []*TaskRef {
	for i, upstream := range tasks {
		switch {
		case upstream == nil:
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q: "+
					"airflow.Inputs got a nil *airflow.TaskRef at index %d",
				taskID, d.dagID, i,
			))
		case upstream.dag != nil && upstream.dag != d:
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q cannot take an input from task %q of "+
					"another Dag, %q; pass tasks of the same Dag to airflow.Inputs",
				taskID, d.dagID, upstream.taskID, upstream.dag.dagID,
			))
		// A zero TaskRef and a copy of a TaskRef get here.
		case d.tasksByID[upstream.taskID] != upstream:
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q: airflow.Inputs got a *airflow.TaskRef "+
					"at index %d that DagRef.Task did not return",
				taskID, d.dagID, i,
			))
		}
	}

	// newTaskFunction has checked that the function takes the Context first and only data
	// after it.
	if params := fnType.NumIn() - 1; len(tasks) != params {
		panic(fmt.Sprintf(
			"airflow.DagRef.Task: task %q of Dag %q has %d parameter(s) after airflow.Context, "+
				"but airflow.Inputs passes %s",
			taskID, d.dagID, params, describeTasks(tasks),
		))
	}
	for i, upstream := range tasks {
		param := i + 1
		paramType := fnType.In(param)
		switch {
		case upstream.triggerDagRun != nil:
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task comes from airflow.TriggerDagRun and returns no result",
				taskID, d.dagID, param, upstream.taskID,
			))
		case upstream.resultType == nil:
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task returns only an error",
				taskID, d.dagID, param, upstream.taskID,
			))
		case !upstream.resultType.AssignableTo(paramType):
			var sameName string
			if upstream.resultType.String() == paramType.String() {
				sameName = ", a different type with the same name"
			}
			panic(fmt.Sprintf(
				"airflow.DagRef.Task: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task returns %s, which cannot be assigned to %s%s",
				taskID, d.dagID, param, upstream.taskID, upstream.resultType, paramType, sameName,
			))
		}
	}
	// Inputs keeps the slice that its caller passed, so the task gets a copy that the caller
	// cannot change.
	return slices.Clone(tasks)
}

func describeTasks(tasks []*TaskRef) string {
	if len(tasks) == 0 {
		return "no task"
	}
	taskIDs := make([]string, len(tasks))
	for i, task := range tasks {
		taskIDs[i] = strconv.Quote(task.taskID)
	}
	return fmt.Sprintf("%d task(s): %s", len(tasks), strings.Join(taskIDs, ", "))
}
