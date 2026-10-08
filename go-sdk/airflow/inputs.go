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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/airflow/go-sdk/pkg/binding"
)

// Input is a value that [Inputs] passes to a parameter of a task function: the result of another
// task, which is a *[TaskRef], or a literal value from [Literal].
//
// Its only method is unexported, so a type outside this package cannot declare it.
type Input interface{ input() }

func (*TaskRef) input() {}

// literal is the Input that Literal returns. Literal copies value through JSON, so that the Dag
// holds the copy and err records why the copy failed. checkInputs reports err for the task that
// takes the literal.
type literal struct {
	value any
	err   error
}

func (literal) input() {}

// Literal returns an [Input] that passes value to a parameter of a task function, where a *[TaskRef]
// passes the result of a task:
//
//	dag.Task(load, airflow.Inputs(transformed, airflow.Literal("s3://bucket/out")))
//
// value must be JSON. A string, a number, a bool, nil, a slice, a map with string keys or a struct
// all work, and so does anything else that encoding/json writes, including a type with a
// MarshalJSON method. A number must fit in 64 bits, and a float must not be NaN or infinite.
// Literal copies value when it is called, so a later change to value does not reach the Dag.
//
// When the task runs, the SDK decodes the JSON into the type of the parameter that the literal
// fills, as it does for the result of a task. So a literal map fills a struct parameter, and a
// whole number fills an int or a float64.
//
// The method that adds the task panics if the literal does not decode into the parameter type, or
// if value holds a *TaskRef, because a literal is data and a TaskRef is an edge. Pass the TaskRef
// to Inputs itself instead. A literal adds no edge to the Dag.
func Literal(value any) Input {
	copied, err := copyLiteral(value)
	return literal{value: copied, err: err}
}

// copyLiteral returns a copy of value made by way of JSON, with each number in the type that
// copyConf gives it. It returns an error for a value that JSON cannot hold, and for a *TaskRef
// anywhere in value.
func copyLiteral(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var copied any
	if err := dec.Decode(&copied); err != nil {
		return nil, err
	}
	if path, found := findTaskRef(reflect.ValueOf(value), "value", map[uintptr]bool{}); found {
		return nil, fmt.Errorf(
			"%s is a *airflow.TaskRef, which is an edge and not data; "+
				"pass the TaskRef to airflow.Inputs itself",
			path,
		)
	}
	return resolveNumbers(copied, "value")
}

var (
	taskRefType    = reflect.TypeFor[TaskRef]()
	taskRefPtrType = reflect.TypeFor[*TaskRef]()
)

// findTaskRef returns the path to the first TaskRef or *TaskRef that encoding/json would read
// inside v. It looks at exported struct fields only, as encoding/json does. seen holds the
// pointers, maps and slices already walked, so that a value that points at itself ends the walk.
func findTaskRef(v reflect.Value, path string, seen map[uintptr]bool) (string, bool) {
	if !v.IsValid() {
		return "", false
	}
	if t := v.Type(); t == taskRefType || t == taskRefPtrType {
		return path, true
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() || seen[v.Pointer()] {
			return "", false
		}
		seen[v.Pointer()] = true
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return findTaskRef(v.Elem(), path, seen)
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if found, ok := findTaskRef(v.Index(i), fmt.Sprintf("%s[%d]", path, i), seen); ok {
				return found, true
			}
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			name := fmt.Sprintf("%s[%v]", path, key)
			if found, ok := findTaskRef(v.MapIndex(key), name, seen); ok {
				return found, true
			}
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if field := v.Type().Field(i); field.IsExported() {
				if found, ok := findTaskRef(v.Field(i), path+"."+field.Name, seen); ok {
					return found, true
				}
			}
		}
	}
	return "", false
}

// taskInput is one Input after the method that adds the task has checked it. A task input holds
// either a ref, the task whose result fills the parameter, or a literal value.
type taskInput struct {
	ref     *TaskRef
	value   any
	literal bool
}

type inputs []Input

func (in inputs) applyTask(c *taskConfig) error {
	if c.hasInputs {
		return errors.New(
			"got more than one airflow.Inputs; pass all of the task's inputs to one airflow.Inputs",
		)
	}
	c.inputs, c.hasInputs = in, true
	return nil
}

// Inputs passes the results of tasks and literal values to a task that [DagRef.Task], [DagRef.If],
// [DagRef.Switch] or a method of the same name on [TaskGroupRef] adds. Each task becomes an upstream
// task of the new one. It is the Go form of a Python TaskFlow call such as transform(extract()):
//
//	extracted := dag.Task(extract)
//	transformed := dag.Task(transform, airflow.Inputs(extracted))
//	dag.Task(load, airflow.Inputs(transformed, airflow.Literal("s3://bucket/out")))
//
// The first input fills the first parameter after the [Context], the second input fills the
// second parameter, and so on. An input is a *[TaskRef], which passes the result of that task, or
// the value of [Literal], which passes the literal itself. For the Dag above, the task functions
// could be:
//
//	func extract(actx airflow.Context) ([]string, error)
//	func transform(actx airflow.Context, rows []string) (int, error)
//	func load(actx airflow.Context, count int, target string) error
//
// The SDK decodes the result of a task, or a literal, from JSON into the type of the parameter
// that it fills. So each field of a struct parameter comes from the matching JSON key. A parameter
// of type any gets a map[string]any when the value is a struct.
//
// The method that adds the task panics unless each parameter after the Context gets exactly one
// input. A task input must be a task of the same Dag, inside a task group or not, and the result
// type of that task must be assignable to the parameter type, as in a Go function call. A literal
// must decode into the parameter type. Pass at most one Inputs to a task.
func Inputs(in ...Input) TaskOption { return inputs(in) }

// checkInputs returns the inputs that task taskID got through Inputs, in a new slice. It panics if
// any task among them was not added to d by DagRef.Task, or if the inputs do not match the
// parameters that a function of type fnType takes after the Context. method names the caller in
// panic messages.
func (d *DagRef) checkInputs(
	method, taskID string,
	fnType reflect.Type,
	in []Input,
) []taskInput {
	checked := make([]taskInput, len(in))
	for i, input := range in {
		switch input := input.(type) {
		case nil:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q: airflow.Inputs got a nil input at index %d",
				method, taskID, d.dagID, i,
			))
		case literal:
			checked[i] = taskInput{value: input.value, literal: true}
		case *TaskRef:
			d.checkInputRef(method, taskID, i, input)
			checked[i] = taskInput{ref: input}
		}
	}

	// newTaskFunction has checked that the function takes the Context first and only data
	// after it.
	if params := fnType.NumIn() - 1; len(in) != params {
		panic(fmt.Sprintf(
			"%s: task %q of Dag %q has %d parameter(s) after airflow.Context, "+
				"but airflow.Inputs passes %s",
			method, taskID, d.dagID, params, describeInputs(checked),
		))
	}
	for i, input := range checked {
		param := i + 1
		paramType := fnType.In(param)
		if input.literal {
			if err := in[i].(literal).err; err != nil {
				panic(fmt.Sprintf(
					"%s: task %q of Dag %q: the airflow.Literal for parameter %d is not valid: %v",
					method, taskID, d.dagID, param, err,
				))
			}
			// The runtime decodes the literal with the same function.
			if _, err := binding.DecodeLiteral(input.value, paramType); err != nil {
				panic(fmt.Sprintf(
					"%s: task %q of Dag %q: the airflow.Literal for parameter %d cannot "+
						"be decoded into %s: %v",
					method, taskID, d.dagID, param, paramType, err,
				))
			}
			continue
		}
		upstream := input.ref
		switch {
		case upstream.triggerDagRun != nil:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task comes from airflow.TriggerDagRun and returns no result",
				method, taskID, d.dagID, param, upstream.taskID,
			))
		case upstream.resultType == nil:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task returns only an error",
				method, taskID, d.dagID, param, upstream.taskID,
			))
		case !upstream.resultType.AssignableTo(paramType):
			var sameName string
			if upstream.resultType.String() == paramType.String() {
				sameName = ", a different type with the same name"
			}
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes parameter %d from task %q, "+
					"but that task returns %s, which cannot be assigned to %s%s",
				method, taskID, d.dagID, param, upstream.taskID, upstream.resultType, paramType,
				sameName,
			))
		}
	}
	return checked
}

// checkInputRef panics unless upstream is a task that DagRef.Task or TaskGroupRef.Task added to d.
// i is the index of the input.
func (d *DagRef) checkInputRef(method, taskID string, i int, upstream *TaskRef) {
	switch {
	case upstream == nil:
		panic(fmt.Sprintf(
			"%s: task %q of Dag %q: airflow.Inputs got a nil *airflow.TaskRef at index %d",
			method, taskID, d.dagID, i,
		))
	case upstream.dag != nil && upstream.dag != d:
		panic(fmt.Sprintf(
			"%s: task %q of Dag %q cannot take an input from task %q of "+
				"another Dag, %q; pass tasks of the same Dag to airflow.Inputs",
			method, taskID, d.dagID, upstream.taskID, upstream.dag.dagID,
		))
	// A zero TaskRef and a copy of a TaskRef get here.
	case d.tasksByID[upstream.taskID] != upstream:
		panic(fmt.Sprintf(
			"%s: task %q of Dag %q: airflow.Inputs got a *airflow.TaskRef "+
				"at index %d that DagRef.Task or TaskGroupRef.Task did not return",
			method, taskID, d.dagID, i,
		))
	}
}

func describeInputs(inputs []taskInput) string {
	if len(inputs) == 0 {
		return "no input"
	}
	described := make([]string, len(inputs))
	for i, input := range inputs {
		if input.literal {
			described[i] = "a literal"
		} else {
			described[i] = strconv.Quote(input.ref.taskID)
		}
	}
	return fmt.Sprintf("%d input(s): %s", len(inputs), strings.Join(described, ", "))
}
