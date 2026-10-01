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
	"slices"
	"time"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// TriggerDagRunSpec holds the options of a task that triggers a Dag run. [TriggerDagRun] takes a
// TriggerDagRunSpec as its argument. Each field sets one parameter of TriggerDagRunOperator. DagID
// sets trigger_dag_id and RunID sets trigger_run_id. Every other field sets the parameter of the
// same name in snake_case, such as reset_dag_run for ResetDagRun. A field left at its zero value
// leaves its parameter at the Python default. PokeInterval and Deferrable are pointers so that a
// pointer to 0 or false can set that value instead of leaving the default.
//
// DagID, RunID, LogicalDate and the values in Conf are templated. They can hold Jinja such as
// "{{ ds }}", which Airflow renders when the task runs.
type TriggerDagRunSpec struct {
	// DagID is the dag_id of the Dag to trigger. It is required.
	DagID string
	// RunID is the run_id of the new Dag run. When RunID is empty, Airflow generates one.
	RunID string
	// Conf is the conf of the new Dag run. Airflow receives it as JSON, so each value must
	// marshal to JSON.
	Conf map[string]any
	// LogicalDate is the logical date of the new Dag run, as an ISO 8601 string such as
	// "2026-09-30T00:00:00+00:00" or a template such as "{{ ds }}". When LogicalDate is empty and
	// RunAfter is the zero Time, the logical date is the time the task runs. When LogicalDate is
	// empty and RunAfter is set, the new Dag run has no logical date.
	LogicalDate string
	// RunAfter is the earliest time at which the new Dag run can start. When RunAfter is the zero
	// Time, the new Dag run can start as soon as the task triggers it.
	RunAfter time.Time
	// ResetDagRun clears the Dag run if it already exists, instead of failing the task.
	ResetDagRun bool
	// WaitForCompletion makes the task wait until the new Dag run is in a state that
	// AllowedStates or FailedStates lists.
	WaitForCompletion bool
	// PokeInterval is how often a task that waits checks the state of the new Dag run. It must be
	// a whole number of seconds. When PokeInterval is nil, the task checks every 60 seconds.
	PokeInterval *time.Duration
	// AllowedStates are the states of the new Dag run in which a task that waits succeeds. Each
	// state is one of queued, running, success and failed. When AllowedStates is empty, the task
	// succeeds in the success state.
	AllowedStates []string
	// FailedStates are the states of the new Dag run in which a task that waits fails. Each state
	// is one of queued, running, success and failed. A nil FailedStates fails the task in the
	// failed state. A FailedStates that is empty but not nil means that no state fails the task.
	// The task then succeeds once the new Dag run is in a state that AllowedStates lists. In any
	// other state, including failed, the task keeps waiting.
	FailedStates []string
	// SkipWhenAlreadyExists marks the task skipped if the Dag run already exists.
	SkipWhenAlreadyExists bool
	// FailWhenDagIsPaused fails the task when the Dag to trigger is paused.
	FailWhenDagIsPaused bool
	// Note is the note of the new Dag run.
	Note string
	// Deferrable makes a task that waits defer instead of holding a worker slot. When Deferrable
	// is nil, the task follows the default_deferrable option in the operators section of the
	// Airflow configuration.
	Deferrable *bool
}

// TriggerDagRunTask is what [TriggerDagRun] returns. [DagRef.Task] takes it in place of a Go
// function and adds a task that triggers a Dag run.
type TriggerDagRunTask struct {
	spec TriggerDagRunSpec
}

// TriggerDagRun returns a value to pass to [DagRef.Task] in place of a Go function. DagRef.Task
// then adds a task that triggers a run of the Dag that spec.DagID names:
//
//	dag.Task(
//		airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagID: "downstream_etl"}),
//		airflow.TaskSpec{TaskID: "trigger_downstream"},
//	)
//
// The task runs no Go code. Once [BundleRef.Serve] serves the Dags from [Dag], Airflow will run
// the task as TriggerDagRunOperator on a Python worker.
//
// Because the task has no Go function to take a task_id from, DagRef.Task needs a [TaskSpec]
// that sets TaskID. For the same reason, the task takes no [Inputs], and it returns no result
// that Inputs can pass to another task. DagRef.Task also checks spec and panics if spec is not
// valid.
func TriggerDagRun(spec TriggerDagRunSpec) TriggerDagRunTask {
	return TriggerDagRunTask{spec: spec}
}

// validDagRunStates are the values that TriggerDagRunOperator accepts in allowed_states and
// failed_states.
var validDagRunStates = []string{
	string(genmodels.DagRunStateQueued),
	string(genmodels.DagRunStateRunning),
	string(genmodels.DagRunStateSuccess),
	string(genmodels.DagRunStateFailed),
}

// copyTriggerDagRunSpec checks spec and returns a deep copy of it, so that nothing the caller
// still holds, such as Conf, a state slice or a pointer field, can change the task that
// DagRef.Task added.
func copyTriggerDagRunSpec(spec TriggerDagRunSpec) (TriggerDagRunSpec, error) {
	if spec.DagID == "" {
		return TriggerDagRunSpec{}, errors.New("airflow.TriggerDagRunSpec has no DagID")
	}
	if poke := spec.PokeInterval; poke != nil && (*poke < 0 || *poke%time.Second != 0) {
		return TriggerDagRunSpec{}, fmt.Errorf(
			"airflow.TriggerDagRunSpec.PokeInterval is %v; "+
				"it must be a whole number of seconds and not negative",
			*poke,
		)
	}
	for _, field := range []struct {
		name   string
		states []string
	}{{"AllowedStates", spec.AllowedStates}, {"FailedStates", spec.FailedStates}} {
		for _, state := range field.states {
			if !slices.Contains(validDagRunStates, state) {
				return TriggerDagRunSpec{}, fmt.Errorf(
					"airflow.TriggerDagRunSpec.%s has %q, which is not a Dag run state; "+
						"use one of %q",
					field.name, state, validDagRunStates,
				)
			}
		}
	}
	conf, err := copyConf(spec.Conf)
	if err != nil {
		return TriggerDagRunSpec{}, fmt.Errorf("airflow.TriggerDagRunSpec.Conf: %w", err)
	}

	copied := copySpec(spec)
	// copySpec copies the Conf map but leaves anything nested in its values shared, such as an
	// inner map, so Conf is the copy that copyConf made.
	copied.Conf = conf
	return copied, nil
}

// copyConf copies conf by way of JSON, so it also rejects a conf that JSON cannot hold.
//
// UseNumber stores each number as a json.Number, which keeps an integer that a float64 cannot
// hold exactly, such as 2^53 + 1. encoding/json writes a json.Number back as a number, but an
// encoder that does not know the type, such as msgpack, writes it as a string.
func copyConf(conf map[string]any) (map[string]any, error) {
	data, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var copied map[string]any
	if err := dec.Decode(&copied); err != nil {
		return nil, err
	}
	return copied, nil
}
