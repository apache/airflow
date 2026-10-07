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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// TriggerDagRunSpec holds the options of a task that triggers a Dag run. [TriggerDagRun] takes a
// TriggerDagRunSpec as its argument. Each field sets one parameter of TriggerDagRunOperator. DagID
// sets trigger_dag_id and RunID sets trigger_run_id. Every other field sets the parameter of the
// same name in snake_case, such as reset_dag_run for ResetDagRun. A field left at its zero value
// leaves its parameter at the Python default. PokeInterval and Deferrable are pointers so that a
// pointer to 0 or false can set that value instead of leaving the default.
//
// The task does not render templates. DagID, RunID, Note and the values in Conf are sent as they
// are, so a string such as "{{ ds }}" reaches the new Dag run unchanged.
type TriggerDagRunSpec struct {
	// DagID is the dag_id of the Dag to trigger. It is required.
	DagID string
	// RunID is the run_id of the new Dag run. When RunID is empty, Airflow generates one.
	RunID string
	// Conf is the conf of the new Dag run. Each value must marshal to JSON, and each integer in
	// Conf must fit in 64 bits.
	Conf map[string]any
	// LogicalDate is the logical date of the new Dag run. When LogicalDate is the zero Time and
	// RunAfter is also the zero Time, the logical date is the time the task runs. When LogicalDate
	// is the zero Time and RunAfter is set, the new Dag run has no logical date.
	LogicalDate time.Time
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
	// AllowedStates are the states of the new Dag run in which a task that waits succeeds. When
	// AllowedStates is empty, the task succeeds in the success state of the new Dag run.
	AllowedStates []DagRunState
	// FailedStates are the states of the new Dag run in which a task that waits fails. When
	// FailedStates is nil, the task fails in the failed state of the new Dag run. When
	// FailedStates is empty but not nil, no Dag run state fails the task. The task then succeeds
	// once the new Dag run is in a state that AllowedStates lists. In any other state, including
	// failed, the task keeps waiting.
	FailedStates []DagRunState
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

// TriggerDagRun returns a value to pass to [DagRef.Task] or [TaskGroupRef.Task] in place of a Go
// function. DagRef.Task then adds a task that triggers a run of the Dag that spec.DagID names:
//
//	dag.Task(
//		airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagID: "downstream_etl"}),
//		airflow.TaskSpec{TaskID: "trigger_downstream"},
//	)
//
// The task runs no Go code. Once [BundleRef.Serve] serves the Dags from [Dag], the Go runtime runs
// the task as TriggerDagRunOperator does, so a Dag from [Dag] needs no Python worker for it. The
// task takes the Queue of the [DagSpec], unless its [TaskSpec] sets one. The runtime reads the
// settings it needs from its environment: [api] base_url for the link to the new Dag run, and
// [operators] default_deferrable for a spec that leaves Deferrable nil. Set Deferrable to defer the
// wait, which hands it to the Python triggerer as the DagStateTrigger of the standard provider.
//
// Because the task has no Go function to take a task_id from, DagRef.Task needs a [TaskSpec]
// that sets TaskID. For the same reason, the task takes no [Inputs], and it returns no result
// that Inputs can pass to another task. DagRef.Task also checks spec and panics if spec is not
// valid.
func TriggerDagRun(spec TriggerDagRunSpec) TriggerDagRunTask {
	return TriggerDagRunTask{spec: spec}
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
		states []DagRunState
	}{{"AllowedStates", spec.AllowedStates}, {"FailedStates", spec.FailedStates}} {
		for _, state := range field.states {
			if !slices.Contains(dagRunStates, state) {
				return TriggerDagRunSpec{}, fmt.Errorf(
					"airflow.TriggerDagRunSpec.%s has %q, which is not a Dag run state; "+
						"use one of %q",
					field.name, state, dagRunStates,
				)
			}
		}
	}
	if err := checkTime("airflow.TriggerDagRunSpec.RunAfter", spec.RunAfter); err != nil {
		return TriggerDagRunSpec{}, err
	}
	if err := checkTime("airflow.TriggerDagRunSpec.LogicalDate", spec.LogicalDate); err != nil {
		return TriggerDagRunSpec{}, err
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

// triggerSpec returns the options of spec for the runtime, which cannot import this package. The
// checked spec is a deep copy, but the runtime gets one of its own, so that nothing the runtime
// does can change the task.
func triggerSpec(spec TriggerDagRunSpec) bundle.TriggerSpec {
	copied := copySpec(spec)
	out := bundle.TriggerSpec{
		DagID:                 copied.DagID,
		RunID:                 copied.RunID,
		Note:                  copied.Note,
		LogicalDate:           copied.LogicalDate,
		RunAfter:              copied.RunAfter,
		ResetDagRun:           copied.ResetDagRun,
		WaitForCompletion:     copied.WaitForCompletion,
		SkipWhenAlreadyExists: copied.SkipWhenAlreadyExists,
		FailWhenDagIsPaused:   copied.FailWhenDagIsPaused,
		PokeInterval:          copied.PokeInterval,
		Deferrable:            copied.Deferrable,
	}
	if copied.Conf != nil {
		out.Conf = copyJSON(copied.Conf).(map[string]any)
	}
	if len(copied.AllowedStates) > 0 {
		out.AllowedStates = dagRunStateNames(copied.AllowedStates)
	}
	if copied.FailedStates != nil {
		out.FailedStates = dagRunStateNames(copied.FailedStates)
	}
	return out
}

// copyConf copies conf by way of JSON, so it also rejects a conf that JSON cannot hold.
//
// Each number in the copy is an int64, a uint64 or a float64, which encoding/json and msgpack both
// write as a number. A bundle sends a serialized Dag to Airflow as msgpack, and msgpack would write
// a json.Number as a string. The decoder reads each number as a json.Number first, so that an
// integer that a float64 cannot hold exactly, such as 2^53 + 1, keeps its value in an int64 or a
// uint64. copyConf rejects an integer that fits in neither, and a number past the range of a
// float64, because msgpack cannot carry them.
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
	if _, err := resolveNumbers(copied, ""); err != nil {
		return nil, err
	}
	return copied, nil
}

// resolveNumbers replaces each json.Number in value with an int64, a uint64 or a float64, and
// returns value. It changes a map or a slice in value in place. path names value in an error, as
// in ["rows"][2].
func resolveNumbers(value any, path string) (any, error) {
	switch v := value.(type) {
	case json.Number:
		return resolveNumber(v, path)
	case map[string]any:
		// The keys are sorted, so that a conf with more than one bad number always gets the same
		// error.
		for _, key := range slices.Sorted(maps.Keys(v)) {
			resolved, err := resolveNumbers(v[key], path+"["+strconv.Quote(key)+"]")
			if err != nil {
				return nil, err
			}
			v[key] = resolved
		}
	case []any:
		for i, item := range v {
			resolved, err := resolveNumbers(item, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			v[i] = resolved
		}
	}
	return value, nil
}

func resolveNumber(n json.Number, path string) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return u, nil
	}
	f, err := n.Float64()
	// encoding/json writes a float64 such as 1e20 with neither a point nor an exponent, so a number
	// written like an integer can come from a float64. Such a number is read as a float64 only when
	// the float64 formats back to the same digits. Otherwise the number is an integer that does not
	// fit in 64 bits.
	if !strings.ContainsAny(n.String(), ".eE") &&
		(err != nil || strconv.FormatFloat(f, 'f', -1, 64) != n.String()) {
		return nil, fmt.Errorf("the integer at %s is %s, which does not fit in 64 bits", path, n)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"the number at %s is %s, which is past the range of a float64",
			path,
			n,
		)
	}
	return f, nil
}
