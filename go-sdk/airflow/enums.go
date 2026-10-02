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
	"slices"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// This file declares every enum type whose values a Dag author sets. Each item below gives
// the Go enum, the Python file that declares the same enum, and the fields that hold its values:
//
//   - TriggerRule: airflow-core/src/airflow/task/trigger_rule.py, for TaskSpec.TriggerRule
//   - WeightRule: airflow-core/src/airflow/task/weight_rule.py, for TaskSpec.WeightRule
//   - DagRunState: airflow-core/src/airflow/utils/state.py, for TriggerDagRunSpec.AllowedStates
//     and TriggerDagRunSpec.FailedStates
//
// Two more Python enums are not declared here yet, because no field that an author can reach
// holds their values:
//
//   - DagRunType: airflow-core/src/airflow/utils/types.py. DagSpec leaves out
//     allowed_run_types, and DagRun has no run type field.
//   - TaskInstanceState: airflow-core/src/airflow/utils/state.py. TaskInstance has no state field.
//
// Adding such a field also means declaring its enum here, the way DagRunState is declared.
//
// An enum is a named string type, and each constant has the type name as its prefix, as in
// TriggerRuleAllDone. Go does not scope a constant to its type, so without the prefix, a name
// such as airflow.AllDone would not say which enum the constant belongs to. A field with a fixed
// set of values gets its enum type before the field is released. Changing a released field from
// string to an enum type breaks every Dag that sets the field from a value of type string, such
// as a string variable.
//
// DagRef.Task panics on a TriggerRule, WeightRule or DagRunState value that is not one of the
// constants of that type. The one exception is an empty TriggerRule or WeightRule, which leaves
// that rule unset.
//
// The Dag serialization schema types trigger_rule and weight_rule as plain strings and does not
// list their values. TriggerRule, WeightRule and their constants are therefore written by hand in
// this file, and internal/genspec/authoring.go gives these types to the generated fields
// TaskSpec.TriggerRule and TaskSpec.WeightRule.

// TriggerRule decides when a task runs, based on the states of its upstream tasks. An empty
// TriggerRule leaves the trigger rule of the task unset.
type TriggerRule string

const (
	TriggerRuleAllSuccess              TriggerRule = "all_success"
	TriggerRuleAllFailed               TriggerRule = "all_failed"
	TriggerRuleAllDone                 TriggerRule = "all_done"
	TriggerRuleAllDoneMinOneSuccess    TriggerRule = "all_done_min_one_success"
	TriggerRuleAllDoneSetupSuccess     TriggerRule = "all_done_setup_success"
	TriggerRuleOneSuccess              TriggerRule = "one_success"
	TriggerRuleOneFailed               TriggerRule = "one_failed"
	TriggerRuleOneDone                 TriggerRule = "one_done"
	TriggerRuleNoneFailed              TriggerRule = "none_failed"
	TriggerRuleNoneFailedMinOneSuccess TriggerRule = "none_failed_min_one_success"
	TriggerRuleNoneSkipped             TriggerRule = "none_skipped"
	TriggerRuleAllSkipped              TriggerRule = "all_skipped"
	TriggerRuleAlways                  TriggerRule = "always"
)

// WeightRule decides how Airflow computes the effective priority weight of a task.
// WeightRuleAbsolute uses the TaskSpec.PriorityWeight of the task alone. WeightRuleDownstream
// sums the PriorityWeight of the task and of every task downstream of it. WeightRuleUpstream sums
// the PriorityWeight of the task and of every task upstream of it. An empty WeightRule leaves the
// weight rule of the task unset.
//
// Python also accepts the import path of a PriorityWeightStrategy class for weight_rule. The
// class can be built into Airflow or registered by a plugin. A Go task can only name one of the
// three built-in rules.
type WeightRule string

const (
	WeightRuleDownstream WeightRule = "downstream"
	WeightRuleUpstream   WeightRule = "upstream"
	WeightRuleAbsolute   WeightRule = "absolute"
)

// DagRunState is the state of a Dag run.
//
// DagRunState is an alias of genmodels.DagRunState so that an author does not import genmodels.
// The types in pkg/execution/genmodels are generated from the supervisor schema. Because
// DagRunState is an alias, %T and package reflect report its name as genmodels.DagRunState.
type DagRunState = genmodels.DagRunState

const (
	DagRunStateQueued  DagRunState = "queued"
	DagRunStateRunning DagRunState = "running"
	DagRunStateSuccess DagRunState = "success"
	DagRunStateFailed  DagRunState = "failed"
)

// triggerRules, weightRules and dagRunStates list the valid values of TriggerRule, WeightRule and
// DagRunState. Tests in this package check these lists and the constants above against the
// Python enums for TriggerRule and WeightRule, and against genmodels for DagRunState.
var (
	triggerRules = []TriggerRule{
		TriggerRuleAllSuccess,
		TriggerRuleAllFailed,
		TriggerRuleAllDone,
		TriggerRuleAllDoneMinOneSuccess,
		TriggerRuleAllDoneSetupSuccess,
		TriggerRuleOneSuccess,
		TriggerRuleOneFailed,
		TriggerRuleOneDone,
		TriggerRuleNoneFailed,
		TriggerRuleNoneFailedMinOneSuccess,
		TriggerRuleNoneSkipped,
		TriggerRuleAllSkipped,
		TriggerRuleAlways,
	}
	weightRules  = []WeightRule{WeightRuleDownstream, WeightRuleUpstream, WeightRuleAbsolute}
	dagRunStates = []DagRunState{
		DagRunStateQueued,
		DagRunStateRunning,
		DagRunStateSuccess,
		DagRunStateFailed,
	}
)

// checkTaskSpec rejects a TaskSpec whose TriggerRule is not a TriggerRule constant or whose
// WeightRule is not a WeightRule constant. An empty TriggerRule or WeightRule is valid and leaves
// that rule unset.
func checkTaskSpec(spec TaskSpec) error {
	if err := checkRule("TriggerRule", "trigger rule", spec.TriggerRule, triggerRules); err != nil {
		return err
	}
	return checkRule("WeightRule", "weight rule", spec.WeightRule, weightRules)
}

func checkRule[T ~string](field, kind string, value T, valid []T) error {
	if value == "" || slices.Contains(valid, value) {
		return nil
	}
	return fmt.Errorf(
		"airflow.TaskSpec.%s is %q, which is not a %s; use one of %q", field, value, kind, valid,
	)
}
