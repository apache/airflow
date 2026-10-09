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

package airflow_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/airflow"
)

// TestSerializeConformanceDags builds the Dags of scripts/ci/lang_sdk_serialization/test_dags.yaml
// with this SDK, serializes them, and writes them to a JSON file keyed by dag_id.
// serialize_python.py in the same directory does the same with Airflow's own serializer. compare.py
// there runs this test as the serializer of the Go SDK for the
// check-go-sdk-serialization-conformance prek hook, and passes the two paths after -args:
//
//	go -C go-sdk test ./airflow -count=1 -run '^TestSerializeConformanceDags$' -args <test_dags.yaml> <output.json>
//
// A plain go test run passes no paths, so the test skips.
//
// The Airflow core tests load airflow-core/tests/unit/dag_processing/lang_sdk_fixtures/go_native.json
// to check that Airflow still loads what this SDK writes. It is this test's output for every Dag of
// test_dags.yaml. After a change to the serializer or to test_dags.yaml, rewrite it from the
// repository root with
//
//	go -C go-sdk test ./airflow -count=1 -run '^TestSerializeConformanceDags$' -args \
//	  $PWD/scripts/ci/lang_sdk_serialization/test_dags.yaml \
//	  $PWD/airflow-core/tests/unit/dag_processing/lang_sdk_fixtures/go_native.json
func TestSerializeConformanceDags(t *testing.T) {
	args := flag.Args()
	if len(args) != 2 {
		t.Skip(
			"compare.py runs this with the paths of test_dags.yaml and of the output after -args",
		)
	}
	raw, err := os.ReadFile(args[0])
	require.NoError(t, err)
	var doc yaml.Node
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	var file struct {
		Dags []conformanceDag `yaml:"dags"`
	}
	require.NoError(t, doc.Decode(&file))

	bundle := airflow.Bundle()
	serialized := make(map[string]any, len(file.Dags))
	for _, dagCase := range file.Dags {
		dag := buildConformanceDag(t, dagCase)
		bundle.Register(dag)
		// compare.py does not compare fileloc, which names the file that declares a Dag, so any path
		// works here. Airflow still needs a fileloc to load the Dag.
		serialized[dagCase.DagID] = airflow.SerializeDag(dag, "/bundles/app/etl", "etl")
	}
	out, err := json.MarshalIndent(serialized, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(args[1], append(out, '\n'), 0o644))
}

// conformanceDag is one Dag of test_dags.yaml. A spec maps the snake_case keys of the serialization
// schema to values. The tag !datetime marks a value that is a moment, and !timedelta marks a number
// of seconds.
type conformanceDag struct {
	DagID      string             `yaml:"dag_id"`
	Spec       yaml.Node          `yaml:"spec"`
	Groups     []conformanceGroup `yaml:"groups"`
	Tasks      []conformanceTask  `yaml:"tasks"`
	OrderEdges [][]string         `yaml:"order_edges"`
}

// conformanceGroup is a task group of a Dag. test_dags.yaml writes a group with no options as its
// id, and a group with options as a mapping.
type conformanceGroup struct {
	ID   string    `yaml:"id"`
	Spec yaml.Node `yaml:"spec"`
}

func (g *conformanceGroup) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		g.ID = node.Value
		return nil
	}
	type plain conformanceGroup
	return node.Decode((*plain)(g))
}

type conformanceTask struct {
	TaskID        string             `yaml:"task_id"`
	Group         string             `yaml:"group"`
	Upstream      []string           `yaml:"upstream"`
	Literals      []any              `yaml:"literals"`
	Branch        *conformanceBranch `yaml:"branch"`
	TriggerDagRun yaml.Node          `yaml:"trigger_dag_run"`
	Spec          yaml.Node          `yaml:"spec"`
}

// conformanceBranch makes a task a condition, when it has Then, or a switch, when it has Cases.
type conformanceBranch struct {
	Then  string   `yaml:"then"`
	Else  string   `yaml:"else"`
	Cases []string `yaml:"cases"`
}

// conformanceDecider is a condition or a switch that buildConformanceDag has added, and the ids of
// the tasks it chooses between. They are named after the tasks exist, because a Dag file gives a
// decider before the tasks it chooses.
type conformanceDecider struct {
	ifRef     *airflow.IfRef
	switchRef *airflow.SwitchRef
	branch    conformanceBranch
}

func buildConformanceDag(t *testing.T, dagCase conformanceDag) *airflow.DagRef {
	t.Helper()
	var spec airflow.DagSpec
	setConformanceSpec(t, &spec, dagCase.Spec, dagCase.DagID)
	dag := airflow.Dag(dagCase.DagID, spec)

	// test_dags.yaml gives the full group_id, so the parent of a group is what comes before the last
	// dot. A parent comes before the groups it holds.
	groups := make(map[string]*airflow.TaskGroupRef)
	for _, group := range dagCase.Groups {
		var groupSpec airflow.TaskGroupSpec
		setConformanceSpec(t, &groupSpec, group.Spec, dagCase.DagID+"."+group.ID)
		cut := strings.LastIndex(group.ID, ".")
		if cut < 0 {
			groups[group.ID] = dag.TaskGroup(group.ID, groupSpec)
			continue
		}
		parent, ok := groups[group.ID[:cut]]
		require.True(t, ok, "%s: group %q comes before its parent", dagCase.DagID, group.ID)
		groups[group.ID] = parent.TaskGroup(group.ID[cut+1:], groupSpec)
	}

	// A task is named by its group and its own id in test_dags.yaml, even when its group leaves the
	// group id off the id that the task gets.
	tasks := make(map[string]*airflow.TaskRef)
	var deciders []conformanceDecider
	for _, task := range dagCase.Tasks {
		var taskSpec airflow.TaskSpec
		label := dagCase.DagID + "." + task.TaskID
		setConformanceSpec(t, &taskSpec, task.Spec, label)
		taskSpec.TaskID = task.TaskID
		// Each upstream task passes its result to a parameter of the task, which gives the task the edge
		// that test_dags.yaml asks for. A literal fills a parameter after those.
		var inputs []airflow.Input
		for _, upstreamID := range task.Upstream {
			upstream, ok := tasks[upstreamID]
			require.True(t, ok, "%s: upstream %q comes after the task", label, upstreamID)
			inputs = append(inputs, upstream)
		}
		for _, value := range task.Literals {
			inputs = append(inputs, airflow.Literal(value))
		}
		opts := []airflow.TaskOption{taskSpec, airflow.Inputs(inputs...)}

		fullID := task.TaskID
		var adder conformanceAdder = dag
		if task.Group != "" {
			group, ok := groups[task.Group]
			require.True(t, ok, "%s: no group %q", label, task.Group)
			adder = group
			fullID = task.Group + "." + task.TaskID
		}
		switch {
		case task.TriggerDagRun.Kind != 0:
			// A task that triggers a Dag takes no Inputs.
			trigger := conformanceTriggerDagRun(t, task.TriggerDagRun, label)
			tasks[fullID] = adder.Task(airflow.TriggerDagRun(trigger), taskSpec)
		case task.Branch != nil && len(task.Branch.Cases) > 0:
			fn := conformanceTaskFunction(len(inputs), reflect.TypeFor[*airflow.TaskRef]())
			deciders = append(deciders, conformanceDecider{
				switchRef: adder.Switch(fn, opts...), branch: *task.Branch,
			})
		case task.Branch != nil:
			fn := conformanceTaskFunction(len(inputs), reflect.TypeFor[bool]())
			deciders = append(deciders, conformanceDecider{
				ifRef: adder.If(fn, opts...), branch: *task.Branch,
			})
		default:
			fn := conformanceTaskFunction(len(inputs), reflect.TypeFor[any]())
			tasks[fullID] = adder.Task(fn, opts...)
		}
	}

	node := func(id string) airflow.Node {
		if group, ok := groups[id]; ok {
			return group
		}
		task, ok := tasks[id]
		require.True(t, ok, "%s: no task or group %q", dagCase.DagID, id)
		return task
	}
	for _, decider := range deciders {
		task := func(id string) *airflow.TaskRef {
			chosen, ok := tasks[id]
			require.True(
				t,
				ok,
				"%s: the decider chooses %q, which is not a task",
				dagCase.DagID,
				id,
			)
			return chosen
		}
		if decider.ifRef != nil {
			decider.ifRef.Then(task(decider.branch.Then))
			if decider.branch.Else != "" {
				decider.ifRef.Else(task(decider.branch.Else))
			}
			continue
		}
		for _, id := range decider.branch.Cases {
			decider.switchRef.Case(task(id))
		}
	}
	for _, edge := range dagCase.OrderEdges {
		downstream := node(edge[1])
		if len(edge) == 3 {
			downstream = airflow.Label(downstream, edge[2])
		}
		node(edge[0]).Before(downstream)
	}
	return dag
}

// conformanceAdder is what a Dag and a task group both offer for adding a task.
type conformanceAdder interface {
	Task(fn any, opts ...airflow.TaskOption) *airflow.TaskRef
	If(fn any, opts ...airflow.TaskOption) *airflow.IfRef
	Switch(fn any, opts ...airflow.TaskOption) *airflow.SwitchRef
}

// conformanceTriggerDagRun reads the template fields of a TriggerDagRunOperator from a task of
// test_dags.yaml.
func conformanceTriggerDagRun(
	t *testing.T, node yaml.Node, label string,
) airflow.TriggerDagRunSpec {
	t.Helper()
	require.Equal(t, yaml.MappingNode, node.Kind, "%s: trigger_dag_run is not a mapping", label)
	var spec airflow.TriggerDagRunSpec
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i].Value, node.Content[i+1]
		var target any
		switch key {
		case "trigger_dag_id":
			target = &spec.DagID
		case "trigger_run_id":
			target = &spec.RunID
		case "conf":
			target = &spec.Conf
		case "logical_date":
			target = &spec.LogicalDate
		case "wait_for_completion":
			target = &spec.WaitForCompletion
		case "skip_when_already_exists":
			target = &spec.SkipWhenAlreadyExists
		default:
			require.Failf(
				t,
				"unknown key",
				"%s: %q is not a template field of the trigger",
				label,
				key,
			)
		}
		if moment, ok := target.(*time.Time); ok {
			// The value is spelled as Python's str(datetime), such as "2026-09-30 00:00:00+00:00".
			parsed, err := time.Parse("2006-01-02 15:04:05Z07:00", value.Value)
			require.NoError(t, err, "%s: %s", label, key)
			*moment = parsed
			continue
		}
		require.NoError(t, value.Decode(target), "%s: %s", label, key)
	}
	return spec
}

// conformanceTaskFunction returns a task function that takes the given number of arguments after
// the Context and returns a result of the given type with an error. So any task can pass its
// result to any other, and a literal of any kind fills any parameter.
func conformanceTaskFunction(params int, result reflect.Type) any {
	in := []reflect.Type{reflect.TypeFor[airflow.Context]()}
	for range params {
		in = append(in, reflect.TypeFor[any]())
	}
	out := []reflect.Type{result, reflect.TypeFor[error]()}
	fnType := reflect.FuncOf(in, out, false)
	return reflect.MakeFunc(fnType, func([]reflect.Value) []reflect.Value {
		return []reflect.Value{reflect.Zero(out[0]), reflect.Zero(out[1])}
	}).Interface()
}

// setConformanceSpec sets fields of the struct that target points to, from a spec in
// test_dags.yaml. It finds the field for each key of the spec in the generated field table.
func setConformanceSpec(t *testing.T, target any, spec yaml.Node, label string) {
	t.Helper()
	if spec.Kind == 0 {
		return
	}
	require.Equal(t, yaml.MappingNode, spec.Kind, "%s: spec is not a mapping", label)
	byKey := airflow.SpecFieldNames(reflect.ValueOf(target).Elem().Interface())
	value := reflect.ValueOf(target).Elem()
	for i := 0; i < len(spec.Content); i += 2 {
		key, node := spec.Content[i].Value, spec.Content[i+1]
		name, ok := byKey[key]
		require.True(t, ok, "%s: %q is not a key the Go SDK sets", label, key)
		setConformanceField(t, value.FieldByName(name), node, fmt.Sprintf("%s: %s", label, key))
	}
}

func setConformanceField(t *testing.T, field reflect.Value, node *yaml.Node, label string) {
	t.Helper()
	if field.Kind() == reflect.Pointer {
		field.Set(reflect.New(field.Type().Elem()))
		field = field.Elem()
	}
	switch {
	case node.Tag == "!datetime":
		moment, err := time.Parse(time.RFC3339, node.Value)
		require.NoError(t, err, label)
		field.Set(reflect.ValueOf(moment))
	case node.Tag == "!timedelta":
		seconds, err := strconv.ParseFloat(node.Value, 64)
		require.NoError(t, err, label)
		field.Set(reflect.ValueOf(time.Duration(seconds * float64(time.Second))))
	default:
		// yaml decodes into the Go type of the field, such as a TriggerRule or a []string.
		require.NoError(t, node.Decode(field.Addr().Interface()), label)
	}
}
