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

	bundle := Bundle()
	dags := make([]*DagRef, len(file.Dags))
	for i, dagCase := range file.Dags {
		dags[i] = buildConformanceDag(t, dagCase)
		bundle.Register(dags[i])
	}
	serialized := make(map[string]any, len(dags))
	for _, dag := range dags {
		// compare.py does not compare fileloc, which names the file that declares a Dag, so any path
		// works here. Airflow still needs a fileloc to load the Dag.
		serialized[dag.dagID] = dag.serialize("/bundles/app/etl", "etl")
	}
	out, err := json.MarshalIndent(serialized, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(args[1], append(out, '\n'), 0o644))
}

// conformanceDag is one Dag of test_dags.yaml. A spec maps the snake_case keys of the serialization
// schema to values. The tag !datetime marks a value that is a moment, and !timedelta marks a number
// of seconds.
type conformanceDag struct {
	DagID      string            `yaml:"dag_id"`
	Spec       yaml.Node         `yaml:"spec"`
	Groups     []string          `yaml:"groups"`
	Tasks      []conformanceTask `yaml:"tasks"`
	OrderEdges [][2]string       `yaml:"order_edges"`
}

type conformanceTask struct {
	TaskID   string    `yaml:"task_id"`
	Group    string    `yaml:"group"`
	Upstream []string  `yaml:"upstream"`
	Spec     yaml.Node `yaml:"spec"`
}

func buildConformanceDag(t *testing.T, dagCase conformanceDag) *DagRef {
	t.Helper()
	var spec DagSpec
	setConformanceSpec(t, &spec, dagSpecRules, dagCase.Spec, dagCase.DagID)
	dag := Dag(dagCase.DagID, spec)

	// test_dags.yaml gives the full group_id, so the parent of a group is what comes before the last
	// dot. A parent comes before the groups it holds.
	groups := make(map[string]*TaskGroupRef)
	for _, groupID := range dagCase.Groups {
		cut := strings.LastIndex(groupID, ".")
		if cut < 0 {
			groups[groupID] = dag.TaskGroup(groupID)
			continue
		}
		parent, ok := groups[groupID[:cut]]
		require.True(t, ok, "%s: group %q comes before its parent", dagCase.DagID, groupID)
		groups[groupID] = parent.TaskGroup(groupID[cut+1:])
	}

	tasks := make(map[string]*TaskRef)
	for _, task := range dagCase.Tasks {
		var taskSpec TaskSpec
		label := dagCase.DagID + "." + task.TaskID
		setConformanceSpec(t, &taskSpec, taskSpecRules, task.Spec, label)
		taskSpec.TaskID = task.TaskID
		// Each upstream task passes its result to a parameter of the task, which gives the task the edge
		// that test_dags.yaml asks for.
		upstreams := make([]Input, len(task.Upstream))
		for i, upstreamID := range task.Upstream {
			upstream, ok := tasks[upstreamID]
			require.True(t, ok, "%s: upstream %q comes after the task", label, upstreamID)
			upstreams[i] = upstream
		}
		fn := conformanceTaskFunction(len(upstreams))
		opts := []TaskOption{taskSpec, Inputs(upstreams...)}
		var added *TaskRef
		if task.Group == "" {
			added = dag.Task(fn, opts...)
		} else {
			group, ok := groups[task.Group]
			require.True(t, ok, "%s: no group %q", label, task.Group)
			added = group.Task(fn, opts...)
		}
		tasks[added.taskID] = added
	}

	node := func(id string) Node {
		if group, ok := groups[id]; ok {
			return group
		}
		task, ok := tasks[id]
		require.True(t, ok, "%s: no task or group %q", dagCase.DagID, id)
		return task
	}
	for _, edge := range dagCase.OrderEdges {
		node(edge[0]).Before(node(edge[1]))
	}
	return dag
}

// conformanceTaskFunction returns a task function that takes the given number of results after the
// Context and returns a result of its own. So any task can pass its result to any other.
func conformanceTaskFunction(params int) any {
	in := []reflect.Type{reflect.TypeFor[Context]()}
	for range params {
		in = append(in, reflect.TypeFor[any]())
	}
	out := []reflect.Type{reflect.TypeFor[any](), reflect.TypeFor[error]()}
	fnType := reflect.FuncOf(in, out, false)
	return reflect.MakeFunc(fnType, func([]reflect.Value) []reflect.Value {
		return []reflect.Value{reflect.Zero(out[0]), reflect.Zero(out[1])}
	}).Interface()
}

// setConformanceSpec sets fields of the struct that target points to, from a spec in
// test_dags.yaml. It finds the field for each key of the spec in rules.
func setConformanceSpec(
	t *testing.T, target any, rules specRules, spec yaml.Node, label string,
) {
	t.Helper()
	if spec.Kind == 0 {
		return
	}
	require.Equal(t, yaml.MappingNode, spec.Kind, "%s: spec is not a mapping", label)
	byKey := map[string]string{}
	for name, field := range rules.fields {
		byKey[field.key] = name
	}
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
