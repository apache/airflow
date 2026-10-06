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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

// authoringShape rewrites the definitions the airflow package generates from
// into the shape a Dag author writes, rather than the shape Airflow serializes.
// Each definition drops the properties in exclude, rewrites the properties in
// override, and gains the properties in inject.
type authoringShape struct {
	// doc becomes the description of the definition, which go-jsonschema writes as
	// the doc comment of the generated type.
	doc string
	// exclude names each property that must not reach the generated struct, mapped
	// to why. A property absent from the list generates, so that a property added
	// on the Python side surfaces in review rather than vanishing; the reason is
	// what a reviewer reads when deciding whether a new one belongs here.
	exclude map[string]string
	// override rewrites a property that generates as the wrong Go type. The
	// serialization schema types a moment in time and a duration as a number of
	// seconds and an integral count as a JSON number, none of which is the type an
	// author sets.
	override map[string]propertyOverride
	// inject adds a property the schema has no counterpart for, so that every field
	// of the generated struct comes from generation and the struct stays one
	// declaration.
	inject map[string]map[string]any
}

// propertyOverride is the part of a property genspec rewrites. goType and imports
// become go-jsonschema's goJSONSchema extension, which it reads before a $ref, so
// an override applies to a property written as a reference too.
type propertyOverride struct {
	goType  string
	imports []string
	// doc replaces the description, and so the doc comment of the generated field,
	// where the serialized property has nothing to say about how an author sets it.
	doc string
	// pointer forces a pointer where setPointers cannot tell that the Go zero value is
	// a setting an author could mean. Whether 0 is a legal value for a count is not
	// something the schema says, so it is stated here for the counts where it is.
	pointer bool
}

var authoringShapes = map[string]authoringShape{
	"dag":        dagShape,
	"operator":   taskShape,
	"task_group": taskGroupShape,
}

var dagShape = authoringShape{
	doc: "DagSpec holds the attributes of a Dag other than its dag_id. Dag takes one.",
	exclude: map[string]string{
		"dag_id":                    "a positional parameter of airflow.Dag, not a spec field",
		"fileloc":                   "the path of the Dag file, which the bundle fills in",
		"relative_fileloc":          "the path of the Dag file, which the bundle fills in",
		"_processor_dags_folder":    "the Dag processor's own folder, filled in at parse time",
		"bundle_name":               "the name of the bundle that carries the Dag, not the Dag's",
		"tasks":                     "the tasks dag.Task registers",
		"task_group":                "the groups dag.TaskGroup registers",
		"edge_info":                 "the labels airflow.Label carries into an edge verb",
		"dag_dependencies":          "derived from the edges and the assets a Dag declares",
		"timezone":                  "always UTC for a Go Dag, even when StartDate has another location",
		"timetable":                 "the serialized form of Schedule, which is injected instead",
		"allowed_run_types":         "no Go authoring type yet: a list of DagRunType values",
		"_concurrency":              "the pre-2.2 spelling of MaxActiveTasks",
		"has_on_success_callback":   "derived from whether a callback is registered",
		"has_on_failure_callback":   "derived from whether a callback is registered",
		"params":                    "no Go authoring type yet: a param carries a schema of its own",
		"default_args":              "no Go authoring type yet: the values are arbitrary and untyped",
		"access_control":            "no Go authoring type yet, and it is deprecated in Airflow 3",
		"owner_links":               "no Go authoring type yet: an object of arbitrary link targets",
		"deadline":                  "no Go authoring type yet: a serialized deadline reference",
		"rerun_with_latest_version": "no Go authoring type yet: the tri-state a null allows",
	},
	override: map[string]propertyOverride{
		"start_date": {
			goType:  "time.Time",
			imports: []string{"time"},
			doc:     "StartDate is the start_date of the Dag. The timezone of the Dag is UTC even when StartDate has another location, so Airflow reads Schedule in UTC.",
		},
		"end_date":         {goType: "time.Time", imports: []string{"time"}},
		"dagrun_timeout":   {goType: "time.Duration", imports: []string{"time"}},
		"max_active_tasks": {goType: "int"},
		"max_active_runs":  {goType: "int"},
		// 0 means "never pause this Dag", and the default comes from
		// [core] max_consecutive_failed_dag_runs_per_dag, which a deployment can set
		// above 0, so an author has to be able to say 0 and be heard.
		"max_consecutive_failed_dag_runs": {goType: "int", pointer: true},
		"tags":                            {goType: "[]string"},
	},
	inject: map[string]map[string]any{
		// The schema carries the serialized timetable this resolves to, never the
		// expression an author writes.
		"schedule": {
			"type":        "string",
			"description": "Schedule is when the Dag runs: a cron expression such as \"0 3 * * *\", or one of the presets \"@hourly\", \"@daily\", \"@weekly\", \"@monthly\", \"@quarterly\", \"@yearly\", \"@once\" and \"@continuous\". A cron expression has five fields. A sixth field adds the seconds, and a seventh field after it adds the year. Airflow reads a cron expression in UTC. A Dag with an empty Schedule runs only when something triggers it.",
		},
		// The schema has no Dag-level queue. A Python Dag gives all of its tasks a
		// queue through default_args, which the Go SDK does not have. The Go tasks of a
		// Dag all run on a coordinator for Go, so one queue on the Dag can route all of
		// them there.
		"queue": {
			"type":        "string",
			"description": "Queue is the queue that each task of the Dag runs on, unless the TaskSpec of the task sets a Queue. The queue_to_coordinator option in the [sdk] section of the Airflow configuration maps the queue to the coordinator that runs Go code. A task from TriggerDagRun runs on a Python worker, so it does not take this queue.",
		},
	},
}

var taskShape = authoringShape{
	doc: "TaskSpec holds the attributes of a task. DagRef.Task, DagRef.If, DagRef.Switch and " +
		"the methods of the same names on TaskGroupRef take at most one per task.",
	exclude: map[string]string{
		"task_type":                     "the operator class name, which the SDK fills in",
		"_task_module":                  "the operator's Python module, which the SDK fills in",
		"_operator_extra_links":         "links a Python operator class declares, which a Go task has none of",
		"ui_color":                      "the grid colour, which the SDK fills in",
		"ui_fgcolor":                    "the grid colour, which the SDK fills in",
		"template_fields":               "the templated attributes of a Python operator class",
		"template_ext":                  "the templated attributes of a Python operator class",
		"template_fields_renderers":     "the templated attributes of a Python operator class",
		"downstream_task_ids":           "the edges Before, After and Inputs declare",
		"partial_kwargs":                "the serialized form of a mapped task's partial arguments",
		"_logger_name":                  "the logger the task runner names",
		"_needs_expansion":              "derived from whether the task is mapped",
		"_is_mapped":                    "derived from whether the task is mapped",
		"_is_sensor":                    "derived from the task's own kind",
		"_disallow_kwargs_override":     "a mapped-task serialization detail",
		"_expand_input_attr":            "a mapped-task serialization detail",
		"_arg_bindings":                 "the bindings airflow.Inputs records",
		"has_on_execute_callback":       "derived from whether a callback is registered",
		"has_on_failure_callback":       "derived from whether a callback is registered",
		"has_on_skipped_callback":       "derived from whether a callback is registered",
		"has_on_success_callback":       "derived from whether a callback is registered",
		"has_on_retry_callback":         "derived from whether a callback is registered",
		"start_from_trigger":            "deferral is Python's, per decision 11 of ADR 8",
		"start_trigger_args":            "deferral is Python's, per decision 11 of ADR 8",
		"multiple_outputs":              "derived from the Go function's return type",
		"params":                        "no Go authoring type yet: a param carries a schema of its own",
		"executor_config":               "no Go authoring type yet: the keys are executor-specific",
		"inlets":                        "no Go authoring type yet: an asset needs its own spec",
		"outlets":                       "no Go authoring type yet: an asset needs its own spec",
		"render_template_as_native_obj": "set on the Dag, where the schema types it without a null",
		// 4 and 5: an attribute an author can set but the SDK cannot yet honour, and one
		// that only Python has, are worse than a missing field: a field is easy to add
		// later and hard to take away.
		"is_setup":               "setup/teardown needs trigger-rule handling the SDK does not model yet",
		"is_teardown":            "setup/teardown needs trigger-rule handling the SDK does not model yet",
		"on_failure_fail_dagrun": "only meaningful on a teardown task",
		"allow_nested_operators": "Python-only: it warns when an operator executes inside another",
		"doc":                    "a legacy rendering of the task's docs; only doc_md is exposed",
		"doc_json":               "a legacy rendering of the task's docs; only doc_md is exposed",
		"doc_rst":                "a legacy rendering of the task's docs; only doc_md is exposed",
		"doc_yaml":               "a legacy rendering of the task's docs; only doc_md is exposed",
	},
	override: map[string]propertyOverride{
		"start_date":                {goType: "time.Time", imports: []string{"time"}},
		"end_date":                  {goType: "time.Time", imports: []string{"time"}},
		"execution_timeout":         {goType: "time.Duration", imports: []string{"time"}},
		"retry_delay":               {goType: "time.Duration", imports: []string{"time"}},
		"max_retry_delay":           {goType: "time.Duration", imports: []string{"time"}},
		"retries":                   {goType: "int"},
		"pool_slots":                {goType: "int"},
		"priority_weight":           {goType: "int"},
		"max_active_tis_per_dag":    {goType: "int"},
		"max_active_tis_per_dagrun": {goType: "int"},
		// TriggerRule, WeightRule and their constants are hand-written in the airflow
		// package, because the schema types trigger_rule and weight_rule as plain
		// strings and does not list their values.
		"trigger_rule": {goType: "TriggerRule"},
		"weight_rule":  {goType: "WeightRule"},
		// A multiplier, not a switch: 0 keeps the delay constant, 2.0 doubles it each
		// retry. The schema's number is right, and the float is what carries the 2.0.
		"retry_exponential_backoff": {goType: "float64"},
		"task_id": {
			goType: "string",
			doc:    "TaskID is the task_id of the task. When TaskID is empty, the task_id is the name of the Go function that the task runs. A task from TriggerDagRun runs no Go function, so it needs a TaskID. A task added through a task group takes the group_id as a prefix of its task_id, unless the TaskGroupSpec of the group sets PrefixGroupID to false.",
		},
	},
}

var taskGroupShape = authoringShape{
	doc: "TaskGroupSpec holds the attributes of a task group other than its group_id. " +
		"DagRef.TaskGroup and TaskGroupRef.TaskGroup take at most one per group.",
	exclude: map[string]string{
		"_group_id":            "a positional parameter of DagRef.TaskGroup and TaskGroupRef.TaskGroup",
		"children":             "the tasks and groups added through the group",
		"is_mapped":            "derived from whether the group is mapped, which the SDK does not model yet",
		"upstream_group_ids":   "the edges Before and After declare",
		"downstream_group_ids": "the edges Before and After declare",
		"upstream_task_ids":    "the edges Before and After declare",
		"downstream_task_ids":  "the edges Before and After declare",
	},
	override: map[string]propertyOverride{
		// The schema allows null for doc_md, as anyOf [string, null], which rejectCombinators
		// refuses. An empty DocMD means a group without docs, as null does.
		"doc_md": {goType: "string"},
		"prefix_group_id": {
			goType: "bool",
			doc:    "PrefixGroupID says whether the group_id prefixes the IDs of the tasks and groups added through the group, as in \"transform.cleanRows\". When PrefixGroupID is nil, the group_id prefixes them.",
		},
	},
}

// shapeForAuthoring rewrites doc into the schema the spec structs generate from:
// each definition in shapes takes its authoring shape, and everything the airflow
// package does not generate is dropped.
func shapeForAuthoring(doc map[string]any, shapes map[string]authoringShape) error {
	if err := applyAuthoringShapes(doc, shapes); err != nil {
		return err
	}
	return keepOnlySpecGeneratingSchema(doc, shapes)
}

// applyAuthoringShapes rewrites each definition in shapes. It reports a list entry
// that no longer matches the schema — an excluded or overridden property that has
// gone, an injected property the schema has grown — because each of those means the
// list here decides nothing and the generated struct would silently change shape.
func applyAuthoringShapes(doc map[string]any, shapes map[string]authoringShape) error {
	definitions, ok := doc["definitions"].(map[string]any)
	if !ok {
		return errNoDefinitions
	}
	for _, name := range sortedKeys(shapes) {
		definition, ok := definitions[name].(map[string]any)
		if !ok {
			return fmt.Errorf(
				"definitions/%s is missing, and the airflow package generates from it",
				name,
			)
		}
		properties, ok := definition["properties"].(map[string]any)
		if !ok {
			return fmt.Errorf("definitions/%s has no properties to generate a struct from", name)
		}
		shape := shapes[name]
		if err := excludeProperties(name, definition, properties, shape.exclude); err != nil {
			return err
		}
		if err := overrideProperties(name, properties, shape.override); err != nil {
			return err
		}
		if err := injectProperties(name, properties, shape.inject); err != nil {
			return err
		}
		if err := rejectCombinators(name, definition); err != nil {
			return err
		}
		definition["description"] = shape.doc
		// An authoring struct holds the fields it declares and no others, and every
		// field is optional: airflow.Dag takes the dag_id positionally and a task_id
		// defaults to the name of the Go function.
		definition["additionalProperties"] = false
		delete(definition, "required")
		setPointers(properties)
	}
	return nil
}

func excludeProperties(
	name string, definition, properties map[string]any, exclude map[string]string,
) error {
	for _, property := range sortedKeys(exclude) {
		if _, ok := properties[property]; !ok {
			return fmt.Errorf(
				"definitions/%s/properties/%s is excluded as %q, but the schema no longer has it",
				name, property, exclude[property],
			)
		}
		delete(properties, property)
	}
	return nil
}

// overrideProperties replaces the Go type of a property with goJSONSchema, the
// extension go-jsonschema reads before it reads a type or a $ref.
func overrideProperties(
	name string,
	properties map[string]any,
	override map[string]propertyOverride,
) error {
	for _, property := range sortedKeys(override) {
		node, ok := properties[property].(map[string]any)
		if !ok {
			return fmt.Errorf(
				"definitions/%s/properties/%s is overridden to %s, but the schema no longer has it",
				name, property, override[property].goType,
			)
		}
		extension := map[string]any{"type": override[property].goType}
		if imports := override[property].imports; len(imports) > 0 {
			extension["imports"] = anySlice(imports)
		}
		if doc := override[property].doc; doc != "" {
			node["description"] = doc
		}
		if override[property].pointer {
			extension["pointer"] = true
		}
		node["goJSONSchema"] = extension
		// The override replaces whatever the reference resolves to, and dropping it
		// keeps the pruned schema free of references to definitions that are gone.
		delete(node, "$ref")
	}
	return nil
}

// rejectCombinators reports an anyOf, oneOf or allOf left in a definition the specs
// generate from. genspec has no rule for one, and go-jsonschema does not fail on it
// either: it degrades the property to interface{} or gives it a typedef of its own,
// so the field's meaning is lost in a diff that still looks like a field. The
// nullable pair the schema writes as anyOf [boolean, null] is the shape to expect,
// and resolveNullableTypes only handles the type-list spelling of it.
//
// A property whose type an override replaces is exempt: the override stands for
// whatever the schema says the property is.
func rejectCombinators(name string, definition map[string]any) error {
	return walkFrom("/definitions/"+name, definition, func(path string, node map[string]any) error {
		if extension, ok := node["goJSONSchema"].(map[string]any); ok {
			if _, overridden := extension["type"]; overridden {
				return nil
			}
		}
		for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
			if _, ok := node[keyword]; ok {
				return fmt.Errorf(
					"%s has %s, which genspec has no rule for; exclude the property or give it "+
						"a type override",
					path, keyword,
				)
			}
		}
		return nil
	})
}

// setPointers decides, for every property left, whether its field is a pointer. A
// field needs one wherever the Go zero value is something an author could mean and
// the schema does not assert that it is already the default: a concrete field would
// make that setting indistinguishable from an unset one, and omitempty would drop
// it on the way out.
//
// Two shapes qualify. A scalar whose schema default is not the Go zero value is the
// rule pkg/execution/genmodels applies to the supervisor schema. A boolean with no
// schema default at all is the second: false is always one of its two legal values,
// and the absence of a default does not mean the default is false — catchup and
// is_paused_upon_creation take theirs from [scheduler] catchup_by_default and
// [core] dags_are_paused_at_creation, both of which can be true. A count keeps its
// concrete type where 0 is not a value it can take, as for max_active_runs, so the
// zero value can only mean unset; the counts where 0 does mean something say so with
// a pointer override, which the schema gives no way to work out.
func setPointers(properties map[string]any) {
	for _, name := range sortedKeys(properties) {
		node, ok := properties[name].(map[string]any)
		if !ok {
			continue
		}
		extension, ok := node["goJSONSchema"].(map[string]any)
		if !ok {
			extension = map[string]any{}
			node["goJSONSchema"] = extension
		}
		if _, forced := extension["pointer"]; forced {
			continue
		}
		extension["pointer"] = needsPointer(node)
	}
}

func needsPointer(node map[string]any) bool {
	if node["type"] == "boolean" {
		_, declared := node["default"]
		return !declared || hasNonZeroDefault(node)
	}
	return hasNonZeroDefault(node)
}

// hasNonZeroDefault reports whether the property carries a scalar default that a
// Go zero value does not satisfy. A string default is not one: an author leaves a
// string empty to mean unset, and no default the schema declares is the empty
// string.
func hasNonZeroDefault(node map[string]any) bool {
	value, ok := node["default"]
	if !ok {
		return false
	}
	switch value := value.(type) {
	case bool:
		return value
	// A number arrives as a json.Number from readSchema, which decodes with
	// UseNumber, and as a float64 from any plain decoder.
	case json.Number:
		number, err := value.Float64()
		return err == nil && number != 0
	case float64:
		return value != 0
	default:
		return false
	}
}

func injectProperties(
	name string,
	properties map[string]any,
	inject map[string]map[string]any,
) error {
	for _, property := range sortedKeys(inject) {
		if _, ok := properties[property]; ok {
			return fmt.Errorf(
				"definitions/%s/properties/%s is injected, but the schema now declares it too",
				name, property,
			)
		}
		properties[property] = cloneProperty(inject[property])
	}
	return nil
}

// keepOnlySpecGeneratingSchema leaves the definitions in shapes and drops
// everything else, so that the generated file holds the spec structs alone. The
// root schema describes a serialized Dag file rather than a spec, and every other
// definition is reachable only from a property the shapes exclude.
//
// It reports a definition that still references a dropped one, which would
// otherwise generate against a $ref that resolves to nothing.
func keepOnlySpecGeneratingSchema(doc map[string]any, shapes map[string]authoringShape) error {
	definitions, ok := doc["definitions"].(map[string]any)
	if !ok {
		return errNoDefinitions
	}
	for _, name := range sortedKeys(definitions) {
		if _, kept := shapes[name]; !kept {
			delete(definitions, name)
		}
	}
	for _, key := range sortedKeys(doc) {
		switch key {
		case "$schema", "$id", "definitions":
		default:
			delete(doc, key)
		}
	}
	return walkObjects(doc, func(path string, node map[string]any) error {
		ref, ok := node["$ref"].(string)
		if !ok {
			return nil
		}
		target, ok := definitionName(ref)
		if !ok {
			return fmt.Errorf("%s references %s, which is not a definition", path, ref)
		}
		if _, ok := definitions[target]; !ok {
			return fmt.Errorf(
				"%s references definitions/%s, which the spec structs do not generate; "+
					"exclude the property or give it a type override",
				path, target,
			)
		}
		return nil
	})
}

func definitionName(ref string) (string, bool) {
	const prefix = "#/definitions/"
	if len(ref) <= len(prefix) || ref[:len(prefix)] != prefix {
		return "", false
	}
	return ref[len(prefix):], true
}

func cloneProperty(property map[string]any) map[string]any {
	clone := make(map[string]any, len(property))
	for key, value := range property {
		clone[key] = value
	}
	return clone
}

func anySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

var errNoDefinitions = errors.New(`the schema has no "definitions" object`)
