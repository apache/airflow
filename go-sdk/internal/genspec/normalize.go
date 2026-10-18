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
	"fmt"
	"slices"
)

var specTitles = map[string]string{
	"dag":        "DagSpec",
	"operator":   "TaskSpec",
	"task_group": "TaskGroupSpec",
}

// normalize rewrites doc in place so that go-jsonschema can read it, and injects
// the title of each definition in titles. It reports the first construct it cannot
// rewrite, naming the path to it, so that a schema change on the Python side that
// needs a new rule here fails the generate step with somewhere to look.
func normalize(doc map[string]any, titles map[string]string) error {
	if err := dropDependentRequired(doc); err != nil {
		return err
	}
	if err := resolveNullableTypes(doc); err != nil {
		return err
	}
	return injectTitles(doc, titles)
}

// dropDependentRequired removes every dependencies entry written in draft-07's
// array form, the one later drafts renamed dependentRequired. It is the single
// construct in the schema that go-jsonschema v0.23.1 cannot parse at all: it
// fails the whole file with "cannot unmarshal array into Go value of type
// schemas.ObjectAsType". The entry states that one property requires others
// alongside it, which constrains an instance and not the generated type. An entry
// whose value is a schema is left alone: go-jsonschema reads that form.
func dropDependentRequired(doc map[string]any) error {
	return walkObjects(doc, func(_ string, node map[string]any) error {
		deps, ok := node["dependencies"].(map[string]any)
		if !ok {
			return nil
		}
		for _, name := range sortedKeys(deps) {
			if _, isList := deps[name].([]any); isList {
				delete(deps, name)
			}
		}
		if len(deps) == 0 {
			delete(node, "dependencies")
		}
		return nil
	})
}

// resolveNullableTypes rewrites every nullable type pair such as
// ["boolean", "null"] to the one type it allows, wherever it appears. Whether the
// field is a pointer is not decided here: setPointers writes that on every
// property, so the type pair only has to leave a type go-jsonschema names well.
//
// go-jsonschema reads a type list, but it gives the field a typedef of its own
// (type DagRerunWithLatestVersion *bool, type DagTagsElem *string) instead of the
// plain *bool or *string a single nullable type produces, and that typedef would
// be an exported name in the airflow package standing for nothing an author names.
func resolveNullableTypes(doc map[string]any) error {
	return walkObjects(doc, resolveNodeType)
}

// resolveNodeType rewrites the type list on node itself, so that a pair nested in
// items or additionalProperties resolves by the same rule as one on a property.
func resolveNodeType(path string, node map[string]any) error {
	types, ok := node["type"].([]any)
	if !ok {
		return nil
	}
	kept := slices.DeleteFunc(slices.Clone(types), func(t any) bool {
		return t == "null"
	})
	switch len(kept) {
	case 1:
		node["type"] = kept[0]
		return nil
	case 0:
		return fmt.Errorf("%s has type %v, which allows nothing to generate from", path, types)
	default:
		return fmt.Errorf(
			"%s has type %v: go-jsonschema degrades a type union to interface{}, "+
				"which is not a field an author can set",
			path, types,
		)
	}
}

// injectTitles gives each definition in titles the title that
// --struct-name-from-title reads. It reports a definition that has gone missing
// or already carries a title of its own, either of which means titles is stale.
//
// No definition carries a title, so the flag has nothing to read and go-jsonschema
// falls back to capitalizing the definition keys dag, operator and task_group. Dag
// is already the name of the constructor, Operator is not the SDK's vocabulary, and
// TaskGroup is the name of the method that adds a group, which is why the titles are
// injected rather than left to the tool.
func injectTitles(doc map[string]any, titles map[string]string) error {
	definitions, ok := doc["definitions"].(map[string]any)
	if !ok {
		return errNoDefinitions
	}
	for _, name := range sortedKeys(titles) {
		definition, ok := definitions[name].(map[string]any)
		if !ok {
			return fmt.Errorf(
				"definitions/%s is missing, and %s generates from it", name, titles[name],
			)
		}
		switch title := definition["title"]; title {
		case nil, titles[name]:
			definition["title"] = titles[name]
		default:
			return fmt.Errorf(
				"definitions/%s already has the title %q, so generating %s from it "+
					"would rename the type",
				name, title, titles[name],
			)
		}
	}
	return nil
}

// walkObjects calls visit on doc and on every object below it, in key order so
// that the error normalize reports for a schema with several offending
// constructs is the same on every run.
func walkObjects(doc map[string]any, visit func(path string, node map[string]any) error) error {
	return walkFrom("", doc, visit)
}

func walkFrom(path string, node any, visit func(path string, node map[string]any) error) error {
	switch node := node.(type) {
	case map[string]any:
		if err := visit(path, node); err != nil {
			return err
		}
		for _, key := range sortedKeys(node) {
			if err := walkFrom(path+"/"+key, node[key], visit); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range node {
			if err := walkFrom(fmt.Sprintf("%s/%d", path, i), item, visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
