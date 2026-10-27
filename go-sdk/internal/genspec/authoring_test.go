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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shapes is one definition's worth of rules, named as the schema names it.
func shapes(shape authoringShape) map[string]authoringShape {
	return map[string]authoringShape{"dag": shape}
}

func TestShapeForAuthoringDropsAnExcludedPropertyAndItsRequiredEntry(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"required": ["fileloc", "description"],
				"properties": {
					"fileloc": {"type": "string"},
					"description": {"type": "string"}
				}
			}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		exclude: map[string]string{"fileloc": "the bundle fills it in"},
	})))

	dag := doc["definitions"].(map[string]any)["dag"].(map[string]any)
	assert.NotContains(t, dag["properties"], "fileloc")
	assert.Contains(t, dag["properties"], "description")
	assert.NotContains(t, dag, "required", "an authoring struct has no required field")
}

func TestShapeForAuthoringReportsAnExclusionTheSchemaNoLongerHas(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {"dag": {"type": "object", "properties": {"description": {"type": "string"}}}}
	}`)

	err := shapeForAuthoring(doc, shapes(authoringShape{
		exclude: map[string]string{"fileloc": "the bundle fills it in"},
	}))

	assert.EqualError(
		t,
		err,
		`definitions/dag/properties/fileloc is excluded as "the bundle fills it in", `+
			"but the schema no longer has it",
	)
}

func TestShapeForAuthoringOverridesTheGoTypeOfAReferencedProperty(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object", "properties": {"start_date": {"$ref": "#/definitions/datetime"}}},
			"datetime": {"type": "number"}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		override: map[string]propertyOverride{
			"start_date": {goType: "time.Time", imports: []string{"time"}},
		},
	})))

	properties := doc["definitions"].(map[string]any)["dag"].(map[string]any)["properties"].(map[string]any)
	startDate := properties["start_date"].(map[string]any)
	assert.NotContains(t, startDate, "$ref", "the reference would outlive the definition it names")
	assert.Equal(t, "time.Time", startDate["goJSONSchema"].(map[string]any)["type"])
	assert.Equal(t, []any{"time"}, startDate["goJSONSchema"].(map[string]any)["imports"])
}

func TestShapeForAuthoringOverridesTheDocCommentOfAProperty(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {"dag": {"type": "object", "properties": {"dag_id": {"type": "string"}}}}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		override: map[string]propertyOverride{
			"dag_id": {goType: "string", doc: "DagID is what the constructor takes."},
		},
	})))

	properties := doc["definitions"].(map[string]any)["dag"].(map[string]any)["properties"].(map[string]any)
	assert.Equal(
		t,
		"DagID is what the constructor takes.",
		properties["dag_id"].(map[string]any)["description"],
	)
}

func TestShapeForAuthoringOverridesTheDocOfAPropertyWithoutChangingItsType(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"operator": {
				"type": "object",
				"properties": {"email_on_failure": {"type": "boolean", "default": true}}
			}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, map[string]authoringShape{
		"operator": {
			override: map[string]propertyOverride{
				"email_on_failure": {doc: "EmailOnFailure has no effect yet."},
			},
		},
	}))

	property := doc["definitions"].(map[string]any)["operator"].(map[string]any)["properties"].(map[string]any)["email_on_failure"].(map[string]any)
	assert.Equal(t, "EmailOnFailure has no effect yet.", property["description"])
	assert.NotContains(t, property["goJSONSchema"], "type")
	assert.Equal(t, "boolean", property["type"])
}

func TestShapeForAuthoringLetsAnOverrideForceAPointer(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {
					"max_consecutive_failed_dag_runs": {"type": "number"},
					"max_active_runs": {"type": "number"}
				}
			}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		override: map[string]propertyOverride{
			"max_consecutive_failed_dag_runs": {goType: "int", pointer: true},
			"max_active_runs":                 {goType: "int"},
		},
	})))

	properties := doc["definitions"].(map[string]any)["dag"].(map[string]any)["properties"].(map[string]any)
	forced := properties["max_consecutive_failed_dag_runs"].(map[string]any)
	assert.Equal(t, true, forced["goJSONSchema"].(map[string]any)["pointer"],
		"0 is a setting for this count, which the schema gives no way to work out")
	ruled := properties["max_active_runs"].(map[string]any)
	assert.Equal(t, false, ruled["goJSONSchema"].(map[string]any)["pointer"])
}

func TestShapeForAuthoringReportsAnOverrideTheSchemaNoLongerHas(t *testing.T) {
	doc := schemaFrom(t, `{"definitions": {"dag": {"type": "object", "properties": {}}}}`)

	err := shapeForAuthoring(doc, shapes(authoringShape{
		override: map[string]propertyOverride{"start_date": {goType: "time.Time"}},
	}))

	assert.EqualError(
		t,
		err,
		"definitions/dag/properties/start_date is overridden to time.Time, "+
			"but the schema no longer has it",
	)
}

func TestShapeForAuthoringInjectsAPropertyTheSchemaHasNoCounterpartFor(t *testing.T) {
	doc := schemaFrom(t, `{"definitions": {"dag": {"type": "object", "properties": {}}}}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		inject: map[string]map[string]any{"schedule": {"type": "string"}},
	})))

	properties := doc["definitions"].(map[string]any)["dag"].(map[string]any)["properties"].(map[string]any)
	assert.Equal(t, "string", properties["schedule"].(map[string]any)["type"])
}

func TestShapeForAuthoringReportsAnInjectionTheSchemaNowDeclares(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {"dag": {"type": "object", "properties": {"schedule": {"type": "object"}}}}
	}`)

	err := shapeForAuthoring(doc, shapes(authoringShape{
		inject: map[string]map[string]any{"schedule": {"type": "string"}},
	}))

	assert.EqualError(
		t,
		err,
		"definitions/dag/properties/schedule is injected, but the schema now declares it too",
	)
}

func TestShapeForAuthoringReportsAReferenceToAPrunedDefinition(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object", "properties": {"params": {"$ref": "#/definitions/params"}}},
			"params": {"type": "object"}
		}
	}`)

	err := shapeForAuthoring(doc, shapes(authoringShape{}))

	assert.EqualError(
		t,
		err,
		"/definitions/dag/properties/params references definitions/params, which the spec structs "+
			"do not generate; exclude the property or give it a type override",
	)
}

func TestShapeForAuthoringKeepsOnlyTheDefinitionsTheSpecsGenerateFrom(t *testing.T) {
	doc := schemaFrom(t, `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"allOf": [{"properties": {"dag": {"$ref": "#/definitions/dag"}}}],
		"definitions": {
			"dag": {"type": "object", "properties": {}},
			"asset": {"type": "object"}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{})))

	assert.Equal(t, []string{"$schema", "definitions"}, sortedKeys(doc),
		"the root describes a serialized Dag file, which is not a spec")
	assert.Equal(t, []string{"dag"}, sortedKeys(doc["definitions"].(map[string]any)))
}

func TestShapeForAuthoringRejectsACombinatorItHasNoRuleFor(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {
					"render_template_as_native_obj": {
						"anyOf": [{"type": "boolean"}, {"type": "null"}]
					}
				}
			}
		}
	}`)

	err := shapeForAuthoring(doc, shapes(authoringShape{}))

	assert.EqualError(
		t,
		err,
		"/definitions/dag/properties/render_template_as_native_obj has anyOf, which genspec "+
			"has no rule for; exclude the property or give it a type override",
	)
}

func TestShapeForAuthoringKeepsACombinatorAnOverrideReplaces(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {"deadline": {"anyOf": [{"type": "number"}, {"type": "null"}]}}
			}
		}
	}`)

	assert.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{
		override: map[string]propertyOverride{
			"deadline": {goType: "time.Duration", imports: []string{"time"}},
		},
	})))
}

func TestShapeForAuthoringPointsOnlyAtAScalarWhoseDefaultIsNotTheGoZeroValue(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {
					"do_xcom_push": {"type": "boolean", "default": true},
					"fail_fast": {"type": "boolean", "default": false},
					"catchup": {"type": "boolean"},
					"retries": {"type": "number", "default": 0},
					"pool_slots": {"type": "number", "default": 1},
					"max_active_runs": {"type": "number"},
					"owner": {"type": "string", "default": "airflow"},
					"tags": {"type": "array"}
				}
			}
		}
	}`)

	require.NoError(t, shapeForAuthoring(doc, shapes(authoringShape{})))

	properties := doc["definitions"].(map[string]any)["dag"].(map[string]any)["properties"].(map[string]any)
	for name, pointer := range map[string]bool{
		"do_xcom_push": true,
		"pool_slots":   true,
		// A boolean with no schema default takes its default from Airflow config, which
		// can be true, so false has to be expressible.
		"catchup":   true,
		"fail_fast": false,
		"retries":   false,
		// 0 is not a value max_active_runs can take, so its zero value can only mean unset.
		"max_active_runs": false,
		"owner":           false,
		"tags":            false,
	} {
		node := properties[name].(map[string]any)
		assert.Equal(t, pointer, node["goJSONSchema"].(map[string]any)["pointer"], name)
	}
}

// TestShapeForAuthoringShapesTheCoreSchema is the tripwire for a property added,
// renamed or retyped on the Python side that the shape tables have no rule for.
func TestShapeForAuthoringShapesTheCoreSchema(t *testing.T) {
	doc := readCoreSchema(t)

	require.NoError(t, shapeForAuthoring(doc, authoringShapes))
	require.NoError(t, normalize(doc, specTitles))

	definitions := doc["definitions"].(map[string]any)
	assert.Equal(t, []string{"dag", "operator", "task_group"}, sortedKeys(definitions))
	dag := definitions["dag"].(map[string]any)["properties"].(map[string]any)
	assert.Contains(t, dag, "schedule", "Schedule is injected over the serialized timetable")
	assert.Contains(t, dag, "queue", "Queue is injected, because the schema has no Dag queue")
	task := definitions["operator"].(map[string]any)["properties"].(map[string]any)
	assert.Equal(
		t,
		"TriggerRule",
		task["trigger_rule"].(map[string]any)["goJSONSchema"].(map[string]any)["type"],
	)
	assert.Equal(
		t,
		"WeightRule",
		task["weight_rule"].(map[string]any)["goJSONSchema"].(map[string]any)["type"],
	)
	assert.Contains(
		t,
		task["task_id"].(map[string]any)["description"],
		"the name of the Go function",
		"the serialized property says nothing about how a task_id is defaulted",
	)
	group := definitions["task_group"].(map[string]any)["properties"].(map[string]any)
	assert.Equal(
		t,
		[]string{
			"doc_md",
			"group_display_name",
			"prefix_group_id",
			"tooltip",
			"ui_color",
			"ui_fgcolor",
		},
		sortedKeys(group),
	)
	prefix := group["prefix_group_id"].(map[string]any)["goJSONSchema"].(map[string]any)
	assert.Equal(
		t,
		true,
		prefix["pointer"],
		"a group_id prefixes by default, so false has to be expressible",
	)
}
