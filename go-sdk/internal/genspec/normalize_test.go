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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// coreSchemaPath is the schema genspec normalizes: go-sdk's vendored copy of
// airflow-core's, so this test runs in a standalone checkout as well.
const coreSchemaPath = "../../schema/dag-schema.json"

func schemaFrom(t *testing.T, body string) map[string]any {
	t.Helper()

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &doc))
	return doc
}

func readCoreSchema(t *testing.T) map[string]any {
	t.Helper()

	doc, err := readSchema(filepath.FromSlash(coreSchemaPath))
	require.NoError(t, err)
	return doc
}

func TestNormalizeResolvesNullableType(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {"rerun_with_latest_version": {"type": ["boolean", "null"]}}
			},
			"operator": {"type": "object"}
		}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	dag := doc["definitions"].(map[string]any)["dag"].(map[string]any)
	properties := dag["properties"].(map[string]any)
	assert.Equal(
		t,
		"boolean",
		properties["rerun_with_latest_version"].(map[string]any)["type"],
	)
}

func TestNormalizeKeepsASingleTypeAsItIs(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object", "properties": {"dag_id": {"type": "string"}}},
			"operator": {"type": "object"}
		}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	dag := doc["definitions"].(map[string]any)["dag"].(map[string]any)
	properties := dag["properties"].(map[string]any)
	assert.Equal(t, "string", properties["dag_id"].(map[string]any)["type"])
}

func TestNormalizeDropsDependentRequired(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object"},
			"operator": {
				"type": "object",
				"dependencies": {
					"expand_input": ["partial_kwargs", "_is_mapped"],
					"partial_kwargs": ["expand_input", "_is_mapped"]
				}
			}
		}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	operator := doc["definitions"].(map[string]any)["operator"].(map[string]any)
	assert.NotContains(t, operator, "dependencies")
}

func TestNormalizeKeepsSchemaFormDependencies(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object"},
			"operator": {
				"type": "object",
				"dependencies": {
					"expand_input": ["partial_kwargs"],
					"pool": {"properties": {"pool_slots": {"type": "number"}}}
				}
			}
		}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	operator := doc["definitions"].(map[string]any)["operator"].(map[string]any)
	dependencies := operator["dependencies"].(map[string]any)
	assert.NotContains(t, dependencies, "expand_input")
	assert.Contains(t, dependencies, "pool",
		"go-jsonschema reads a dependency whose value is a schema")
}

func TestNormalizeInjectsTheSpecTitles(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {"dag": {"type": "object"}, "operator": {"type": "object"}}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	definitions := doc["definitions"].(map[string]any)
	assert.Equal(t, "DagSpec", definitions["dag"].(map[string]any)["title"])
	assert.Equal(t, "TaskSpec", definitions["operator"].(map[string]any)["title"])
}

func TestNormalizeRejects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		schema  string
		wantErr string
	}{
		{
			name: "a type union it cannot resolve",
			schema: `{
				"definitions": {
					"dag": {
						"type": "object",
						"properties": {"either": {"type": ["boolean", "string"]}}
					},
					"operator": {"type": "object"}
				}
			}`,
			wantErr: "/definitions/dag/properties/either",
		},
		{
			name: "a type that allows only null",
			schema: `{
				"definitions": {
					"dag": {"type": "object", "properties": {"nothing": {"type": ["null"]}}},
					"operator": {"type": "object"}
				}
			}`,
			wantErr: "allows nothing to generate from",
		},
		{
			name:    "a missing definition",
			schema:  `{"definitions": {"dag": {"type": "object"}}}`,
			wantErr: "definitions/operator is missing",
		},
		{
			name: "a definition that already has a title",
			schema: `{
				"definitions": {
					"dag": {"type": "object", "title": "SerializedDag"},
					"operator": {"type": "object"}
				}
			}`,
			wantErr: `already has the title "SerializedDag"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := normalize(schemaFrom(t, tc.schema), specTitles)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestNormalizeAcceptsTheTitleItWouldInject(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {"type": "object", "title": "DagSpec"},
			"operator": {"type": "object"}
		}
	}`)

	assert.NoError(t, normalize(doc, specTitles))
}

func TestNormalizeResolvesANullableTypeNestedInItems(t *testing.T) {
	doc := schemaFrom(t, `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {"tags": {"type": "array", "items": {"type": ["string", "null"]}}}
			},
			"operator": {"type": "object"}
		}
	}`)

	require.NoError(t, normalize(doc, specTitles))

	dag := doc["definitions"].(map[string]any)["dag"].(map[string]any)
	tags := dag["properties"].(map[string]any)["tags"].(map[string]any)
	assert.Equal(t, "string", tags["items"].(map[string]any)["type"],
		"a pair nested in items resolves by the same rule as one on a property")
}

func TestNormalizeReportsTheSameConstructOnEveryRun(t *testing.T) {
	body := `{
		"definitions": {
			"dag": {
				"type": "object",
				"properties": {
					"a": {"type": ["boolean", "string"]},
					"b": {"type": ["number", "string"]}
				}
			},
			"operator": {"type": "object"}
		}
	}`

	first := normalize(schemaFrom(t, body), specTitles)
	require.Error(t, first)
	for range 20 {
		assert.EqualError(t, normalize(schemaFrom(t, body), specTitles), first.Error())
	}
}

// TestNormalizeReadsTheCoreSchema is the tripwire for a schema change on the
// Python side that genspec has no rule for yet.
func TestNormalizeReadsTheCoreSchema(t *testing.T) {
	doc, err := readSchema(filepath.FromSlash(coreSchemaPath))
	require.NoError(t, err)

	require.NoError(t, normalize(doc, specTitles))

	definitions := doc["definitions"].(map[string]any)
	assert.Equal(t, "DagSpec", definitions["dag"].(map[string]any)["title"])
	assert.Equal(t, "TaskSpec", definitions["operator"].(map[string]any)["title"])
}
