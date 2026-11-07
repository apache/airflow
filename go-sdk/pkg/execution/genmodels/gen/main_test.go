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
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadTestSchema(t *testing.T, defs string) *schemaDoc {
	t.Helper()
	var doc schemaDoc
	require.NoError(t, json.Unmarshal([]byte(`{"$defs": `+defs+`}`), &doc))
	return &doc
}

const (
	dateTimeJSON     = `{"type": "string", "format": "date-time"}`
	nullJSON         = `{"type": "null"}`
	dateTimeListJSON = `{"type": "array", "items": ` + dateTimeJSON + `}`
)

func TestFindNullableDateTimes(t *testing.T) {
	doc := loadTestSchema(t, `{"GetAssetByUri": {"properties": {
		"value":         `+dateTimeJSON+`,
		"nullable":      {"anyOf": [`+dateTimeJSON+`, `+nullJSON+`]},
		"null_first":    {"anyOf": [`+nullJSON+`, `+dateTimeJSON+`]},
		"list":          {"anyOf": [`+dateTimeListJSON+`, `+nullJSON+`]},
		"nullable_text": {"anyOf": [{"type": "string"}, `+nullJSON+`]}
	}}}`)
	structByKey := map[string]string{"GETASSETBYURI": "GetAssetByURI"}

	got, err := findNullableDateTimes(doc, structByKey)

	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]bool{
		"GetAssetByURI": {"nullable": true, "null_first": true},
	}, got)
}

func TestFindNullableDateTimes_RejectsUnsupportedShape(t *testing.T) {
	tests := map[string]string{
		"date-time or string":      `{"anyOf": [` + dateTimeJSON + `, {"type": "string"}]}`,
		"three branches":           `{"anyOf": [` + dateTimeJSON + `, {"type": "string"}, ` + nullJSON + `]}`,
		"non-nullable list":        dateTimeListJSON,
		"date-time nested in list": `{"anyOf": [{"type": "array", "items": {"anyOf": [` + dateTimeJSON + `, ` + nullJSON + `]}}, ` + nullJSON + `]}`,
	}
	for name, prop := range tests {
		t.Run(name, func(t *testing.T) {
			doc := loadTestSchema(t, `{"DagRun": {"properties": {"end_date": `+prop+`}}}`)

			_, err := findNullableDateTimes(doc, map[string]string{"DAGRUN": "DagRun"})

			require.ErrorContains(t, err, "DagRun.end_date: unsupported date-time schema shape")
		})
	}
}

const unretypedModels = `package genmodels

import "time"

type DagRun struct {
	RunAfter    time.Time   ` + "`msgpack:\"run_after\"`" + `
	QueuedAt    *time.Time  ` + "`msgpack:\"queued_at,omitempty\"`" + `
	LogicalDate interface{} ` + "`msgpack:\"logical_date\"`" + `
	Note        interface{} ` + "`msgpack:\"note\"`" + `
}

type LogicalDates []time.Time
`

const retypedModels = `package genmodels

type DagRun struct {
	RunAfter    Time        ` + "`msgpack:\"run_after\"`" + `
	QueuedAt    *Time       ` + "`msgpack:\"queued_at,omitempty\"`" + `
	LogicalDate *Time       ` + "`msgpack:\"logical_date\"`" + `
	Note        interface{} ` + "`msgpack:\"note\"`" + `
}

type LogicalDates []Time
`

var nullableLogicalDate = map[string]map[string]bool{"DagRun": {"logical_date": true}}

func writeTestModels(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "models.gen.go")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))
	return path
}

func readFormatted(t *testing.T, path string) string {
	t.Helper()
	src, err := os.ReadFile(path)
	require.NoError(t, err)
	formatted, err := format.Source(src)
	require.NoError(t, err)
	return string(formatted)
}

func TestRetypeDateTimes(t *testing.T) {
	path := writeTestModels(t, unretypedModels)

	require.NoError(t, retypeDateTimes(path, nullableLogicalDate))

	want, err := format.Source([]byte(retypedModels))
	require.NoError(t, err)
	assert.Equal(t, string(want), readFormatted(t, path))
}

func TestRetypeDateTimes_RerunIsNoOp(t *testing.T) {
	// Not gofmt-clean, so a rewrite would show up as reformatted bytes.
	src := strings.Replace(
		retypedModels,
		"type LogicalDates []Time",
		"type LogicalDates   []Time",
		1,
	)
	path := writeTestModels(t, src)

	require.NoError(t, retypeDateTimes(path, nullableLogicalDate))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, src, string(got), "an already retyped file must not be rewritten")
}

func TestRetypeDateTimes_RejectsNullableThatIsNotInterface(t *testing.T) {
	path := writeTestModels(t, unretypedModels)

	err := retypeDateTimes(path, map[string]map[string]bool{"DagRun": {"run_after": true}})

	require.ErrorContains(t, err, "DagRun.RunAfter: nullable date-time is not interface{}")
}
