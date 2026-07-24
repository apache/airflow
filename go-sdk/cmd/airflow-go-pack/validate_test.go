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
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

func warningsFor(dags map[string]airflowmetadata.Dag) string {
	var buf bytes.Buffer
	warnOnSuspiciousIDs(&buf, airflowmetadata.Manifest{Dags: dags})
	return buf.String()
}

func TestWarnOnSuspiciousIDs_ValidIdsProduceNoWarnings(t *testing.T) {
	for _, id := range []string{
		"simple", "with-dash", "with.dot", "with_underscore", "0numeric",
		"café_dag", "任務", strings.Repeat("a", 250), strings.Repeat("任", 250),
	} {
		out := warningsFor(map[string]airflowmetadata.Dag{id: {Tasks: []string{id}}})
		assert.Empty(t, out, "id %q should not warn", id)
	}
}

func TestWarnOnSuspiciousIDs_TooLongIDWarns(t *testing.T) {
	for _, id := range []string{strings.Repeat("a", 251), strings.Repeat("任", 251)} {
		out := warningsFor(map[string]airflowmetadata.Dag{id: {}})
		assert.Contains(t, out, "is longer than 250 characters (251)")
	}
}

func TestWarnOnSuspiciousIDs_InvalidCharsWarn(t *testing.T) {
	for _, id := range []string{"", "with space", "with/slash", "with:colon", "with\ttab"} {
		out := warningsFor(map[string]airflowmetadata.Dag{id: {}})
		assert.Contains(
			t, out,
			"must be made of alphanumeric characters, dashes, dots, and underscores",
			"id %q should warn", id,
		)
	}
}

func TestWarnOnSuspiciousIDs_DoubleDotWarns(t *testing.T) {
	out := warningsFor(map[string]airflowmetadata.Dag{"a..b": {}})
	assert.Contains(t, out, `dag id "a..b" contains '..'`)
	assert.Contains(t, out, "allow_double_dot_in_ids")
	assert.NotContains(t, out, "must be made of alphanumeric characters")
}

func TestWarnOnSuspiciousIDs_TaskWarningNamesItsDag(t *testing.T) {
	out := warningsFor(map[string]airflowmetadata.Dag{"my_dag": {Tasks: []string{"bad task"}}})
	assert.Contains(t, out, `task id "bad task" in dag "my_dag" must be made of`)
}

// Packing succeeds despite a suspicious dag id: the check is best-effort and
// the server-side validation stays the source of truth.
func TestRunPack_WarnsOnSuspiciousIDsButPacks(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "foreign")
	require.NoError(t, os.WriteFile(exe, []byte("foreign-arch-binary-bytes"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	meta := filepath.Join(dir, "airflow-metadata.json")
	require.NoError(t, os.WriteFile(meta, []byte(
		`{"airflow_bundle_metadata_version":"1.0",`+
			`"sdk":{"language":"go","version":"0.1.0","supervisor_schema_version":"2026-06-16"},`+
			`"dags":{"bad dag":{"tasks":["t1"]}}}`,
	), 0o644))
	out := filepath.Join(dir, "bundle")

	var stderr bytes.Buffer
	err := runPack(&bytes.Buffer{}, &stderr, &packOptions{
		executable:      exe,
		source:          source,
		airflowMetadata: meta,
		output:          out,
	})
	require.NoError(t, err)
	assert.Contains(t, stderr.String(), `warning: dag id "bad dag" must be made of`)
	assert.FileExists(t, out)
}
