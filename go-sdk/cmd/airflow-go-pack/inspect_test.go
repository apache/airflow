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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inspect reads a bundle through bundlefooter.Read and prints the embedded
// manifest, prefixing the source too under --source.
func TestInspectCmd(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "input-bin")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))
	source := []byte("package main\n\nfunc main() {}\n")
	manifest := []byte(
		"airflow_bundle_metadata_version: \"1.0\"\n" +
			"dags:\n" +
			"  my_dag:\n" +
			"    tasks:\n" +
			"      - \"t1\"\n",
	)
	bundle := filepath.Join(dir, "bundle")
	require.NoError(t, writeBundle(exe, bundle, source, manifest))

	for _, tc := range []struct {
		name   string
		args   []string
		expect string
	}{
		{
			name:   "manifest only",
			args:   []string{bundle},
			expect: string(manifest),
		},
		{
			name: "with source",
			args: []string{"--source", bundle},
			expect: "# --- source ---\n" + string(source) +
				"# --- manifest ---\n" + string(manifest),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newInspectCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tc.args)
			require.NoError(t, cmd.Execute())
			assert.Equal(t, tc.expect, out.String())
		})
	}
}
