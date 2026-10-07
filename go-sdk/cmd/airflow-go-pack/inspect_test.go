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
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inspect reads a bundle through bundlefooter.Read and prints the embedded
// manifest, prefixing each embedded source file too under --source.
func TestInspectCmd(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "input-bin")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))
	entry := "package main\n\nfunc main() {}\n"
	dag := "package dags"
	manifest := []byte(
		"airflow_bundle_metadata_version: \"1.0\"\n" +
			"entrypoint_path: \"cmd/main.go\"\n" +
			"sources:\n" +
			"  - path: \"cmd/main.go\"\n" +
			"    offset: 0\n" +
			"    length: " + strconv.Itoa(len(entry)) + "\n" +
			"  - path: \"dags/etl.go\"\n" +
			"    offset: " + strconv.Itoa(len(entry)) + "\n" +
			"    length: " + strconv.Itoa(len(dag)) + "\n" +
			"dags:\n" +
			"  my_dag:\n" +
			"    tasks:\n" +
			"      - \"t1\"\n",
	)
	bundle := filepath.Join(dir, "bundle")
	require.NoError(t, writeBundle(exe, bundle, []byte(entry+dag), manifest))

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
			expect: "# --- source: cmd/main.go ---\n" + entry +
				"# --- source: dags/etl.go ---\n" + dag + "\n" +
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

func TestEmbeddedSources_RejectsIndexOutsideRegion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		region   string
		manifest string
		wantErr  string
	}{
		{
			name:     "past the end",
			region:   "abc",
			manifest: "sources:\n  - {path: a.go, offset: 2, length: 5}\n",
			wantErr:  `source "a.go" (offset 2, length 5) does not fit the 3-byte source region`,
		},
		{
			name:     "negative offset",
			region:   "abc",
			manifest: "sources:\n  - {path: a.go, offset: -1, length: 2}\n",
			wantErr:  `source "a.go"`,
		},
		{
			name:     "region without an index",
			region:   "abc",
			manifest: "dags: {}\n",
			wantErr:  "repack the bundle",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := embeddedSources([]byte(tc.region), []byte(tc.manifest))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	files, err := embeddedSources(nil, []byte("dags: {}\n"))
	require.NoError(t, err)
	assert.Empty(t, files)
}

// Inspect prints every file that packing the multidag fixture embedded.
func TestInspectCmd_PrintsEachPackedFile(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out := filepath.Join(t.TempDir(), "bundle")
	pack := newRootCmd()
	pack.SetArgs([]string{"./testdata/multidag", "--output", out})
	pack.SetOut(&bytes.Buffer{})
	pack.SetErr(&bytes.Buffer{})
	require.NoError(t, pack.Execute())

	inspect := newInspectCmd()
	var printed bytes.Buffer
	inspect.SetOut(&printed)
	inspect.SetArgs([]string{"--source", out})
	require.NoError(t, inspect.Execute())
	for _, name := range []string{"main.go", "factory/factory.go", "reports/reports.go"} {
		assert.Contains(
			t,
			printed.String(),
			"# --- source: cmd/airflow-go-pack/testdata/multidag/"+name+" ---\n",
		)
	}
	assert.Contains(t, printed.String(), "# --- manifest ---\n")
}
