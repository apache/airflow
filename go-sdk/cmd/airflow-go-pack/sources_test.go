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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// packedManifest is the part of a packed manifest that the source layout adds.
type packedManifest struct {
	EntrypointPath string            `yaml:"entrypoint_path"`
	DagSourcePaths map[string]string `yaml:"dag_source_paths"`
	Sources        []struct {
		Path   string `yaml:"path"`
		Offset int    `yaml:"offset"`
		Length int    `yaml:"length"`
		SHA256 string `yaml:"sha256"`
	} `yaml:"sources"`
}

func readPacked(t *testing.T, bundle string) (packedManifest, []byte, []byte) {
	t.Helper()
	region, metadata, err := bundlefooter.Read(bundle)
	require.NoError(t, err)
	var m packedManifest
	require.NoError(t, yaml.Unmarshal(metadata, &m))
	return m, region, metadata
}

// writeModule lays out files under a new directory with a go.mod, and returns the directory.
func writeModule(t *testing.T, modulePath string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"),
		[]byte("module "+modulePath+"\n\ngo 1.24\n"), 0o644))
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
	}
	return dir
}

func TestModulePath(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  string
		want string
	}{
		{name: "plain", mod: "module example.com/app\n\ngo 1.24\n", want: "example.com/app"},
		{name: "quoted", mod: "module \"example.com/app\"\n", want: "example.com/app"},
		{name: "comment", mod: "// header\nmodule example.com/app // trailing\n", want: "example.com/app"},
		{name: "missing", mod: "go 1.24\n", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, modulePath([]byte(tc.mod)))
		})
	}
}

func TestFindModule(t *testing.T) {
	dir := writeModule(
		t,
		"example.com/app",
		map[string]string{"cmd/bundle/main.go": "package main\n"},
	)

	mod, err := findModule(filepath.Join(dir, "cmd", "bundle"))
	require.NoError(t, err)
	assert.Equal(t, goModule{dir: dir, path: "example.com/app"}, mod)

	bare := t.TempDir()
	mod, err = findModule(bare)
	require.NoError(t, err)
	assert.Equal(t, goModule{dir: bare}, mod, "without a go.mod the directory is the module root")
}

func TestModuleRelPath(t *testing.T) {
	dir := writeModule(t, "example.com/app", map[string]string{
		"main.go":       "package main\n",
		"dags/etl.go":   "package dags\n",
		"sub/nested.go": "package sub\n",
	})
	mod := goModule{dir: dir, path: "example.com/app"}

	for _, tc := range []struct {
		name     string
		reported string
		want     string
		ok       bool
	}{
		{name: "absolute build path", reported: filepath.ToSlash(filepath.Join(dir, "dags", "etl.go")), want: "dags/etl.go", ok: true},
		{name: "trimpath", reported: "example.com/app/dags/etl.go", want: "dags/etl.go", ok: true},
		{name: "trimpath at root", reported: "example.com/app/main.go", want: "main.go", ok: true},
		{name: "absolute path outside the module", reported: "/home/u/go/pkg/mod/example.com/lib@v1.0.0/x.go"},
		{name: "another module", reported: "example.com/lib@v1.0.0/x.go"},
		{name: "module path prefix of another module", reported: "example.com/app/other@v1.0.0/x.go"},
		{name: "missing file", reported: "example.com/app/gone.go"},
		{name: "escapes the module", reported: "example.com/app/../x.go"},
		{name: "directory", reported: "example.com/app/dags"},
		{name: "empty", reported: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := moduleRelPath(tc.reported, mod)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLayoutSources(t *testing.T) {
	dir := writeModule(t, "example.com/app", map[string]string{
		"cmd/bundle/main.go": "package main\n",
		"dags/b.go":          "package dags // b\n",
		"dags/a.go":          "package dags // a\n",
	})
	mod := goModule{dir: dir, path: "example.com/app"}
	meta := airflowmetadata.Manifest{DagSourceFiles: map[string]string{
		"on_b":    "example.com/app/dags/b.go",
		"also_b":  filepath.ToSlash(filepath.Join(dir, "dags", "b.go")),
		"on_a":    "example.com/app/dags/a.go",
		"on_main": "example.com/app/cmd/bundle/main.go",
		"foreign": "example.com/lib@v1.0.0/x.go",
	}}

	var stderr bytes.Buffer
	layout, region, err := layoutSources(
		&stderr,
		meta,
		filepath.Join(dir, "cmd", "bundle", "main.go"),
		mod,
	)
	require.NoError(t, err)

	assert.Equal(t, "cmd/bundle/main.go", layout.entrypoint)
	assert.Equal(t, map[string]string{
		"on_b":    "dags/b.go",
		"also_b":  "dags/b.go",
		"on_a":    "dags/a.go",
		"on_main": "cmd/bundle/main.go",
		"foreign": "cmd/bundle/main.go",
	}, layout.dagPaths)

	var paths []string
	offset := 0
	for _, f := range layout.files {
		paths = append(paths, f.path)
		data, err := os.ReadFile(f.disk)
		require.NoError(t, err)
		assert.Equal(t, offset, f.offset, f.path)
		assert.Equal(t, len(data), f.length, f.path)
		assert.Equal(t, sha256Hex(data), f.sha256, f.path)
		assert.Equal(t, data, region[f.offset:f.offset+f.length], f.path)
		offset += f.length
	}
	assert.Equal(t, []string{"cmd/bundle/main.go", "dags/a.go", "dags/b.go"}, paths,
		"entrypoint first, then the rest sorted, each file once")
	assert.Len(t, region, offset)

	assert.Contains(
		t,
		stderr.String(),
		`warning: source file "example.com/lib@v1.0.0/x.go" of dag "foreign"`,
	)
	assert.Equal(t, 1, bytes.Count(stderr.Bytes(), []byte("warning:")))
}

func TestLayoutSources_EntrypointOutsideModule(t *testing.T) {
	mod := goModule{dir: t.TempDir()}
	elsewhere := filepath.Join(t.TempDir(), "main.go")
	require.NoError(t, os.WriteFile(elsewhere, []byte("package main\n"), 0o644))

	layout, _, err := layoutSources(&bytes.Buffer{}, airflowmetadata.Manifest{}, elsewhere, mod)
	require.NoError(t, err)
	assert.Equal(t, "main.go", layout.entrypoint)
	assert.Empty(t, layout.dagPaths)
}

func TestRunPack_EmbedsEntrypointWhenBundleHasNoDagSources(t *testing.T) {
	dir := writeModule(
		t,
		"example.com/app",
		map[string]string{"main.go": "package main\nfunc main() {}\n"},
	)
	exe := filepath.Join(dir, "prebuilt")
	require.NoError(t, os.WriteFile(exe, []byte("prebuilt-binary-bytes"), 0o755))
	meta := filepath.Join(dir, "airflow-metadata.json")
	require.NoError(t, os.WriteFile(meta, []byte(
		`{"airflow_bundle_metadata_version":"1.0",`+
			`"sdk":{"language":"go","version":"0.1.0","supervisor_schema_version":"2026-06-16"},`+
			`"dags":{"my_dag":{"tasks":["t1"]}}}`,
	), 0o644))
	out := filepath.Join(t.TempDir(), "bundle")

	require.NoError(t, runPack(&bytes.Buffer{}, &bytes.Buffer{}, &packOptions{
		executable:      exe,
		source:          filepath.Join(dir, "main.go"),
		airflowMetadata: meta,
		output:          out,
	}))

	m, region, _ := readPacked(t, out)
	assert.Equal(t, "main.go", m.EntrypointPath)
	assert.Empty(t, m.DagSourcePaths)
	require.Len(t, m.Sources, 1)
	assert.Equal(t, "package main\nfunc main() {}\n", string(region))
}

func TestRunPack_DagFileOutsideModuleFallsBackToEntrypoint(t *testing.T) {
	dir := writeModule(
		t,
		"example.com/app",
		map[string]string{"main.go": "package main\nfunc main() {}\n"},
	)
	exe := filepath.Join(dir, "prebuilt")
	require.NoError(t, os.WriteFile(exe, []byte("prebuilt-binary-bytes"), 0o755))
	meta := filepath.Join(dir, "airflow-metadata.json")
	require.NoError(t, os.WriteFile(meta, []byte(
		`{"airflow_bundle_metadata_version":"1.0",`+
			`"sdk":{"language":"go","version":"0.1.0","supervisor_schema_version":"2026-06-16"},`+
			`"dags":{"py_dag":{"tasks":["t1"]}},`+
			`"dag_source_files":{"vendored":"/root/go/pkg/mod/example.com/lib@v1.0.0/dag.go"}}`,
	), 0o644))
	out := filepath.Join(t.TempDir(), "bundle")

	var stderr bytes.Buffer
	require.NoError(t, runPack(&bytes.Buffer{}, &stderr, &packOptions{
		executable:      exe,
		source:          filepath.Join(dir, "main.go"),
		airflowMetadata: meta,
		output:          out,
	}))

	m, _, _ := readPacked(t, out)
	assert.Equal(t, map[string]string{"vendored": "main.go"}, m.DagSourcePaths)
	require.Len(t, m.Sources, 1)
	assert.Contains(
		t,
		stderr.String(),
		`warning: source file "/root/go/pkg/mod/example.com/lib@v1.0.0/dag.go" of dag "vendored"`,
	)
}

func TestRejectOutputAlias_EmbeddedSourceFile(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "exe")
	entry := filepath.Join(dir, "main.go")
	other := filepath.Join(dir, "dag.go")
	for _, p := range []string{exe, entry, other} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	}

	err := rejectOutputAlias(other, exe, []string{entry, other}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same file as the source")

	assert.NoError(t, rejectOutputAlias(filepath.Join(dir, "out"), exe, []string{entry, other}, ""))
}

// Packs the multidag fixture through the real build path: three Dag files across two imported
// packages plus the entrypoint, with and without -trimpath.
func TestPack_MultiDagBundle(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	for _, tc := range []struct {
		name  string
		build []string
	}{
		{name: "build paths"},
		{name: "trimpath", build: []string{"--", "-trimpath"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const fixture = "cmd/airflow-go-pack/testdata/multidag"
			out := filepath.Join(t.TempDir(), "bundle")
			var stderr bytes.Buffer
			cmd := newRootCmd()
			cmd.SetArgs(append([]string{"./testdata/multidag", "--output", out}, tc.build...))
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&stderr)
			require.NoError(t, cmd.Execute())
			assert.NotContains(t, stderr.String(), "warning")

			m, region, _ := readPacked(t, out)
			assert.Equal(t, fixture+"/main.go", m.EntrypointPath)
			assert.Equal(t, map[string]string{
				"orders":   fixture + "/main.go",
				"reports":  fixture + "/reports/reports.go",
				"billing":  fixture + "/factory/factory.go",
				"shipping": fixture + "/factory/factory.go",
			}, m.DagSourcePaths)

			var paths []string
			for _, s := range m.Sources {
				paths = append(paths, s.Path)
				data, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(s.Path)))
				require.NoError(t, err)
				assert.Equal(t, data, region[s.Offset:s.Offset+s.Length], s.Path)
				assert.Equal(t, sha256Hex(data), s.SHA256, s.Path)
			}
			assert.Equal(t, []string{
				fixture + "/main.go",
				fixture + "/factory/factory.go",
				fixture + "/reports/reports.go",
			}, paths)
		})
	}
}

// A bundle with task handlers only has no Dag source of its own: just the entrypoint.
func TestPack_TaskHandlersOnlyBundle(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out := filepath.Join(t.TempDir(), "bundle")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"./testdata/handlersonly", "--output", out})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	require.NoError(t, cmd.Execute())

	m, _, metadata := readPacked(t, out)
	assert.Equal(t, "cmd/airflow-go-pack/testdata/handlersonly/main.go", m.EntrypointPath)
	assert.Empty(t, m.DagSourcePaths)
	assert.Len(t, m.Sources, 1)
	assert.Contains(t, string(metadata), "dag_source_paths: {}")
}

// Identical inputs give a byte-identical bundle.
func TestPack_IsDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	exe := filepath.Join(t.TempDir(), "multidag")
	goBuild(t, "./testdata/multidag", exe, runtime.GOOS, runtime.GOARCH)

	pack := func() string {
		out := filepath.Join(t.TempDir(), "bundle")
		cmd := newRootCmd()
		cmd.SetArgs([]string{
			"--executable", exe,
			"--source", "testdata/multidag/main.go",
			"--output", out,
		})
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		require.NoError(t, cmd.Execute())
		return out
	}
	first, second := pack(), pack()
	a, err := os.ReadFile(first)
	require.NoError(t, err)
	b, err := os.ReadFile(second)
	require.NoError(t, err)
	assert.Equal(t, a, b)
}
