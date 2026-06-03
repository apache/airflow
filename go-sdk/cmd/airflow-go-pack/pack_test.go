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
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

func TestRenderManifest_DeterministicDagOrdering(t *testing.T) {
	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: "1.0",
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 "0.1.0",
			SupervisorSchemaVersion: "2026-06-16",
		},
		Dags: map[string]airflowmetadata.Dag{
			"zeta_dag":  {Tasks: []string{"a", "b"}},
			"alpha_dag": {Tasks: []string{"x"}},
		},
	}

	got1, err := renderManifest(meta, "main.go")
	require.NoError(t, err)
	got2, err := renderManifest(meta, "main.go")
	require.NoError(t, err)

	assert.Equal(t, got1, got2, "manifest should be byte-identical for identical input")

	expected := `airflow_bundle_metadata_version: "1.0"
sdk:
  language: go
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: main.go
dags:
  alpha_dag:
    tasks:
      - x
  zeta_dag:
    tasks:
      - a
      - b
`
	assert.Equal(t, expected, string(got1))
}

func TestRenderManifest_EmptyDags(t *testing.T) {
	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: "1.0",
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 "0.1.0",
			SupervisorSchemaVersion: "2026-06-16",
		},
		Dags: map[string]airflowmetadata.Dag{},
	}
	got, err := renderManifest(meta, "main.go")
	require.NoError(t, err)
	assert.Contains(t, string(got), "dags: {}")
}

// TestRootArgs_AllowsBuildFlagsAfterDoubleDash regression-tests the
// positional-arg validator: forwarded `go build` flags after "--" must not
// be counted against MaximumNArgs(1). The runtime call would otherwise fail
// with `accepts at most 1 arg(s), received N`.
func TestRootArgs_AllowsBuildFlagsAfterDoubleDash(t *testing.T) {
	cases := [][]string{
		{"--", "-ldflags", "-X main.dagId=foo"},
		{"./pkg", "--", "-ldflags", "-X main.dagId=foo"},
		{"--", "-trimpath", "-tags=prod"},
	}
	for _, argv := range cases {
		cmd := newRootCmd()
		// Stop the command from actually running; we only want arg validation.
		cmd.RunE = func(*cobra.Command, []string) error { return nil }
		cmd.SetArgs(argv)
		assert.NoError(t, cmd.Execute(), "args=%v should validate", argv)
	}
}

// TestRunIntrospect_ClassifiesExecFailure verifies the exec-startability
// classification that drives the --executable host-fallback path: a file the
// OS refuses to exec (wrong format / arch stand-in) is reported as
// errExecNotStartable, while a binary that runs and exits non-zero is not.
func TestRunIntrospect_ClassifiesExecFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exec-format semantics differ on Windows")
	}
	dir := t.TempDir()

	// A non-binary file with the exec bit set: execve rejects it with an
	// exec-format error, standing in for a foreign-arch executable.
	garbage := filepath.Join(dir, "garbage")
	require.NoError(t, os.WriteFile(garbage, []byte("not a real executable\n"), 0o755))
	_, err := runIntrospect(garbage, "--airflow-metadata")
	require.Error(t, err)
	assert.ErrorIs(t, err, errExecNotStartable)

	// A script that starts and exits non-zero is a genuine run failure, not
	// an unrunnable binary, so it must NOT be classified as not-startable.
	failing := filepath.Join(dir, "failing")
	require.NoError(t, os.WriteFile(failing, []byte("#!/bin/sh\nexit 3\n"), 0o755))
	_, err = runIntrospect(failing, "--airflow-metadata")
	require.Error(t, err)
	assert.False(
		t,
		errors.Is(err, errExecNotStartable),
		"non-zero exit should not be errExecNotStartable",
	)
}

func TestRootArgs_RejectsExtraPositionalBeforeDash(t *testing.T) {
	cmd := newRootCmd()
	cmd.RunE = func(*cobra.Command, []string) error { return nil }
	cmd.SetArgs([]string{"./pkg1", "./pkg2", "--", "-ldflags", "-X main.dagId=foo"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts at most 1 arg")
}

func TestSameFile(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	require.NoError(t, os.WriteFile(a, []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(b, []byte("b"), 0o644))

	// Distinct existing files do not alias.
	same, err := sameFile(a, b)
	require.NoError(t, err)
	assert.False(t, same)

	// Two spellings of the same path alias even though only one exists on
	// disk under the literal string ("./a" vs "a" relative to the same dir).
	same, err = sameFile(filepath.Join(dir, "x"), filepath.Join(dir, ".", "x"))
	require.NoError(t, err)
	assert.True(t, same, "cleaned-abs equality should treat ./x and x as the same file")

	// A non-existent output never aliases an existing input by inode.
	same, err = sameFile(filepath.Join(dir, "does-not-exist"), a)
	require.NoError(t, err)
	assert.False(t, same)

	// A symlink that points at the input shares its inode.
	link := filepath.Join(dir, "link-to-a")
	if err := os.Symlink(a, link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	same, err = sameFile(link, a)
	require.NoError(t, err)
	assert.True(t, same, "a symlink to the file should alias it")
}

// TestRunPack_RejectsOutputAliasingExecutable is the regression test for the
// truncation bug: when --output resolves to the same file as --executable,
// runPack must refuse before copyFile truncates the input. We assert both the
// error and that the executable's bytes survive untouched.
func TestRunPack_RejectsOutputAliasingExecutable(t *testing.T) {
	dir := t.TempDir()
	exec := filepath.Join(dir, "bundle")
	source := filepath.Join(dir, "main.go")
	original := []byte("prebuilt-binary-bytes")
	require.NoError(t, os.WriteFile(exec, original, 0o755))
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable: exec,
		source:     source,
		output:     exec,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same file as the executable")

	// The guard must fire before any write: the executable is intact.
	got, readErr := os.ReadFile(exec)
	require.NoError(t, readErr)
	assert.Equal(t, original, got, "executable must not be truncated when output aliases it")
}

// TestRunPack_RejectsDefaultOutputAliasingExecutable covers the no-explicit
// --output path the reviewer flagged: packing "./bundle" from a package
// directory named "bundle" derives a default output that collides with the
// pre-built binary.
func TestRunPack_RejectsDefaultOutputAliasingExecutable(t *testing.T) {
	parent := t.TempDir()
	pkgDir := filepath.Join(parent, "bundle")
	require.NoError(t, os.Mkdir(pkgDir, 0o755))
	exec := filepath.Join(pkgDir, "bundle")
	source := filepath.Join(pkgDir, "main.go")
	original := []byte("prebuilt-binary-bytes")
	require.NoError(t, os.WriteFile(exec, original, 0o755))
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))

	// defaultOutputPath derives the bundle name from the package dir base
	// ("bundle") relative to cwd, so run from pkgDir to reproduce the collision.
	t.Chdir(pkgDir)

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable: "./bundle",
		source:     "main.go",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same file as the executable")

	got, readErr := os.ReadFile(exec)
	require.NoError(t, readErr)
	assert.Equal(
		t,
		original,
		got,
		"executable must not be truncated by the default output collision",
	)
}
