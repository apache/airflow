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
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
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
  language: "go"
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
dags:
  alpha_dag:
    tasks:
      - "x"
  zeta_dag:
    tasks:
      - "a"
      - "b"
`
	assert.Equal(t, expected, string(got1))
}

// Values (task IDs, source, SDK fields) are quoted so a scalar-looking value
// stays a string; Dag ID keys stay plain scalars.
func TestRenderManifest_QuotesValuesNotKeys(t *testing.T) {
	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: "1.0",
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 "0.1.0",
			SupervisorSchemaVersion: "2026-06-16",
		},
		Dags: map[string]airflowmetadata.Dag{
			"my_dag": {Tasks: []string{"123", "true"}},
		},
	}

	got, err := renderManifest(meta, "main.go")
	require.NoError(t, err)

	// Task values that look like scalars are quoted.
	assert.Contains(t, string(got), `- "123"`)
	assert.Contains(t, string(got), `- "true"`)
	// The Dag ID key is a plain scalar, not quoted.
	assert.Contains(t, string(got), "\n  my_dag:\n")
	assert.NotContains(t, string(got), `"my_dag"`)
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

// Forwarded `go build` flags after "--" must not count against MaximumNArgs(1).
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

// A file the OS refuses to exec is errExecNotStartable; a binary that runs and
// exits non-zero is not.
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

// When --output resolves to the same file as --executable, runPack must refuse
// before copyFile truncates the input; the executable's bytes survive.
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

// When --output resolves to the same file as --airflow-metadata, runPack must
// refuse before the bundle is renamed onto it; the manifest file survives.
func TestRunPack_RejectsOutputAliasingMetadataFile(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "foreign")
	require.NoError(t, os.WriteFile(exe, []byte("foreign-arch-binary-bytes"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	meta := filepath.Join(dir, "airflow-metadata.json")
	original := []byte(
		`{"airflow_bundle_metadata_version":"1.0",` +
			`"sdk":{"language":"go","version":"0.1.0","supervisor_schema_version":"2026-06-16"},` +
			`"dags":{"my_dag":{"tasks":["t1"]}}}`,
	)
	require.NoError(t, os.WriteFile(meta, original, 0o644))

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable:      exe,
		source:          source,
		airflowMetadata: meta,
		output:          meta,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same file as the --airflow-metadata file")

	// The guard must fire before any write: the manifest file is intact.
	got, readErr := os.ReadFile(meta)
	require.NoError(t, readErr)
	assert.Equal(t, original, got, "metadata file must not be clobbered when output aliases it")
}

// When the default output path names an existing directory, the packer must
// reject it with --output guidance, not a bare os.Rename "file exists".
func TestRunPack_RejectsDirectoryOutput(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "prebuilt")
	require.NoError(t, os.WriteFile(exe, []byte("prebuilt-binary-bytes"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))

	outDir := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(outDir, 0o755))

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable: exe,
		source:     source,
		output:     outDir,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is an existing directory")
	assert.Contains(t, err.Error(), "--output", "error must point the user at --output")
}

// --executable and --goos/--goarch are mutually exclusive: --executable packs
// the binary as-is and never builds, so it cannot cross-compile.
func TestRunPack_RejectsExecutableWithCrossFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts packOptions
	}{
		{name: "goos", opts: packOptions{executable: "bin", goos: "linux"}},
		{name: "goarch", opts: packOptions{executable: "bin", goarch: "amd64"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runPack(io.Discard, io.Discard, &tc.opts)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"--executable is mutually exclusive with --goos/--goarch",
			)
		})
	}
}

// A foreign-arch --executable that cannot be exec'd on the host is a hard error
// with remediation guidance, never a silent host rebuild that could describe a
// different DAG/task set than the shipped binary.
func TestRunPack_FailsFastWhenExecutableUnrunnableAndNoMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exec-format semantics differ on Windows")
	}
	dir := t.TempDir()
	// A non-binary file with the exec bit set stands in for a foreign-arch
	// executable: execve rejects it with an exec-format error.
	exe := filepath.Join(dir, "foreign")
	require.NoError(t, os.WriteFile(exe, []byte("not a runnable binary\n"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	out := filepath.Join(dir, "bundle")

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable: exe,
		source:     source,
		output:     out,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot exec --executable")
	assert.Contains(t, err.Error(), "--goarch", "error must point at the cross-build workflow")
	assert.Contains(
		t,
		err.Error(),
		"--airflow-metadata",
		"error must mention the --airflow-metadata escape hatch",
	)

	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "no bundle should be written on a fail-fast")
}

// --airflow-metadata short-circuits introspection: a binary that cannot run on
// the host is packed using the supplied JSON manifest, and its bytes survive.
func TestRunPack_UsesMetadataFile(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "foreign")
	exeBytes := []byte("foreign-arch-binary-bytes")
	require.NoError(t, os.WriteFile(exe, exeBytes, 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	meta := filepath.Join(dir, "airflow-metadata.json")
	require.NoError(t, os.WriteFile(meta, []byte(
		`{"airflow_bundle_metadata_version":"1.0",`+
			`"sdk":{"language":"go","version":"0.1.0","supervisor_schema_version":"2026-06-16"},`+
			`"dags":{"my_dag":{"tasks":["t1"]}}}`,
	), 0o644))
	out := filepath.Join(dir, "bundle")

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable:      exe,
		source:          source,
		airflowMetadata: meta,
		output:          out,
	})
	require.NoError(t, err)

	gotSource, gotMeta, err := bundlefooter.Read(out)
	require.NoError(t, err)
	srcBytes, err := os.ReadFile(source)
	require.NoError(t, err)
	assert.Equal(t, srcBytes, gotSource)
	assert.Contains(
		t,
		string(gotMeta),
		"my_dag:",
		"Dag from --airflow-metadata must appear in the manifest",
	)

	bundleBytes, err := os.ReadFile(out)
	require.NoError(t, err)
	binaryRegion := bundleBytes[:len(bundleBytes)-len(gotSource)-len(gotMeta)-bundlefooter.TrailerSize]
	assert.Equal(t, exeBytes, binaryRegion, "the supplied --executable must be packed verbatim")
}

// --airflow-metadata also accepts a YAML manifest, not only the JSON the
// binary prints.
func TestRunPack_AcceptsYAMLMetadataFile(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "foreign")
	require.NoError(t, os.WriteFile(exe, []byte("foreign-arch-binary-bytes"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	meta := filepath.Join(dir, "airflow-metadata.yaml")
	require.NoError(t, os.WriteFile(meta, []byte(
		"airflow_bundle_metadata_version: \"1.0\"\n"+
			"sdk:\n"+
			"  language: \"go\"\n"+
			"  version: \"0.1.0\"\n"+
			"  supervisor_schema_version: \"2026-06-16\"\n"+
			"dags:\n"+
			"  yaml_dag:\n"+
			"    tasks:\n"+
			"      - \"t1\"\n",
	), 0o644))
	out := filepath.Join(dir, "bundle")

	err := runPack(io.Discard, io.Discard, &packOptions{
		executable:      exe,
		source:          source,
		airflowMetadata: meta,
		output:          out,
	})
	require.NoError(t, err)

	_, gotMeta, err := bundlefooter.Read(out)
	require.NoError(t, err)
	assert.Contains(
		t,
		string(gotMeta),
		"yaml_dag:",
		"Dag from a YAML --airflow-metadata file must appear in the manifest",
	)
}

// With no explicit --output, packing "./bundle" from a package dir named
// "bundle" derives a default output that collides with the pre-built binary.
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

// --goos/--goarch > env > host precedence. The flags let `go tool
// airflow-go-pack` cross-compile without GOOS/GOARCH in the env (which would
// cross-build the packer itself).
func TestTargetPlatform(t *testing.T) {
	// An empty value reads like an unset var to os.Getenv, so this clears any
	// ambient cross-compile setting for the default case.
	t.Setenv("GOOS", "")
	t.Setenv("GOARCH", "")

	t.Run("defaults to host", func(t *testing.T) {
		goos, goarch := targetPlatform(&packOptions{})
		assert.Equal(t, runtime.GOOS, goos)
		assert.Equal(t, runtime.GOARCH, goarch)
	})

	t.Run("env overrides host", func(t *testing.T) {
		t.Setenv("GOOS", "linux")
		t.Setenv("GOARCH", "arm64")
		goos, goarch := targetPlatform(&packOptions{})
		assert.Equal(t, "linux", goos)
		assert.Equal(t, "arm64", goarch)
	})

	t.Run("flags override env", func(t *testing.T) {
		t.Setenv("GOOS", "linux")
		t.Setenv("GOARCH", "arm64")
		goos, goarch := targetPlatform(&packOptions{goos: "windows", goarch: "amd64"})
		assert.Equal(t, "windows", goos)
		assert.Equal(t, "amd64", goarch)
	})

	t.Run("resolves each axis independently", func(t *testing.T) {
		t.Setenv("GOOS", "")
		t.Setenv("GOARCH", "arm64")
		goos, goarch := targetPlatform(&packOptions{goos: "windows"})
		assert.Equal(t, "windows", goos, "goos from flag")
		assert.Equal(t, "arm64", goarch, "goarch from env")
	})
}

// When the --output parent directory does not exist, the packer must create it
// instead of failing with an opaque temp-file error.
func TestWriteBundle_CreatesMissingOutputDir(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "input-bin")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))

	output := filepath.Join(dir, "bin", "nested", "bundle")
	require.NoError(t, writeBundle(exe, output, []byte("source"), []byte("manifest")))

	info, err := os.Stat(output)
	require.NoError(t, err, "bundle should be written into the created directory")
	assert.False(t, info.IsDir())
}
