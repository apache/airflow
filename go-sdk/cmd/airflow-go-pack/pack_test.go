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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
	"github.com/apache/airflow/go-sdk/pkg/execution"
)

func testSDK() airflowmetadata.Manifest {
	return airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: "1.0",
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 "0.1.0",
			SupervisorSchemaVersion: "2026-06-16",
		},
	}
}

func TestRenderManifest(t *testing.T) {
	got1, err := renderManifest(testSDK(), "main.go", nil)
	require.NoError(t, err)
	got2, err := renderManifest(testSDK(), "main.go", nil)
	require.NoError(t, err)

	assert.Equal(t, got1, got2, "manifest should be byte-identical for identical input")
	assert.Equal(t, `airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
`, string(got1))
}

// Values (source, SDK fields) are quoted so a scalar-looking value stays a string; keys stay plain.
func TestRenderManifest_QuotesValuesNotKeys(t *testing.T) {
	meta := testSDK()
	meta.SDK.Version = "1.0"

	got, err := renderManifest(meta, "true", nil)
	require.NoError(t, err)

	assert.Contains(t, string(got), "\n  version: \"1.0\"\n")
	assert.Contains(t, string(got), "\nsource: \"true\"\n")
	assert.NotContains(t, string(got), `"sdk"`)
}

func TestRenderManifest_Digests(t *testing.T) {
	got, err := renderManifest(
		testSDK(),
		"main.go",
		&bundleDigests{Integrity: "aa11", Cache: "bb22"},
	)
	require.NoError(t, err)

	assert.Equal(t, `airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
digests:
  integrity: "aa11"
  cache: "bb22"
`, string(got))
}

// The manifest is rendered without the packer running the binary: a Dag inventory is not part of it.
func TestRenderManifest_HasNoDagInventory(t *testing.T) {
	got, err := renderManifest(testSDK(), "main.go", nil)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, yaml.Unmarshal(got, &parsed))
	assert.NotContains(t, parsed, "dags")
	assert.NotContains(t, parsed, "task_handlers")
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

// A file that is not a Go binary cannot say which go-sdk it was built against, so it is refused
// before anything is written.
func TestRunPack_RejectsExecutableWithoutBuildInformation(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "not-go")
	require.NoError(t, os.WriteFile(exe, []byte("not a Go binary\n"), 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	out := filepath.Join(dir, "bundle")

	err := runPack(
		io.Discard,
		io.Discard,
		&packOptions{executable: exe, source: source, output: out},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading the build information of "+exe)
	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "no bundle should be written")
}

// The packer never runs the binary: the fixture exits with status 3 when it runs.
func TestRunPack_DoesNotRunTheBinary(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "bundle-bin")
	exeBytes := readFile(t, bundleBinary(t))
	require.NoError(t, os.WriteFile(exe, exeBytes, 0o755))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0o644))
	out := filepath.Join(dir, "bundle")

	require.NoError(t, runPack(io.Discard, io.Discard, &packOptions{
		executable: exe,
		source:     source,
		output:     out,
	}))

	gotSource, gotMeta, err := bundlefooter.Read(out)
	require.NoError(t, err)
	assert.Equal(t, readFile(t, source), gotSource)
	assert.Contains(
		t,
		string(gotMeta),
		`supervisor_schema_version: "`+execution.SupervisorSchemaVersion+`"`,
	)
	bundleBytes := readFile(t, out)
	binaryRegion := bundleBytes[:len(bundleBytes)-len(gotSource)-len(gotMeta)-bundlefooter.TrailerSize]
	assert.Equal(t, exeBytes, binaryRegion, "the supplied --executable must be packed verbatim")
}

// --airflow-metadata is gone: the packer takes no manifest from the user.
func TestRootCmd_RejectsAirflowMetadataFlag(t *testing.T) {
	cmd := newRootCmd()
	cmd.RunE = func(*cobra.Command, []string) error { return nil }
	cmd.SetArgs(
		[]string{"--executable", "bin", "--source", "main.go", "--airflow-metadata", "meta.yaml"},
	)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown flag: --airflow-metadata")
}

// fixedManifest is a writeBundle renderMetadata that ignores the staged executable.
func fixedManifest(manifest []byte) func(string) ([]byte, error) {
	return func(string) ([]byte, error) { return manifest, nil }
}

// packDigests packs the given executable and source in dir, and returns the digests the packed
// bundle's manifest records.
func packDigests(t *testing.T, dir string, exe, source []byte) (integrity, cache string) {
	t.Helper()
	exePath := filepath.Join(dir, "exe")
	require.NoError(t, os.WriteFile(exePath, exe, 0o755))
	sourcePath := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(sourcePath, source, 0o644))
	out := filepath.Join(dir, "bundle")

	require.NoError(t, runPack(io.Discard, io.Discard, &packOptions{
		executable: exePath,
		source:     sourcePath,
		output:     out,
	}))

	_, gotMeta, err := bundlefooter.Read(out)
	require.NoError(t, err)
	var parsed struct {
		Digests struct {
			Integrity string `yaml:"integrity"`
			Cache     string `yaml:"cache"`
		} `yaml:"digests"`
	}
	require.NoError(t, yaml.Unmarshal(gotMeta, &parsed))
	return parsed.Digests.Integrity, parsed.Digests.Cache
}

func TestRunPack_RecordsDigests(t *testing.T) {
	exe := readFile(t, bundleBinary(t))
	source := []byte("package main\nfunc main() {}\n")

	integrity, cache := packDigests(t, t.TempDir(), exe, source)

	binaryHash := sha256.Sum256(exe)
	assert.Equal(
		t,
		hex.EncodeToString(binaryHash[:]),
		integrity,
		"integrity is the trailer's binary_sha256",
	)
	assert.Len(t, cache, sha256.Size*2)
	assert.NotEqual(t, integrity, cache)

	t.Run("a repack of the same inputs", func(t *testing.T) {
		gotIntegrity, gotCache := packDigests(t, t.TempDir(), exe, source)
		assert.Equal(t, integrity, gotIntegrity)
		assert.Equal(t, cache, gotCache)
	})

	oneByte := func(b []byte, i int) []byte {
		changed := bytes.Clone(b)
		changed[i] ^= 1
		return changed
	}
	for name, tc := range map[string]struct {
		exe, source      []byte
		integrityChanges bool
	}{
		"one source byte": {exe: exe, source: oneByte(source, len(source)-2)},
		// A byte after the binary leaves its build information readable.
		"one binary byte": {exe: append(bytes.Clone(exe), 0), source: source, integrityChanges: true},
	} {
		t.Run(name, func(t *testing.T) {
			gotIntegrity, gotCache := packDigests(t, t.TempDir(), tc.exe, tc.source)
			assert.NotEqual(t, cache, gotCache)
			if tc.integrityChanges {
				assert.NotEqual(t, integrity, gotIntegrity)
			} else {
				assert.Equal(t, integrity, gotIntegrity)
			}
		})
	}
}

func TestComputeDigests_CoverTheManifest(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "exe")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))

	first, err := computeDigests(exe, []byte("source"), []byte("manifest"))
	require.NoError(t, err)
	changed, err := computeDigests(exe, []byte("source"), []byte("manifest 2"))
	require.NoError(t, err)

	assert.Equal(t, first.Integrity, changed.Integrity)
	assert.NotEqual(t, first.Cache, changed.Cache)
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

func TestWriteBundle_RendersMetadataFromTheStagedCopy(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "input-bin")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))

	var staged string
	var stagedBytes []byte
	require.NoError(t, writeBundle(exe, filepath.Join(dir, "bundle"), []byte("source"),
		func(path string) ([]byte, error) {
			staged = path
			var err error
			stagedBytes, err = os.ReadFile(path)
			return []byte("manifest"), err
		},
	))

	assert.NotEqual(t, exe, staged)
	assert.Equal(t, []byte("binary-bytes"), stagedBytes)
}

// When the --output parent directory does not exist, the packer must create it
// instead of failing with an opaque temp-file error.
func TestWriteBundle_CreatesMissingOutputDir(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "input-bin")
	require.NoError(t, os.WriteFile(exe, []byte("binary-bytes"), 0o755))

	output := filepath.Join(dir, "bin", "nested", "bundle")
	require.NoError(
		t,
		writeBundle(exe, output, []byte("source"), fixedManifest([]byte("manifest"))),
	)

	info, err := os.Stat(output)
	require.NoError(t, err, "bundle should be written into the created directory")
	assert.False(t, info.IsDir())
}
