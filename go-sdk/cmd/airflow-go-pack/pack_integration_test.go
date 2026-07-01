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
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

// crossArchFor returns an architecture different from the host that the Go
// toolchain can target, or "" if we have no safe mapping for this host.
func crossArchFor(hostArch string) string {
	switch hostArch {
	case "amd64":
		return "arm64"
	case "arm64":
		return "amd64"
	default:
		return ""
	}
}

// End-to-end cross-arch --executable test: a binary built for an arch the host
// cannot run is packed into a spec-conforming bundle with its binary region
// preserved byte-for-byte. The caller supplies the artefact's own
// --airflow-metadata output (captured here from a host build of the same
// sources) rather than the packer rebuilding to guess the metadata.
func TestPack_CrossArchExecutableWithMetadataFile(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-arch pack test shells out to `go build` twice")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	crossArch := crossArchFor(runtime.GOARCH)
	if crossArch == "" {
		t.Skipf("no cross-arch mapping for host arch %q", runtime.GOARCH)
	}

	// The example bundle is a real BundleProvider that answers
	// --airflow-metadata, so it exercises the genuine metadata path.
	exampleDir, err := filepath.Abs(filepath.Join("..", "..", "example", "bundle"))
	require.NoError(t, err)
	sourceFile := filepath.Join(exampleDir, "main.go")
	if _, err := os.Stat(sourceFile); err != nil {
		t.Skipf("example bundle source not found: %v", err)
	}

	tmp := t.TempDir()
	crossBin := filepath.Join(tmp, "prebuilt_cross")
	hostBin := filepath.Join(tmp, "prebuilt_host")

	// Build the example for a foreign arch (the --executable input) and for
	// the host. CGO is disabled so the cross build needs no C toolchain.
	goBuild(t, exampleDir, crossBin, runtime.GOOS, crossArch)
	goBuild(t, exampleDir, hostBin, runtime.GOOS, runtime.GOARCH)

	crossBytes, err := os.ReadFile(crossBin)
	require.NoError(t, err)
	hostBytes, err := os.ReadFile(hostBin)
	require.NoError(t, err)
	require.False(t, bytes.Equal(crossBytes, hostBytes),
		"cross and host builds should differ; cross-compile may not have taken effect")

	// Capture the artefact's own --airflow-metadata JSON from the host build,
	// standing in for the author running the binary on its native platform.
	metaJSON := filepath.Join(tmp, "airflow-metadata.json")
	captureMetadata(t, hostBin, metaJSON)

	// Pack the foreign-arch executable through the real CLI command, feeding
	// the captured metadata so no host rebuild is needed.
	outPath := filepath.Join(tmp, "bundle")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--executable", crossBin,
		"--source", sourceFile,
		"--airflow-metadata", metaJSON,
		"--output", outPath,
	})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	require.NoError(t, cmd.Execute())

	bundleBytes, err := os.ReadFile(outPath)
	require.NoError(t, err)

	// Read parses the trailer and verifies binary_sha256 over the binary
	// region; success means the bundle is spec-valid.
	source, metadata, err := bundlefooter.Read(outPath)
	require.NoError(t, err)

	srcBytes, err := os.ReadFile(sourceFile)
	require.NoError(t, err)
	assert.Equal(t, srcBytes, source, "embedded source must match --source bytes")

	// sdk.version is environment-dependent and not asserted verbatim: a plain
	// `go build` from a local module tree leaves Main.Version unset and yields
	// "(devel)", while Go 1.24's VCS stamping (e.g. in CI, building from the
	// git checkout) yields a pseudo-version like v0.0.0-<timestamp>-<commit>,
	// and a tagged-release build yields a semver tag. Assert the version line
	// matches an accepted form, then fold the observed value into the expected
	// manifest so the remaining fields and ordering are checked exactly.
	// supervisor_schema_version and the format version come from SDK constants
	// and change only when those constants do.
	versionLine := regexp.MustCompile(`(?m)^  version: "([^"]*)"$`)
	m := versionLine.FindStringSubmatch(string(metadata))
	require.NotNil(t, m, "manifest must contain an sdk.version line:\n%s", metadata)
	sdkVersion := m[1]
	assert.Regexp(t, `^(\(devel\)|v[0-9].*)$`, sdkVersion,
		`sdk.version must be "(devel)" or a v-prefixed module version`)

	expectedManifest := `airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "` + sdkVersion + `"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
dags:
  concurrent_xcom_dag:
    tasks:
      - "pull_xcoms_concurrently"
  simple_dag:
    tasks:
      - "extract"
      - "transform"
      - "load"
`
	assert.Equal(t, expectedManifest, string(metadata))

	// The packed binary region must be exactly the foreign-arch executable:
	// the captured metadata describes it, and the binary is never rebuilt.
	binaryRegion := bundleBytes[:len(bundleBytes)-len(source)-len(metadata)-bundlefooter.TrailerSize]
	assert.Equal(t, crossBytes, binaryRegion,
		"packed binary region must be the foreign-arch --executable, not a host rebuild")
}

// End-to-end build-mode cross-compile (no --executable): with GOOS/GOARCH set,
// the packer builds the target-arch artefact and a host-arch binary (solely to
// read the manifest), forwarding the `--` go build flags. The packed binary
// must be the target-arch artefact built with the forwarded flags.
func TestPack_CrossCompileBuildModeForwardsFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-arch pack test shells out to `go build` twice")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	crossArch := crossArchFor(runtime.GOARCH)
	if crossArch == "" {
		t.Skipf("no cross-arch mapping for host arch %q", runtime.GOARCH)
	}

	exampleDir, err := filepath.Abs(filepath.Join("..", "..", "example", "bundle"))
	require.NoError(t, err)
	if _, err := os.Stat(filepath.Join(exampleDir, "main.go")); err != nil {
		t.Skipf("example bundle source not found: %v", err)
	}

	// Cross-compile via the environment, exactly as a user would. CGO is
	// disabled so the cross build needs no C toolchain.
	t.Setenv("GOOS", runtime.GOOS)
	t.Setenv("GOARCH", crossArch)
	t.Setenv("CGO_ENABLED", "0")

	tmp := t.TempDir()
	outPath := filepath.Join(tmp, "bundle")

	// Pack the example package (no --executable, no --source), forwarding
	// -trimpath after the "--" separator to the internal go build.
	cmd := newRootCmd()
	cmd.SetArgs([]string{exampleDir, "--output", outPath, "--", "-trimpath"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	require.NoError(t, cmd.Execute())

	source, metadata, err := bundlefooter.Read(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(metadata), "simple_dag:",
		"manifest must be read from the host introspection build")

	// Independently build the target-arch artefact with the same forwarded
	// flag; the packed binary region must match it byte-for-byte, proving the
	// deployable artefact is the cross build (not the host introspection one)
	// and that -trimpath was forwarded.
	wantBin := filepath.Join(tmp, "want_cross")
	build := exec.Command("go", "build", "-trimpath", "-o", wantBin, exampleDir)
	build.Env = append(os.Environ(), "GOOS="+runtime.GOOS, "GOARCH="+crossArch, "CGO_ENABLED=0")
	if combined, berr := build.CombinedOutput(); berr != nil {
		t.Fatalf("reference cross build failed: %v\n%s", berr, combined)
	}
	wantBytes, err := os.ReadFile(wantBin)
	require.NoError(t, err)

	bundleBytes, err := os.ReadFile(outPath)
	require.NoError(t, err)
	binaryRegion := bundleBytes[:len(bundleBytes)-len(source)-len(metadata)-bundlefooter.TrailerSize]
	assert.Equal(t, wantBytes, binaryRegion,
		"packed binary must be the cross-built artefact with -trimpath forwarded")
}

// When --source is supplied, the packer skips source discovery, so a package
// whose main file cannot be auto-detected (two files with func main, which
// discovery rejects as ambiguous) is still packable. The failure is the
// downstream `go build` error, not the discovery error — proving discovery was
// skipped.
func TestRunPack_SourceBypassesDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to the go toolchain")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	// Two files both define func main(), which discoverMainSource rejects as
	// ambiguous. Building them together is also a compile error, so reaching
	// `go build` (rather than failing in discovery) is the observable signal
	// that --source bypassed discovery.
	pkgDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(pkgDir, "go.mod"),
		[]byte("module ambiguousmain\n\ngo 1.24\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pkgDir, "a.go"),
		[]byte("package main\n\nfunc main() {}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pkgDir, "b.go"),
		[]byte("package main\n\nfunc main() {}\n"), 0o644))

	source := filepath.Join(pkgDir, "a.go")
	err := runPack(io.Discard, io.Discard, &packOptions{
		pkg:    pkgDir,
		source: source,
		output: filepath.Join(t.TempDir(), "bundle"),
	})
	require.Error(t, err)
	// With --source honoured, discovery is skipped; the build still runs and
	// fails on the duplicate main.
	assert.NotContains(t, err.Error(), "locating DAG source file",
		"--source must bypass source discovery")
	assert.Contains(t, err.Error(), "go build failed",
		"packing should proceed to the build step when --source is given")
}

// Checks the bundle binary's --airflow-metadata encodings: default is YAML,
// --format json emits JSON, both decode to the same manifest, and --format
// without --airflow-metadata is a hard error.
func TestBundleBinary_AirflowMetadataFormats(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	exampleDir, err := filepath.Abs(filepath.Join("..", "..", "example", "bundle"))
	require.NoError(t, err)
	if _, err := os.Stat(filepath.Join(exampleDir, "main.go")); err != nil {
		t.Skipf("example bundle source not found: %v", err)
	}

	hostBin := filepath.Join(t.TempDir(), "bundle")
	goBuild(t, exampleDir, hostBin, runtime.GOOS, runtime.GOARCH)

	assertManifest := func(t *testing.T, meta airflowmetadata.Manifest) {
		t.Helper()
		assert.Equal(t, "go", meta.SDK.Language)
		require.Contains(t, meta.Dags, "simple_dag")
		assert.Equal(t, []string{"extract", "transform", "load"}, meta.Dags["simple_dag"].Tasks)
	}

	// Default: YAML. A JSON document would start with '{'.
	yamlOut, err := exec.Command(hostBin, "--airflow-metadata").Output()
	require.NoError(t, err, "running %s --airflow-metadata", hostBin)
	assert.False(t, bytes.HasPrefix(bytes.TrimSpace(yamlOut), []byte("{")),
		"default --airflow-metadata must emit YAML, not JSON")
	assert.Contains(t, string(yamlOut), "airflow_bundle_metadata_version:")
	var fromYAML airflowmetadata.Manifest
	require.NoError(t, yaml.Unmarshal(yamlOut, &fromYAML))
	assertManifest(t, fromYAML)

	// --format json: JSON output.
	jsonOut, err := exec.Command(hostBin, "--airflow-metadata", "--format", "json").Output()
	require.NoError(t, err, "running %s --airflow-metadata --format json", hostBin)
	assert.True(t, bytes.HasPrefix(bytes.TrimSpace(jsonOut), []byte("{")),
		"--format json must emit JSON")
	var fromJSON airflowmetadata.Manifest
	require.NoError(t, json.Unmarshal(jsonOut, &fromJSON))
	assertManifest(t, fromJSON)

	assert.Equal(t, fromYAML, fromJSON, "both encodings must decode to the same manifest")

	// --format without --airflow-metadata is a usage error.
	bad, err := exec.Command(hostBin, "--format", "json").CombinedOutput()
	require.Error(t, err, "--format without --airflow-metadata must exit non-zero")
	assert.Contains(t, string(bad), "--format is only valid together with --airflow-metadata")
}

// Running the packer from a directory that is not a bundle main package must
// turn the bare `go list` failure into an actionable error pointing at a
// package path or --source.
func TestDiscoverMainSource_NoGoFilesGivesGuidance(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"),
		[]byte("module testempty\n\ngo 1.24\n"), 0o644))
	t.Chdir(dir)

	_, err := discoverMainSource(".")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "airflow-go-pack ./path/to/bundle",
		"discovery failure should point the user at a package path")
	assert.Contains(t, err.Error(), "--source")
}

func goBuild(t *testing.T, pkgDir, out, goos, goarch string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", out, pkgDir)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s for %s/%s failed: %v\n%s", pkgDir, goos, goarch, err, combined)
	}
}

// captureMetadata runs a host-runnable bundle binary with --airflow-metadata
// and writes its JSON stdout to outPath.
func captureMetadata(t *testing.T, hostBin, outPath string) {
	t.Helper()
	cmd := exec.Command(hostBin, "--airflow-metadata")
	out, err := cmd.Output()
	require.NoError(t, err, "running %s --airflow-metadata", hostBin)
	require.NoError(t, os.WriteFile(outPath, out, 0o644))
}
