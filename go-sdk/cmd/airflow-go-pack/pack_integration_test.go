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
	"regexp"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestPack_CrossArchExecutable is the end-to-end cross-architecture test for
// the --executable path: a binary built for a CPU arch the host cannot run is
// still packed into a spec-conforming bundle, with its (foreign-arch) binary
// region preserved byte-for-byte and a correct manifest.
//
// The metadata for a foreign-arch --executable is obtained by the host
// fallback build (the binary cannot be exec'd here). On a host with
// qemu/binfmt the foreign binary may actually run, in which case no fallback
// happens — either way the resulting bundle must be identical, so this test
// asserts on the bundle, not on which path produced the manifest.
func TestPack_CrossArchExecutable(t *testing.T) {
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

	// Pack the foreign-arch executable through the real CLI command.
	outPath := filepath.Join(tmp, "bundle")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--executable", crossBin, "--source", sourceFile, "--output", outPath})
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
  language: go
  version: "` + sdkVersion + `"
  supervisor_schema_version: "2026-06-16"
source: main.go
dags:
  simple_dag:
    tasks:
      - extract
      - transform
      - load
`
	assert.Equal(t, expectedManifest, string(metadata))

	// The packed binary region must be exactly the foreign-arch executable:
	// the host fallback build is used only to read metadata, never packed.
	binaryRegion := bundleBytes[:len(bundleBytes)-len(source)-len(metadata)-bundlefooter.TrailerSize]
	assert.Equal(t, crossBytes, binaryRegion,
		"packed binary region must be the foreign-arch --executable, not a host rebuild")
}

func goBuild(t *testing.T, pkgDir, out, goos, goarch string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", out, pkgDir)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s for %s/%s failed: %v\n%s", pkgDir, goos, goarch, err, combined)
	}
}
