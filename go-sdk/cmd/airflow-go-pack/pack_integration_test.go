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
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
	"github.com/apache/airflow/go-sdk/pkg/execution"
)

// packBundleFixture packs the fixture bundle package through the real CLI command, as a user would, and
// returns the bundle's source, manifest and bytes. It reports the packer's output on failure.
func packBundleFixture(t *testing.T, args ...string) (source, manifest, bundleBytes []byte) {
	t.Helper()
	outPath := filepath.Join(t.TempDir(), "bundle")
	var stderr bytes.Buffer
	cmd := newRootCmd()
	cmd.SetArgs(append([]string{"--output", outPath}, args...))
	cmd.SetOut(&stderr)
	cmd.SetErr(&stderr)
	require.NoError(t, cmd.Execute(), stderr.String())

	// Read parses the trailer and verifies binary_sha256 over the binary region; success means
	// the bundle is spec-valid.
	source, manifest, err := bundlefooter.Read(outPath)
	require.NoError(t, err)
	return source, manifest, readFile(t, outPath)
}

// The manifest the packer writes for a binary it did not run: the SDK block comes from the
// binary's build information and the packer's own go-sdk, and there is no Dag inventory.
//
// sdk.version is environment-dependent and not asserted verbatim: a plain `go build` from a local
// module tree leaves Main.Version unset and yields "(devel)", while Go 1.24's VCS stamping (e.g. in
// CI, building from the git checkout) yields a pseudo-version like v0.0.0-<timestamp>-<commit>, and
// a tagged-release build yields a semver tag. The test asserts the version matches an accepted
// form, then folds the observed value into the expected manifest so the remaining fields and
// ordering are checked exactly.
func assertManifestWithoutDags(t *testing.T, manifest, binaryRegion []byte, source string) {
	t.Helper()
	versionLine := regexp.MustCompile(`(?m)^  version: "([^"]*)"$`)
	m := versionLine.FindStringSubmatch(string(manifest))
	require.NotNil(t, m, "manifest must contain an sdk.version line:\n%s", manifest)
	assert.Regexp(t, `^(\(devel\)|v[0-9].*)$`, m[1],
		`sdk.version must be "(devel)" or a v-prefixed module version`)
	// The cache digest covers the environment-dependent binary and sdk.version too.
	cacheLine := regexp.MustCompile(`(?m)^  cache: "([0-9a-f]{64})"$`)
	c := cacheLine.FindStringSubmatch(string(manifest))
	require.NotNil(t, c, "manifest must contain a digests.cache line:\n%s", manifest)
	binaryHash := sha256.Sum256(binaryRegion)

	assert.Equal(t, `airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "`+m[1]+`"
  supervisor_schema_version: "`+execution.SupervisorSchemaVersion+`"
source: "`+source+`"
digests:
  integrity: "`+hex.EncodeToString(binaryHash[:])+`"
  cache: "`+c[1]+`"
`, string(manifest))
}

func binaryRegionOf(bundleBytes, source, manifest []byte) []byte {
	return bundleBytes[:len(bundleBytes)-len(source)-len(manifest)-bundlefooter.TrailerSize]
}

// Packing a package whose binary exits non-zero succeeds, so the packer does not run it.
func TestPack_BuildsWithoutRunningTheBundle(t *testing.T) {
	requireGo(t)

	source, manifest, bundleBytes := packBundleFixture(t, "./testdata/failingbundle")

	srcBytes := readFile(t, filepath.Join("testdata", "failingbundle", "main.go"))
	assert.Equal(t, srcBytes, source, "embedded source must be the file with func main")
	assertManifestWithoutDags(t, manifest, binaryRegionOf(bundleBytes, source, manifest), "main.go")
}

// A cross-arch build-mode pack needs no host build of the bundle: the artefact is the target-arch
// build with the forwarded flags, and the host-arch binary the packer used to run is gone.
func TestPack_CrossCompileBuildModeForwardsFlags(t *testing.T) {
	crossArch := requireCrossArch(t)
	// Cross-compile via the environment, exactly as a user would. CGO is disabled so the cross
	// build needs no C toolchain.
	t.Setenv("GOOS", runtime.GOOS)
	t.Setenv("GOARCH", crossArch)
	t.Setenv("CGO_ENABLED", "0")

	source, manifest, bundleBytes := packBundleFixture(
		t,
		"./testdata/failingbundle",
		"--",
		"-trimpath",
	)

	// Independently build the target-arch artefact with the same forwarded flag; the packed binary
	// region must match it byte-for-byte, proving the deployable artefact is the cross build and
	// that -trimpath was forwarded.
	wantBin := filepath.Join(t.TempDir(), "want_cross")
	build := exec.Command("go", "build", "-trimpath", "-o", wantBin, "./testdata/failingbundle")
	build.Env = append(os.Environ(), "GOOS="+runtime.GOOS, "GOARCH="+crossArch, "CGO_ENABLED=0")
	if combined, berr := build.CombinedOutput(); berr != nil {
		t.Fatalf("reference cross build failed: %v\n%s", berr, combined)
	}
	assert.Equal(t, readFile(t, wantBin), binaryRegionOf(bundleBytes, source, manifest),
		"packed binary must be the cross-built artefact with -trimpath forwarded")
	assertManifestWithoutDags(t, manifest, readFile(t, wantBin), "main.go")
}

// A cross-arch --executable pack needs no manifest and no host build: a binary built for an
// architecture the host cannot run is packed with its binary region preserved byte-for-byte, and
// the SDK version is read from the binary.
func TestPack_CrossArchExecutable(t *testing.T) {
	crossArch := requireCrossArch(t)
	crossBin := filepath.Join(t.TempDir(), "prebuilt_cross")
	goBuild(t, "./testdata/failingbundle", crossBin, runtime.GOOS, crossArch)
	require.NotEqual(t, readFile(t, bundleBinary(t)), readFile(t, crossBin),
		"cross and host builds should differ; cross-compile may not have taken effect")
	sourceFile := filepath.Join("testdata", "failingbundle", "main.go")

	source, manifest, bundleBytes := packBundleFixture(
		t,
		"--executable",
		crossBin,
		"--source",
		sourceFile,
	)

	assert.Equal(t, readFile(t, sourceFile), source, "embedded source must match --source bytes")
	binaryRegion := binaryRegionOf(bundleBytes, source, manifest)
	assert.Equal(t, readFile(t, crossBin), binaryRegion,
		"packed binary region must be the foreign-arch --executable, not a rebuild")
	assertManifestWithoutDags(t, manifest, binaryRegion, "main.go")
}

// Build information survives a stripped binary, so the version can still be read.
func TestPack_StrippedBinary(t *testing.T) {
	requireGo(t)

	source, manifest, bundleBytes := packBundleFixture(
		t, "./testdata/failingbundle", "--", "-ldflags=-s -w",
	)

	// The packed binary must be the stripped build, so the flag reached `go build` and the
	// version was read from a binary without a symbol table.
	wantBin := filepath.Join(t.TempDir(), "want_stripped")
	build := exec.Command(
		"go", "build", "-ldflags=-s -w", "-o", wantBin, "./testdata/failingbundle",
	)
	if combined, berr := build.CombinedOutput(); berr != nil {
		t.Fatalf("reference stripped build failed: %v\n%s", berr, combined)
	}
	binaryRegion := binaryRegionOf(bundleBytes, source, manifest)
	assert.Equal(t, readFile(t, wantBin), binaryRegion,
		"packed binary must be the stripped build with -ldflags forwarded")
	assert.NotEqual(t, readFile(t, bundleBinary(t)), binaryRegion,
		"packed binary must differ from the unstripped build")
	assertManifestWithoutDags(t, manifest, binaryRegion, "main.go")
}

// A Go binary that does not link go-sdk is not a bundle.
func TestRunPack_RejectsBinaryWithoutSDK(t *testing.T) {
	requireGo(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"),
		[]byte("module nosdk\n\ngo 1.24\n"), 0o644))
	source := filepath.Join(dir, "main.go")
	require.NoError(t, os.WriteFile(source, []byte("package main\n\nfunc main() {}\n"), 0o644))
	exe := filepath.Join(dir, "nosdk")
	build := exec.Command("go", "build", "-o", exe, ".")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the binary failed: %v\n%s", err, out)
	}
	out := filepath.Join(dir, "bundle")

	err := runPack(
		io.Discard,
		io.Discard,
		&packOptions{executable: exe, source: source, output: out},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not link github.com/apache/airflow/go-sdk")
	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "no bundle should be written")
}

// writeBundleModule writes a module that requires go-sdk at sdkVersion, resolved to this checkout
// of go-sdk through a replace directive, with airflow-go-pack as a tool and a bundle package that
// links go-sdk. The module copies the SDK's requirements and checksums, so it builds offline.
func writeBundleModule(t *testing.T, dir, name, sdkVersion string) {
	t.Helper()
	sdkDir, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	goMod := strings.Replace(
		string(readFile(t, filepath.Join(sdkDir, "go.mod"))),
		"module "+testSDKModule, "module example.com/"+name, 1,
	)
	goMod += "\nrequire " + testSDKModule + " " + sdkVersion + "\n\nreplace " + testSDKModule + " => " +
		filepath.ToSlash(
			sdkDir,
		) + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644))
	goSum := readFile(t, filepath.Join(sdkDir, "go.sum"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), goSum, 0o644))
	mainGo := readFile(t, filepath.Join("testdata", "failingbundle", "main.go"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.go"), mainGo, 0o644))
}

// goInModule runs go in dir without network access and returns its combined output.
func goInModule(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// The packer and the bundle binary must be built against the same go-sdk. This runs the packer as
// a user does, with `go tool` from modules that resolve go-sdk through a replace directive.
func TestPack_TwoModules(t *testing.T) {
	requireGo(t)
	packerModule := t.TempDir()
	writeBundleModule(t, packerModule, "packer", "v0.0.0")

	t.Run(
		"a module that replaces go-sdk with a local path packs with its own go tool",
		func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "bundle")

			output, err := goInModule(
				t,
				packerModule,
				"tool",
				"airflow-go-pack",
				"--output",
				out,
				".",
			)

			require.NoError(t, err, output)
			assert.Contains(t, output, "Wrote bundle "+out+" (sdk=go/(devel))\n")
			_, manifest, err := bundlefooter.Read(out)
			require.NoError(t, err)
			// A replacement by a local path has the version "(devel)", which is what the manifest records.
			assert.Contains(t, string(manifest), `  version: "(devel)"`)
			assert.Contains(
				t,
				string(manifest),
				`  supervisor_schema_version: "`+execution.SupervisorSchemaVersion+`"`,
			)
		},
	)

	t.Run("a binary built against another go-sdk is refused", func(t *testing.T) {
		otherModule := t.TempDir()
		writeBundleModule(t, otherModule, "other", "v0.0.1")
		binary := filepath.Join(otherModule, "other")
		output, err := goInModule(t, otherModule, "build", "-o", binary, ".")
		require.NoError(t, err, output)
		out := filepath.Join(t.TempDir(), "bundle")

		output, err = goInModule(
			t,
			packerModule,
			"tool",
			"airflow-go-pack",
			"--executable",
			binary,
			"--source",
			filepath.Join(otherModule, "main.go"),
			"--output",
			out,
		)

		require.Error(t, err)
		assert.Contains(t, output, "built against go-sdk v0.0.1 replaced by ")
		assert.Contains(t, output, "airflow-go-pack was built against go-sdk v0.0.0 replaced by ")
		assert.Contains(t, output, "go tool airflow-go-pack")
		_, statErr := os.Stat(out)
		assert.True(t, os.IsNotExist(statErr), "no bundle should be written")
	})

	t.Run("a binary built in go-sdk is refused by a replacing packer", func(t *testing.T) {
		// go-sdk is the main module of the binary and a replaced dependency of the packer, so
		// the comparison runs. -buildvcs=false keeps the main module's version "(devel)",
		// whatever the checkout's version control says.
		sdkDir, err := filepath.Abs(filepath.Join("..", ".."))
		require.NoError(t, err)
		fixtureDir := filepath.Join(sdkDir, "cmd", "airflow-go-pack", "testdata", "failingbundle")
		binary := filepath.Join(t.TempDir(), "inside")
		output, err := goInModule(t, sdkDir, "build", "-buildvcs=false", "-o", binary, fixtureDir)
		require.NoError(t, err, output)
		out := filepath.Join(t.TempDir(), "bundle")

		output, err = goInModule(
			t,
			packerModule,
			"tool",
			"airflow-go-pack",
			"--executable",
			binary,
			"--source",
			filepath.Join(fixtureDir, "main.go"),
			"--output",
			out,
		)

		require.Error(t, err)
		assert.Contains(t, output, "built against go-sdk (devel) (main module)")
		assert.Contains(t, output, "airflow-go-pack was built against go-sdk v0.0.0 replaced by ")
		_, statErr := os.Stat(out)
		assert.True(t, os.IsNotExist(statErr), "no bundle should be written")
	})

	t.Run("a build flag that changes the module graph is refused", func(t *testing.T) {
		// -modfile makes the build resolve go-sdk through another go.mod, as a user's flag after
		// "--" could.
		other := strings.Replace(
			string(readFile(t, filepath.Join(packerModule, "go.mod"))),
			"require "+testSDKModule+" v0.0.0", "require "+testSDKModule+" v0.0.1", 1,
		)
		require.NoError(
			t,
			os.WriteFile(filepath.Join(packerModule, "other.mod"), []byte(other), 0o644),
		)
		require.NoError(t, os.WriteFile(
			filepath.Join(
				packerModule,
				"other.sum",
			),
			readFile(t, filepath.Join(packerModule, "go.sum")),
			0o644,
		))
		out := filepath.Join(t.TempDir(), "bundle")

		output, err := goInModule(
			t,
			packerModule,
			"tool",
			"airflow-go-pack",
			"--output",
			out,
			".",
			"--",
			"-modfile=other.mod",
		)

		require.Error(t, err)
		assert.Contains(t, output, "built against go-sdk v0.0.1 replaced by ")
		assert.Contains(t, output, "airflow-go-pack was built against go-sdk v0.0.0 replaced by ")
		_, statErr := os.Stat(out)
		assert.True(t, os.IsNotExist(statErr), "no bundle should be written")
	})
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
