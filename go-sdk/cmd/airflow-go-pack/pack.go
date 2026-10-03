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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

// packOptions are the flags accepted by the root pack command.
type packOptions struct {
	pkg        string   // target package (default ".")
	source     string   // override the auto-detected DAG source file
	executable string   // pack a pre-built binary instead of building
	output     string   // override the default <bundleName> output path
	goos       string   // target GOOS for the deployable build (falls back to env GOOS, then host)
	goarch     string   // target GOARCH for the deployable build (falls back to env GOARCH, then host)
	buildArgs  []string // forwarded verbatim to `go build` (already includes the leading "--")
}

func runPack(stdout, stderr io.Writer, opts *packOptions) error {
	if opts.executable != "" && len(opts.buildArgs) > 0 {
		return fmt.Errorf("--executable is mutually exclusive with go build flags after \"--\"")
	}
	if opts.executable != "" && (opts.goos != "" || opts.goarch != "") {
		return fmt.Errorf(
			"--executable is mutually exclusive with --goos/--goarch: --executable packs the " +
				"binary as-is and never builds, so it cannot cross-compile. To cross-build a " +
				"bundle, drop --executable and pass --goos/--goarch with the package path",
		)
	}

	// Resolve the DAG source file for both modes up front. --executable requires
	// it explicitly; the build path falls back to discovery.
	sourcePath := opts.source
	if opts.executable != "" {
		if sourcePath == "" {
			return fmt.Errorf(
				"--executable requires --source: cannot infer the DAG source for a pre-built binary",
			)
		}
	} else if sourcePath == "" {
		// --source is the documented escape hatch for packages whose main file
		// cannot be auto-detected: it may be selected by build tags or GOOS, or
		// the package may have several files with func main(). Discovery runs a
		// plain `go list` without the forwarded build flags, so it can fail or
		// pick the wrong file for such packages. Only fall back to discovery
		// when --source was not supplied, so an explicit --source always wins.
		discovered, err := discoverMainSource(opts.pkg)
		if err != nil {
			return fmt.Errorf("locating DAG source file: %w", err)
		}
		sourcePath = discovered
	}
	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source file %s: %w", sourcePath, err)
	}

	output := opts.output
	if output == "" {
		defaultPath, err := defaultOutputPath(sourcePath)
		if err != nil {
			return fmt.Errorf("determining default output path: %w", err)
		}
		output = defaultPath
	}

	// The bundle is finalised by renaming a temp file onto output, which fails
	// if output is an existing directory. Catch that here: the default output is
	// the package directory's name, so packing ./foo from a dir that already has
	// a ./foo directory collides. Report it with the fix instead of a bare
	// "rename ...: file exists" from os.Rename. Done before any build so a
	// misconfigured output fails fast rather than after a (cross) go build.
	if info, err := os.Stat(output); err == nil && info.IsDir() {
		return fmt.Errorf(
			"output path %q is an existing directory (the bundle output defaults to the "+
				"package directory's name); pass --output to write the bundle to a file path",
			output,
		)
	}

	// execPath is the binary that receives the footer: the deployable artefact, which may be
	// cross-compiled. The packer never runs it.
	var execPath string
	cleanupExec := func() {}
	defer func() { cleanupExec() }()

	if opts.executable != "" {
		execPath = opts.executable
	} else {
		targetGOOS, targetGOARCH := targetPlatform(opts)
		artifact, cleanup, err := buildPackage(stderr, opts.pkg, opts.buildArgs, targetGOOS, targetGOARCH)
		if err != nil {
			return err
		}
		execPath = artifact
		cleanupExec = cleanup
	}

	if _, err := os.Stat(execPath); err != nil {
		return fmt.Errorf("executable %s: %w", execPath, err)
	}

	if err := rejectOutputAlias(output, execPath, sourcePath); err != nil {
		return err
	}

	sdk, err := readSDK(execPath)
	if err != nil {
		return err
	}
	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: airflowmetadata.FormatVersion,
		SDK:                          sdk,
	}

	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("reading source file: %w", err)
	}
	// The digests are taken over the staged copy, the file the trailer's
	// binary_sha256 is computed from, so the two agree even if the executable
	// changes while packing.
	renderWithDigests := func(stagedExec string) ([]byte, error) {
		baseManifest, err := renderManifest(meta, filepath.Base(sourcePath), nil)
		if err != nil {
			return nil, fmt.Errorf("rendering manifest: %w", err)
		}
		digests, err := computeDigests(stagedExec, sourceBytes, baseManifest)
		if err != nil {
			return nil, err
		}
		manifest, err := renderManifest(meta, filepath.Base(sourcePath), &digests)
		if err != nil {
			return nil, fmt.Errorf("rendering manifest: %w", err)
		}
		return manifest, nil
	}

	// Assemble the bundle through a temp file and atomically move it into
	// place: we never mutate the build artefact or the user-supplied
	// --executable, and a failed pack never leaves a truncated or half-written
	// file at output.
	if err := writeBundle(execPath, output, sourceBytes, renderWithDigests); err != nil {
		return err
	}

	fmt.Fprintf(
		stdout,
		"Wrote bundle %s (sdk=%s/%s)\n",
		output,
		meta.SDK.Language,
		meta.SDK.Version,
	)
	return nil
}

// defaultOutputPath derives the default bundle output path from the directory
// that owns the DAG source file. That directory is the bundle's main package,
// so its base name is what `go build` itself would name the binary. On Windows
// the .exe suffix is appended.
func defaultOutputPath(sourcePath string) (string, error) {
	abs, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", err
	}
	name := filepath.Base(filepath.Dir(abs))
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return name, nil
}

// discoverMainSource locates the file in the given package whose AST contains
// a top-level `func main()`. Returns an error if the package has zero or
// more than one such file, mirroring ADR 0002's discovery contract.
func discoverMainSource(pkg string) (string, error) {
	cmd := exec.Command("go", "list", "-f", "{{.Dir}}\n{{range .GoFiles}}{{.}}\n{{end}}", pkg)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf(
			"go list %s: %w: %s\n"+
				"airflow-go-pack packs the Go package in the current directory by default; "+
				"pass your bundle's package path (e.g. `airflow-go-pack ./path/to/bundle`) "+
				"or --source to point at the DAG source file directly",
			pkg, err, strings.TrimSpace(stderr.String()),
		)
	}

	lines := splitNonEmpty(stdout.String())
	if len(lines) < 2 {
		return "", fmt.Errorf("package %s has no Go source files", pkg)
	}
	dir := lines[0]
	files := lines[1:]

	fset := token.NewFileSet()
	var matches []string
	for _, name := range files {
		full := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, full, nil, parser.SkipObjectResolution)
		if err != nil {
			return "", fmt.Errorf("parsing %s: %w", full, err)
		}
		if hasMainFunc(f) {
			matches = append(matches, full)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no file in package %s defines func main()", pkg)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf(
			"multiple files in package %s define func main(): %v; use --source to disambiguate",
			pkg,
			matches,
		)
	}
}

func hasMainFunc(f *ast.File) bool {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Recv != nil {
			continue
		}
		if fn.Name.Name != "main" {
			continue
		}
		if fn.Type.Params != nil && len(fn.Type.Params.List) != 0 {
			continue
		}
		return true
	}
	return false
}

func splitNonEmpty(s string) []string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// targetPlatform resolves the GOOS/GOARCH the deployable bundle is built for:
// the --goos/--goarch flags win, then the ambient GOOS/GOARCH env, then the
// host. The flags exist because `go tool airflow-go-pack` builds the packer
// using the ambient GOOS/GOARCH — setting those in the env to cross-compile a
// bundle would instead cross-build the packer itself and fail to exec it on the
// host. Passing the target via flags keeps the env (and the packer build)
// host-native while still cross-building the bundle.
func targetPlatform(opts *packOptions) (goos, goarch string) {
	goos = runtime.GOOS
	if env := os.Getenv("GOOS"); env != "" {
		goos = env
	}
	if opts.goos != "" {
		goos = opts.goos
	}
	goarch = runtime.GOARCH
	if env := os.Getenv("GOARCH"); env != "" {
		goarch = env
	}
	if opts.goarch != "" {
		goarch = opts.goarch
	}
	return goos, goarch
}

// buildPackage runs `go build [extraArgs...] -o <tmp>/bundle <pkg>` for the
// given GOOS/GOARCH and returns the path to the freshly built executable plus a
// cleanup function. extraArgs is the slice that comes after the "--" separator
// on the airflow-go-pack command line; we drop the leading "--" before
// forwarding. GOOS/GOARCH are set explicitly, overriding any ambient env, so
// the caller controls the target platform.
func buildPackage(
	stderr io.Writer,
	pkg string,
	extraArgs []string,
	goos, goarch string,
) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "airflow-go-pack-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	binName := "bundle"
	if goos == "windows" {
		binName += ".exe"
	}
	outPath := filepath.Join(tmpDir, binName)

	args := []string{"build"}
	for _, a := range extraArgs {
		if a == "--" {
			continue
		}
		args = append(args, a)
	}
	args = append(args, "-o", outPath, pkg)

	cmd := exec.Command("go", args...)
	// Later duplicate keys win in os/exec, so these override any ambient
	// GOOS/GOARCH (which `go tool` already used to build this packer).
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("go build failed: %w", err)
	}
	return outPath, cleanup, nil
}

// cacheDigestDomain starts the cache digest's input, so the digest cannot equal
// a SHA-256 taken of the same bytes for another purpose, and a later definition
// can change the version.
const cacheDigestDomain = "airflow-go-pack cache digest v1\x00"

// bundleDigests is the manifest's digests mapping, each a lower-case hex SHA-256.
type bundleDigests struct {
	// Integrity covers the binary region; it equals the trailer's binary_sha256.
	Integrity string
	// Cache covers every region of the bundle except its own value. It is
	// SHA-256(cacheDigestDomain || SHA-256(binary) || SHA-256(source) ||
	// SHA-256(manifest rendered without digests)), with each inner digest as
	// its 32 raw bytes. Packing the same inputs again gives the same value, and
	// a change to any of them gives a different one.
	Cache string
}

func computeDigests(execPath string, source, baseManifest []byte) (bundleDigests, error) {
	binaryHash, err := bundlefooter.HashFile(execPath)
	if err != nil {
		return bundleDigests{}, fmt.Errorf("hashing executable %s: %w", execPath, err)
	}
	sourceHash := sha256.Sum256(source)
	manifestHash := sha256.Sum256(baseManifest)

	h := sha256.New()
	h.Write([]byte(cacheDigestDomain))
	h.Write(binaryHash[:])
	h.Write(sourceHash[:])
	h.Write(manifestHash[:])
	return bundleDigests{
		Integrity: hex.EncodeToString(binaryHash[:]),
		Cache:     hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// renderManifest serialises the airflow-metadata manifest as deterministic YAML matching
// airflow-metadata.schema.json. It injects the schema's source field (the filename the manifest is
// built from) and, when digests is not nil, the digests mapping, both of which only the packer
// knows.
func renderManifest(
	meta airflowmetadata.Manifest,
	sourceName string,
	digests *bundleDigests,
) ([]byte, error) {
	version := meta.AirflowBundleMetadataVersion
	if version == "" {
		version = airflowmetadata.FormatVersion
	}

	root := &yaml.Node{Kind: yaml.DocumentNode}
	manifest := &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalar("airflow_bundle_metadata_version"), quotedScalar(version),
			scalar("sdk"),
			{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("language"), quotedScalar(meta.SDK.Language),
					scalar("version"), quotedScalar(meta.SDK.Version),
					scalar("supervisor_schema_version"),
					quotedScalar(meta.SDK.SupervisorSchemaVersion),
				},
			},
			scalar("source"), quotedScalar(sourceName),
		},
	}
	if digests != nil {
		manifest.Content = append(manifest.Content,
			scalar("digests"),
			&yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("integrity"), quotedScalar(digests.Integrity),
					scalar("cache"), quotedScalar(digests.Cache),
				},
			},
		)
	}
	root.Content = []*yaml.Node{manifest}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// scalar emits a plain (unquoted) node. It is used for the structural keys,
// such as "sdk" and "digests".
func scalar(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: value}
}

// quotedScalar emits a double-quoted node. Data-bearing string values, such as
// the source filename and the SDK fields, go through this so a value that looks
// like a number, bool, or date (e.g. a version of "1.0") round-trips as a string
// rather than being retyped by the YAML parser.
func quotedScalar(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: value, Style: yaml.DoubleQuotedStyle}
}

// rejectOutputAlias fails if output resolves to the same file as any pack
// input: the executable or the source. Packing copies the executable to output
// with O_TRUNC and renames it into place, so an aliased output would clobber
// the input.
func rejectOutputAlias(output, execPath, sourcePath string) error {
	for _, in := range []struct {
		path string
		kind string
	}{
		{execPath, "executable"},
		{sourcePath, "source"},
	} {
		alias, err := sameFile(output, in.path)
		if err != nil {
			return fmt.Errorf("resolving output path %s: %w", output, err)
		}
		if alias {
			return fmt.Errorf(
				"output path %s is the same file as the %s %s; pass --output to write the bundle elsewhere",
				output,
				in.kind,
				in.path,
			)
		}
	}
	return nil
}

// sameFile reports whether a and b refer to the same file. It first compares
// cleaned absolute paths (catching e.g. "./bundle" vs "bundle" before either
// exists) and then, when both paths exist, falls back to os.SameFile to catch
// links that share an inode.
func sameFile(a, b string) (bool, error) {
	absA, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	absB, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	if absA == absB {
		return true, nil
	}
	fiA, err := os.Stat(absA)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	fiB, err := os.Stat(absB)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return os.SameFile(fiA, fiB), nil
}

// writeBundle assembles the bundle at output by copying the executable to a
// temporary file in output's directory, appending the source+manifest footer
// to that copy, then atomically renaming it into place. renderMetadata builds
// the manifest from the copy before the footer is appended. Writing through a
// temp file keeps a failed pack from leaving a truncated or half-written
// artefact at output, and guarantees the file being copied is never the same
// open file as the destination.
func writeBundle(
	execPath, output string,
	source []byte,
	renderMetadata func(stagedExec string) ([]byte, error),
) error {
	outDir := filepath.Dir(output)
	// The temp file and the atomic rename both live in output's directory, so it
	// must exist. Create it for the user (e.g. --output ./bin/bundle with no
	// ./bin) instead of failing with an opaque temp-file "no such file" error.
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating output directory %s: %w", outDir, err)
	}
	tmp, err := os.CreateTemp(outDir, ".airflow-go-pack-*")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", output, err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := copyFile(execPath, tmpPath, 0o755); err != nil {
		return fmt.Errorf("writing %s: %w", output, err)
	}
	metadata, err := renderMetadata(tmpPath)
	if err != nil {
		return err
	}
	if err := bundlefooter.Append(tmpPath, source, metadata); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, output); err != nil {
		return fmt.Errorf("finalising %s: %w", output, err)
	}
	committed = true
	return nil
}

// copyFile copies src to dst, truncating dst if it already exists.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}
