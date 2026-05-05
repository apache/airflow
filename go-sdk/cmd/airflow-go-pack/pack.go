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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

// packOptions are the flags accepted by the root pack command.
type packOptions struct {
	pkg        string   // target package (default ".")
	source     string   // override the auto-detected DAG source file
	executable string   // pack a pre-built binary instead of building
	output     string   // override the default <bundleName> output path
	buildArgs  []string // forwarded verbatim to `go build` (already includes the leading "--")
}

// bundleSpec mirrors the JSON printed by --dump-bundle-spec.
type bundleSpec struct {
	FormatVersion string                   `json:"format_version"`
	SDK           bundleSpecSDK            `json:"sdk"`
	Dags          map[string]bundleSpecDag `json:"dags"`
}

type bundleSpecSDK struct {
	Language string `json:"language"`
	Version  string `json:"version"`
}

type bundleSpecDag struct {
	Tasks []string `json:"tasks"`
}

// bundleMetadata mirrors --bundle-metadata's BundleInfo JSON.
type bundleMetadata struct {
	Name    string  `json:"Name"`
	Version *string `json:"Version,omitempty"`
}

func runPack(stdout, stderr io.Writer, opts *packOptions) error {
	if opts.executable != "" && len(opts.buildArgs) > 0 {
		return fmt.Errorf("--executable is mutually exclusive with go build flags after \"--\"")
	}

	sourcePath := opts.source
	var execPath string
	cleanupExec := func() {}
	defer func() { cleanupExec() }()

	if opts.executable != "" {
		execPath = opts.executable
		if sourcePath == "" {
			return fmt.Errorf(
				"--executable requires --source: cannot infer the DAG source for a pre-built binary",
			)
		}
	} else {
		discovered, err := discoverMainSource(opts.pkg)
		if err != nil {
			return fmt.Errorf("locating DAG source file: %w", err)
		}
		if sourcePath == "" {
			sourcePath = discovered
		}

		tmp, cleanup, err := buildPackage(stderr, opts.pkg, opts.buildArgs)
		if err != nil {
			return err
		}
		execPath = tmp
		cleanupExec = cleanup
	}

	if _, err := os.Stat(execPath); err != nil {
		return fmt.Errorf("executable %s: %w", execPath, err)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source file %s: %w", sourcePath, err)
	}

	meta, err := readBundleMetadata(execPath)
	if err != nil {
		return fmt.Errorf("--bundle-metadata: %w", err)
	}
	if meta.Name == "" {
		return fmt.Errorf(
			"bundle binary returned an empty BundleInfo.Name; set bundleName at build time",
		)
	}

	spec, err := readBundleSpec(execPath)
	if err != nil {
		return fmt.Errorf("--dump-bundle-spec: %w", err)
	}
	if len(spec.Dags) == 0 {
		return fmt.Errorf("bundle exposes no dags: nothing to pack")
	}
	for dagID, dag := range spec.Dags {
		if len(dag.Tasks) == 0 {
			fmt.Fprintf(stderr, "warning: dag %q has no tasks\n", dagID)
		}
	}

	manifest, err := renderManifest(spec, filepath.Base(sourcePath))
	if err != nil {
		return fmt.Errorf("rendering manifest: %w", err)
	}
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("reading source file: %w", err)
	}

	output := opts.output
	if output == "" {
		output = defaultOutputPath(meta.Name)
	}

	// Copy the executable to the output path before appending so we never
	// mutate the build artefact in the temp dir or the user-supplied
	// --executable file.
	if err := copyFile(execPath, output, 0o755); err != nil {
		return fmt.Errorf("writing %s: %w", output, err)
	}
	if err := bundlefooter.Append(output, sourceBytes, manifest); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "Wrote bundle %s (sdk=%s/%s, dags=%d)\n",
		output, spec.SDK.Language, spec.SDK.Version, len(spec.Dags))
	return nil
}

func defaultOutputPath(bundleName string) string {
	if runtime.GOOS == "windows" {
		return bundleName + ".exe"
	}
	return bundleName
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
		return "", fmt.Errorf("go list %s: %w: %s", pkg, err, stderr.String())
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
	for _, line := range bytes.Split([]byte(s), []byte("\n")) {
		t := bytes.TrimSpace(line)
		if len(t) > 0 {
			out = append(out, string(t))
		}
	}
	return out
}

// buildPackage runs `go build [extraArgs...] -o <tmp>/bundle <pkg>` and
// returns the path to the freshly built executable plus a cleanup function.
// extraArgs is the slice that comes after the "--" separator on the
// airflow-go-pack command line; we drop the leading "--" before forwarding.
func buildPackage(stderr io.Writer, pkg string, extraArgs []string) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "airflow-go-pack-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	binName := "bundle"
	if runtime.GOOS == "windows" {
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
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("go build failed: %w", err)
	}
	return outPath, cleanup, nil
}

func readBundleMetadata(execPath string) (bundleMetadata, error) {
	out, err := runIntrospect(execPath, "--bundle-metadata")
	if err != nil {
		return bundleMetadata{}, err
	}
	var meta bundleMetadata
	if err := json.Unmarshal(out, &meta); err != nil {
		return bundleMetadata{}, fmt.Errorf("decoding --bundle-metadata JSON: %w", err)
	}
	return meta, nil
}

func readBundleSpec(execPath string) (bundleSpec, error) {
	out, err := runIntrospect(execPath, "--dump-bundle-spec")
	if err != nil {
		return bundleSpec{}, err
	}
	var spec bundleSpec
	if err := json.Unmarshal(out, &spec); err != nil {
		return bundleSpec{}, fmt.Errorf("decoding --dump-bundle-spec JSON: %w", err)
	}
	return spec, nil
}

func runIntrospect(execPath string, flag string) ([]byte, error) {
	cmd := exec.Command(execPath, flag)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", execPath, flag, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// renderManifest serialises the bundle spec as deterministic, sorted-key
// YAML matching the schema in providers/sdk/executable/docs/bundle-spec.rst.
func renderManifest(spec bundleSpec, sourceName string) ([]byte, error) {
	if spec.FormatVersion == "" {
		spec.FormatVersion = "1.0"
	}

	dagIDs := make([]string, 0, len(spec.Dags))
	for id := range spec.Dags {
		dagIDs = append(dagIDs, id)
	}
	sort.Strings(dagIDs)

	dagsNode := &yaml.Node{Kind: yaml.MappingNode}
	for _, id := range dagIDs {
		tasks := spec.Dags[id].Tasks
		taskItems := make([]*yaml.Node, 0, len(tasks))
		for _, t := range tasks {
			taskItems = append(taskItems, scalar(t))
		}
		dagsNode.Content = append(dagsNode.Content,
			scalar(id),
			&yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("tasks"),
					{Kind: yaml.SequenceNode, Content: taskItems},
				},
			},
		)
	}

	root := &yaml.Node{Kind: yaml.DocumentNode}
	manifest := &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalar("format_version"), quotedScalar(spec.FormatVersion),
			scalar("sdk"),
			{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("language"), scalar(spec.SDK.Language),
					scalar("version"), quotedScalar(spec.SDK.Version),
				},
			},
			scalar("source"), scalar(sourceName),
			scalar("dags"), dagsNode,
		},
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

func scalar(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: value}
}

func quotedScalar(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: value, Style: yaml.DoubleQuotedStyle}
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
