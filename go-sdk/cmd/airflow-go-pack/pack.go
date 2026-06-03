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
	buildArgs  []string // forwarded verbatim to `go build` (already includes the leading "--")
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

	meta, err := readAirflowMetadata(execPath)
	if err != nil {
		return fmt.Errorf("--airflow-metadata: %w", err)
	}
	if len(meta.Dags) == 0 {
		return fmt.Errorf("bundle exposes no dags: nothing to pack")
	}
	for dagID, dag := range meta.Dags {
		if len(dag.Tasks) == 0 {
			fmt.Fprintf(stderr, "warning: dag %q has no tasks\n", dagID)
		}
	}

	manifest, err := renderManifest(meta, filepath.Base(sourcePath))
	if err != nil {
		return fmt.Errorf("rendering manifest: %w", err)
	}
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("reading source file: %w", err)
	}

	output := opts.output
	if output == "" {
		output, err = defaultOutputPath(sourcePath)
		if err != nil {
			return fmt.Errorf("determining default output path: %w", err)
		}
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
		output, meta.SDK.Language, meta.SDK.Version, len(meta.Dags))
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
	for line := range strings.SplitSeq(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
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

func readAirflowMetadata(execPath string) (airflowmetadata.Manifest, error) {
	out, err := runIntrospect(execPath, "--airflow-metadata")
	if err != nil {
		return airflowmetadata.Manifest{}, err
	}
	var meta airflowmetadata.Manifest
	if err := json.Unmarshal(out, &meta); err != nil {
		return airflowmetadata.Manifest{}, fmt.Errorf("decoding --airflow-metadata JSON: %w", err)
	}
	return meta, nil
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

// renderManifest serialises the airflow-metadata manifest as deterministic,
// sorted-key YAML matching airflow-metadata.schema.json. The metadata's name
// field is intentionally omitted: it is not part of the persisted manifest,
// only an introspection hint for the default output filename.
func renderManifest(meta airflowmetadata.Manifest, sourceName string) ([]byte, error) {
	version := meta.AirflowBundleMetadataVersion
	if version == "" {
		version = airflowmetadata.FormatVersion
	}

	dagIDs := make([]string, 0, len(meta.Dags))
	for id := range meta.Dags {
		dagIDs = append(dagIDs, id)
	}
	sort.Strings(dagIDs)

	dagsNode := &yaml.Node{Kind: yaml.MappingNode}
	for _, id := range dagIDs {
		tasks := meta.Dags[id].Tasks
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
			scalar("airflow_bundle_metadata_version"), quotedScalar(version),
			scalar("sdk"),
			{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("language"), scalar(meta.SDK.Language),
					scalar("version"), quotedScalar(meta.SDK.Version),
					scalar("supervisor_schema_version"),
					quotedScalar(meta.SDK.SupervisorSchemaVersion),
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
