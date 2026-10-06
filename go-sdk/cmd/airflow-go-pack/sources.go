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
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

// goModule is the module that owns the bundle's entrypoint.
type goModule struct {
	dir  string // module root on disk
	path string // module path from go.mod; empty when no go.mod was found
}

// sourceFile is one file embedded in the source region.
type sourceFile struct {
	path   string // slash path relative to the module root
	disk   string // where the packer read the file from
	offset int
	length int
	sha256 string // lowercase hex
}

// sourceLayout describes the source region: which file is the entrypoint, which file each
// native Dag came from, and where every file sits in the region.
type sourceLayout struct {
	entrypoint string
	dagPaths   map[string]string
	files      []sourceFile
}

// diskPaths lists the files the packer read, so the caller can keep the output away from them.
func (l sourceLayout) diskPaths() []string {
	paths := make([]string, len(l.files))
	for i, f := range l.files {
		paths[i] = f.disk
	}
	return paths
}

// listModule asks the go tool for the module that owns dir. In a workspace it lists every
// workspace module, so it keeps the innermost one that contains dir.
func listModule(dir string) (goModule, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}\n{{.Path}}")
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return goModule{}, fmt.Errorf(
			"go list -m in %s: %w: %s", dir, err, strings.TrimSpace(stderr.String()),
		)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return goModule{}, err
	}
	lines := splitNonEmpty(stdout.String())
	var best goModule
	for i := 0; i+1 < len(lines); i += 2 {
		if _, ok := relativeTo(lines[i], absDir); ok && len(lines[i]) > len(best.dir) {
			best = goModule{dir: lines[i], path: lines[i+1]}
		}
	}
	if best.dir == "" {
		return goModule{}, fmt.Errorf("go list -m in %s: no module contains the directory", dir)
	}
	return best, nil
}

// findModule walks up from dir to the nearest go.mod and reads its module path. It is for
// --executable, where there is no package to hand to the go tool. Without a go.mod the module
// is dir itself with no path, which still resolves absolute paths under it.
func findModule(dir string) (goModule, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return goModule{}, err
	}
	for cur := abs; ; cur = filepath.Dir(cur) {
		data, err := os.ReadFile(filepath.Join(cur, "go.mod"))
		if err == nil {
			return goModule{dir: cur, path: modulePath(data)}, nil
		}
		if !os.IsNotExist(err) {
			return goModule{}, err
		}
		if filepath.Dir(cur) == cur {
			return goModule{dir: abs}, nil
		}
	}
}

// modulePath returns the path on the module line of a go.mod file, or "" if there is none.
func modulePath(goMod []byte) string {
	for line := range strings.SplitSeq(string(goMod), "\n") {
		line, _, _ = strings.Cut(line, "//")
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return strings.Trim(fields[1], "\"`")
		}
	}
	return ""
}

// relativeTo returns the slash path of file relative to dir. It reports false if file is not
// inside dir.
func relativeTo(dir, file string) (string, bool) {
	rel, err := filepath.Rel(dir, file)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	return rel, true
}

// moduleRelPath turns a source path reported by the bundle binary into a slash path relative
// to the module root. The binary reports an absolute build path, or "<module path>/<rel>"
// when it was built with -trimpath. It reports false for a file that is not a regular file
// inside the module, such as one from the module cache or another module.
func moduleRelPath(reported string, mod goModule) (string, bool) {
	var rel string
	switch {
	case reported == "":
		return "", false
	case filepath.IsAbs(filepath.FromSlash(reported)):
		var ok bool
		if rel, ok = relativeTo(mod.dir, filepath.FromSlash(reported)); !ok {
			return "", false
		}
	case mod.path != "" && strings.HasPrefix(reported, mod.path+"/"):
		rel = path.Clean(strings.TrimPrefix(reported, mod.path+"/"))
		if rel == ".." || strings.HasPrefix(rel, "../") {
			return "", false
		}
	default:
		return "", false
	}
	info, err := os.Stat(filepath.Join(mod.dir, filepath.FromSlash(rel)))
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	return rel, true
}

// layoutSources decides which files the bundle embeds and what each native Dag maps to. The
// entrypoint always comes first, and every other file follows in sorted order, so identical
// inputs give an identical region. A Dag whose file lies outside the module maps to the
// entrypoint, with a warning. It returns the layout and the concatenated region.
func layoutSources(
	stderr io.Writer,
	meta airflowmetadata.Manifest,
	entrypoint string,
	mod goModule,
) (sourceLayout, []byte, error) {
	absEntry, err := filepath.Abs(entrypoint)
	if err != nil {
		return sourceLayout{}, nil, err
	}
	entryPath, ok := relativeTo(mod.dir, absEntry)
	if !ok {
		entryPath = filepath.Base(absEntry)
	}

	layout := sourceLayout{
		entrypoint: entryPath,
		dagPaths:   make(map[string]string, len(meta.DagSourceFiles)),
	}
	disk := map[string]string{entryPath: absEntry}

	dagIDs := make([]string, 0, len(meta.DagSourceFiles))
	for id := range meta.DagSourceFiles {
		dagIDs = append(dagIDs, id)
	}
	sort.Strings(dagIDs)
	for _, id := range dagIDs {
		reported := meta.DagSourceFiles[id]
		rel, ok := moduleRelPath(reported, mod)
		if !ok {
			fmt.Fprintf(stderr,
				"warning: source file %q of dag %q is outside module %s; "+
					"embedding the entrypoint %s for it instead\n",
				reported, id, mod.dir, entryPath)
			layout.dagPaths[id] = entryPath
			continue
		}
		layout.dagPaths[id] = rel
		if _, seen := disk[rel]; !seen {
			disk[rel] = filepath.Join(mod.dir, filepath.FromSlash(rel))
		}
	}

	paths := make([]string, 0, len(disk))
	for p := range disk {
		if p != entryPath {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	paths = append([]string{entryPath}, paths...)

	var region bytes.Buffer
	for _, p := range paths {
		data, err := os.ReadFile(disk[p])
		if err != nil {
			return sourceLayout{}, nil, fmt.Errorf("reading source file: %w", err)
		}
		sum := sha256.Sum256(data)
		layout.files = append(layout.files, sourceFile{
			path:   p,
			disk:   disk[p],
			offset: region.Len(),
			length: len(data),
			sha256: hex.EncodeToString(sum[:]),
		})
		region.Write(data)
		if int64(region.Len()) > bundlefooter.MaxRegionSize {
			return sourceLayout{}, nil, fmt.Errorf(
				"source region too large after embedding %s (max %d bytes)",
				p, int64(bundlefooter.MaxRegionSize),
			)
		}
	}
	return layout, region.Bytes(), nil
}
