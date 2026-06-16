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

// Command gen post-processes the genmodels package after go-jsonschema runs.
//
// It does two things go-jsonschema cannot:
//
//  1. Strips the dead `<Type>_<N>` typedefs go-jsonschema emits for each branch
//     of a nullable `anyOf` (e.g. AssetEventResponsePartitionKey_0). They are
//     never referenced by the generated structs (which use `any`), so they are
//     pure noise.
//
//  2. Generates discriminators.gen.go: a Type<Name> string constant for every
//     schema body whose "type" property is a const. This is the single source
//     of truth for the message-type discriminator, so callers never hand-write
//     wire strings that could drift from the schema.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"
	"regexp"
	"sort"
	"text/template"
)

var deadTypeName = regexp.MustCompile(`_[0-9]+$`)

func main() {
	schemaPath := flag.String("schema", "", "path to the supervisor schema.json")
	modelsPath := flag.String(
		"models",
		"models.gen.go",
		"path to the generated models file to clean in place",
	)
	outPath := flag.String(
		"out",
		"discriminators.gen.go",
		"path to write the generated discriminators file",
	)
	pkg := flag.String("package", "genmodels", "package name for the generated files")
	flag.Parse()

	if *schemaPath == "" {
		log.Fatal("gen: -schema is required")
	}
	if err := stripDeadTypes(*modelsPath); err != nil {
		log.Fatalf("gen: stripping dead types from %s: %v", *modelsPath, err)
	}
	if err := writeDiscriminators(*schemaPath, *outPath, *pkg); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
}

// stripDeadTypes removes top-level `type X_N ...` declarations that are not
// referenced anywhere else in the file.
func stripDeadTypes(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return err
	}

	kept := file.Decls[:0]
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE || len(gen.Specs) != 1 {
			kept = append(kept, decl)
			continue
		}
		spec := gen.Specs[0].(*ast.TypeSpec)
		name := spec.Name.Name
		if deadTypeName.MatchString(name) && referenceCount(src, name) == 1 {
			continue // declared once, never used: drop it
		}
		kept = append(kept, decl)
	}
	file.Decls = kept

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// referenceCount counts whole-word occurrences of name in src. A dead typedef
// appears exactly once (its own declaration); anything used elsewhere appears
// more than once and is kept.
func referenceCount(src []byte, name string) int {
	return len(regexp.MustCompile(`\b`+regexp.QuoteMeta(name)+`\b`).FindAll(src, -1))
}

type discriminator struct {
	Name  string // schema $def name, e.g. "GetConnection"
	Const string // wire value of the "type" property's const
}

// writeDiscriminators reads the schema and emits a Type<Name> constant for each
// $def whose "type" property is a const string.
func writeDiscriminators(schemaPath, outPath, pkg string) error {
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}
	var doc struct {
		Defs map[string]struct {
			Properties struct {
				Type struct {
					Const string `json:"const"`
				} `json:"type"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}

	var discs []discriminator
	for name, def := range doc.Defs {
		if c := def.Properties.Type.Const; c != "" {
			discs = append(discs, discriminator{Name: name, Const: c})
		}
	}
	sort.Slice(discs, func(i, j int) bool { return discs[i].Name < discs[j].Name })

	var buf bytes.Buffer
	if err := discriminatorTmpl.Execute(&buf, map[string]any{
		"Package":        pkg,
		"Discriminators": discs,
	}); err != nil {
		return err
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("formatting generated source: %w", err)
	}
	return os.WriteFile(outPath, formatted, 0o644)
}

var discriminatorTmpl = template.Must(
	template.New("disc").Parse(`// Code generated by genmodels/gen, DO NOT EDIT.

package {{.Package}}

// Message-type discriminator constants, generated from the "type" const of each
// body in the supervisor wire-schema. They are the single source of truth for
// the values carried in the "type" field, so callers never hand-write wire
// strings that could drift from the schema.
const (
{{- range .Discriminators}}
	Type{{.Name}} = "{{.Const}}"
{{- end}}
)
`),
)
