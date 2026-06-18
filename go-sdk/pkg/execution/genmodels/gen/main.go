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

// Command gen post-processes the genmodels package after go-jsonschema, doing
// three things it cannot:
//
//  1. Strips the dead `<Type>_<N>` typedefs emitted for each branch of a nullable
//     `anyOf` (e.g. AssetEventResponsePartitionKey_0); the structs use `any`, so
//     these are never referenced.
//  2. Generates a Type<Struct> constant for every body with a "type" const: the
//     single source of truth for the wire discriminator value.
//  3. Generates EnsureType, which stamps a body's "type" field from its Go type,
//     so the binding lives only in generated code and call sites can't mismatch.
//
// Constants and cases key on the generated struct names (read from models.gen.go)
// matched to the schema's wire values case-insensitively, since go-jsonschema may
// recapitalize a body (ClearAssetStoreByUri -> ClearAssetStoreByURI) away from
// its $def name.
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
	"strings"
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
	typed, err := stripDeadTypes(*modelsPath)
	if err != nil {
		log.Fatalf("gen: stripping dead types from %s: %v", *modelsPath, err)
	}
	if err := writeDiscriminators(*schemaPath, *outPath, *pkg, typed); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
}

// stripDeadTypes removes top-level `type X_N ...` declarations that are not
// referenced anywhere else in the file, and returns the set of remaining struct
// type names that declare a string "Type" field (the bodies that carry a wire
// discriminator).
func stripDeadTypes(path string) (map[string]bool, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	typed := map[string]bool{}
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
		if st, ok := spec.Type.(*ast.StructType); ok && hasStringTypeField(st) {
			typed[name] = true
		}
		kept = append(kept, decl)
	}
	file.Decls = kept

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return nil, err
	}
	return typed, os.WriteFile(path, buf.Bytes(), 0o644)
}

// hasStringTypeField reports whether a struct declares a `Type string` field.
func hasStringTypeField(st *ast.StructType) bool {
	for _, field := range st.Fields.List {
		ident, ok := field.Type.(*ast.Ident)
		if !ok || ident.Name != "string" {
			continue
		}
		for _, name := range field.Names {
			if name.Name == "Type" {
				return true
			}
		}
	}
	return false
}

// referenceCount counts whole-word occurrences of name in src. A dead typedef
// appears exactly once (its own declaration); anything used elsewhere appears
// more than once and is kept.
func referenceCount(src []byte, name string) int {
	return len(regexp.MustCompile(`\b`+regexp.QuoteMeta(name)+`\b`).FindAll(src, -1))
}

type binding struct {
	Struct string // generated Go struct name, e.g. "GetConnection"
	Const  string // wire value of the "type" property's const
}

// writeDiscriminators reads the schema's "type" consts, pairs each with its
// generated struct, and emits the Type<Struct> constants plus EnsureType.
func writeDiscriminators(schemaPath, outPath, pkg string, typed map[string]bool) error {
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

	// Index the typed structs by an upper-cased key so a $def name can be paired
	// with its struct even when go-jsonschema capitalized it differently.
	structByKey := make(map[string]string, len(typed))
	for name := range typed {
		key := strings.ToUpper(name)
		if existing, dup := structByKey[key]; dup {
			return fmt.Errorf("ambiguous struct names %q and %q normalise alike", existing, name)
		}
		structByKey[key] = name
	}

	var bindings []binding
	for defName, def := range doc.Defs {
		c := def.Properties.Type.Const
		if c == "" {
			continue
		}
		structName, ok := structByKey[strings.ToUpper(defName)]
		if !ok {
			log.Printf(
				"gen: schema body %q has a type const but no generated struct; skipping",
				defName,
			)
			continue
		}
		bindings = append(bindings, binding{Struct: structName, Const: c})
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Struct < bindings[j].Struct })

	var buf bytes.Buffer
	if err := discriminatorTmpl.Execute(&buf, map[string]any{
		"Package":  pkg,
		"Bindings": bindings,
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

package {{.Package}}

// Message-type discriminator constants, generated from each body's "type" const
// in the supervisor wire-schema: the single source of truth for the "type"
// field's value, so callers never hand-write strings that could drift.
const (
{{- range .Bindings}}
	Type{{.Struct}} = "{{.Const}}"
{{- end}}
)

// EnsureType returns m with its "type" discriminator set to the constant bound
// to its Go type; bodies of an unknown type are returned unchanged. The frame
// encoder runs every outbound body through it, so the binding lives only here
// and call sites can't pair the wrong constant with a struct.
func EnsureType(m any) any {
	switch b := m.(type) {
{{- range .Bindings}}
	case {{.Struct}}:
		b.Type = Type{{.Struct}}
		return b
{{- end}}
	default:
		return m
	}
}
`),
)
