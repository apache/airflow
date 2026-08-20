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
// five things it cannot:
//
//  1. Strips the dead `<Type>_<N>` typedefs emitted for each branch of a nullable
//     `anyOf` (e.g. AssetEventResponsePartitionKey_0); the structs use `any`, so
//     these are never referenced.
//  2. Widens every concrete int/float/bool field whose schema default the Go zero
//     value does not satisfy to a pointer (e.g. GetPreviousTI.MapIndex int -> *int,
//     GetAssetEventByAsset.Ascending bool -> *bool). With a plain scalar,
//     ,omitempty drops an explicit zero value (0, false) on encode and the
//     supervisor reapplies its default, reversing the caller's intent; a pointer
//     keeps ,omitempty meaning "absent" for nil while encoding an explicit zero,
//     and on decode nil is the natural "use the default". Strings are exempt: an
//     explicit "" is off-contract for these fields, so decode-side seeding (5)
//     serves them better than a pointer.
//  3. Generates a Type<Struct> constant for every body with a "type" const: the
//     single source of truth for the wire discriminator value.
//  4. Generates EnsureType, which stamps a body's "type" field from its Go type,
//     so the binding lives only in generated code and call sites can't mismatch.
//  5. Generates a DecodeMsgpack per struct carrying a schema default the Go zero
//     value would not satisfy (a non-zero builtin scalar, or any non-null default
//     on a nullable interface{}/enum field), seeding it before decode since
//     msgpack applies no defaults. Pointer-widened fields (see 2) are skipped:
//     their default is simply nil.
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
	"reflect"
	"regexp"
	"sort"
	"strconv"
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
	defaultsPath := flag.String(
		"defaults",
		"defaults.gen.go",
		"path to write the generated default-seeding decoders file",
	)
	pkg := flag.String("package", "genmodels", "package name for the generated files")
	flag.Parse()

	if *schemaPath == "" {
		log.Fatal("gen: -schema is required")
	}
	doc, err := loadSchema(*schemaPath)
	if err != nil {
		log.Fatalf("gen: reading schema %s: %v", *schemaPath, err)
	}
	structs, err := parseModels(*modelsPath)
	if err != nil {
		log.Fatalf("gen: parsing %s: %v", *modelsPath, err)
	}
	structByKey, err := indexByUpperName(structs)
	if err != nil {
		log.Fatalf("gen: %v", err)
	}
	pointerized := pointerizedFields(doc, structs, structByKey)
	if err := pointerizeFields(*modelsPath, pointerized); err != nil {
		log.Fatalf("gen: pointerizing %s: %v", *modelsPath, err)
	}
	if err := ensureLicenseHeader(*modelsPath); err != nil {
		log.Fatalf("gen: adding license header to %s: %v", *modelsPath, err)
	}
	if err := writeDiscriminators(doc, *outPath, *pkg, structByKey); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
	if err := writeDefaults(doc, *defaultsPath, *pkg, structs, structByKey, pointerized); err != nil {
		log.Fatalf("gen: writing %s: %v", *defaultsPath, err)
	}
}

type fieldInfo struct {
	GoName string // Go field name, e.g. "MapIndex"
	Tag    string // msgpack wire name, e.g. "map_index"
	GoType string // builtin scalar ("int"/"string"/"bool"/...); "any" for interface{}; a named enum type; "" otherwise
}

// parseModels strips dead `type X_N ...` typedefs in place and returns the
// surviving structs keyed by name with their field metadata. A dead type
// referenced only by another dead type survives, but go-jsonschema's anyOf
// output is flat so that does not arise here.
func parseModels(path string) (map[string][]fieldInfo, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	uses := identUseCounts(file)
	structs := map[string][]fieldInfo{}
	kept := file.Decls[:0]
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE || len(gen.Specs) != 1 {
			kept = append(kept, decl)
			continue
		}
		spec, ok := gen.Specs[0].(*ast.TypeSpec)
		if !ok {
			kept = append(kept, decl)
			continue
		}
		name := spec.Name.Name
		if deadTypeName.MatchString(name) && uses[name] == 1 {
			continue // declared once, never used: drop it
		}
		if st, ok := spec.Type.(*ast.StructType); ok {
			structs[name] = structFromAST(st)
		}
		kept = append(kept, decl)
	}
	file.Decls = kept

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return nil, err
	}
	return structs, os.WriteFile(path, buf.Bytes(), 0o644)
}

// structFromAST records each field's Go name, msgpack wire name, and (for scalar
// idents) Go type.
func structFromAST(st *ast.StructType) []fieldInfo {
	var fields []fieldInfo
	for _, field := range st.Fields.List {
		goType := ""
		switch t := field.Type.(type) {
		case *ast.Ident:
			goType = t.Name // builtin scalar, or a named enum like EmailRequestEmailType
		case *ast.InterfaceType:
			if t.Methods == nil || len(t.Methods.List) == 0 {
				goType = "any" // go-jsonschema renders a nullable scalar as interface{}
			}
		}
		tag := ""
		if field.Tag != nil {
			tag = msgpackName(field.Tag.Value)
		}
		for _, n := range field.Names {
			fields = append(fields, fieldInfo{GoName: n.Name, Tag: tag, GoType: goType})
		}
	}
	return fields
}

// msgpackName extracts the wire name from a raw struct-tag literal (backticks
// included), e.g. `msgpack:"map_index,omitempty"` -> "map_index".
func msgpackName(lit string) string {
	tag := reflect.StructTag(strings.Trim(lit, "`"))
	name, _, _ := strings.Cut(tag.Get("msgpack"), ",")
	return name
}

// indexByUpperName maps each struct's upper-cased name to its real name so a
// schema $def can be paired with its struct even when go-jsonschema capitalized
// it differently.
func indexByUpperName(structs map[string][]fieldInfo) (map[string]string, error) {
	idx := make(map[string]string, len(structs))
	for name := range structs {
		key := strings.ToUpper(name)
		if existing, dup := idx[key]; dup {
			return nil, fmt.Errorf(
				"ambiguous struct names %q and %q normalise alike",
				existing,
				name,
			)
		}
		idx[key] = name
	}
	return idx, nil
}

// widenableScalarTypes are the concrete scalar types whose Go zero value (0,
// 0.0, false) is a legitimate explicit wire value; a schema default the zero
// does not satisfy triggers pointer widening. Strings are excluded: an explicit
// "" is off-contract, so those fields keep decode-side default seeding instead.
var widenableScalarTypes = map[string]bool{
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"float32": true, "float64": true,
	"bool": true,
}

// indexFieldsByTag maps fields by wire tag; pointerizedFields and writeDefaults
// share it so pointer widening and default seeding stay in lockstep.
func indexFieldsByTag(fields []fieldInfo) map[string]fieldInfo {
	fieldByTag := map[string]fieldInfo{}
	for _, f := range fields {
		if f.Tag != "" && f.Tag != "-" {
			fieldByTag[f.Tag] = f
		}
	}
	return fieldByTag
}

// pointerizedFields returns structName -> goFieldName for every widenable scalar
// field whose schema default its Go zero value does not satisfy. These are
// widened to a pointer in models.gen.go so an unset value is omitted on the wire
// (and the supervisor reapplies the default) while an explicit zero still encodes.
func pointerizedFields(
	doc *schemaDoc,
	structs map[string][]fieldInfo,
	structByKey map[string]string,
) map[string]map[string]bool {
	set := map[string]map[string]bool{}
	for defName, def := range doc.Defs {
		structName, ok := structByKey[strings.ToUpper(defName)]
		if !ok {
			continue
		}
		fieldByTag := indexFieldsByTag(structs[structName])
		for propName, prop := range def.Properties {
			f, ok := fieldByTag[propName]
			if !ok || !widenableScalarTypes[f.GoType] {
				continue
			}
			if _, seedable := defaultLiteral(f.GoType, prop.Default); !seedable {
				continue
			}
			if set[structName] == nil {
				set[structName] = map[string]bool{}
			}
			set[structName][f.GoName] = true
		}
	}
	return set
}

// pointerizeFields rewrites each field named in set to a pointer in models.gen.go,
// leaving the struct tag (including ,omitempty) untouched, and writes the file
// back only when something changed. A field already a pointer is left alone, so a
// stray re-run of the gen tool without regenerating models.gen.go is a no-op.
func pointerizeFields(path string, set map[string]map[string]bool) error {
	if len(set) == 0 {
		return nil
	}
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return err
	}

	changed := false
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			fields := set[ts.Name.Name]
			if fields == nil {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range st.Fields.List {
				// go-jsonschema emits one name per field; a multi-name field
				// shares a single type and cannot be widened selectively, so skip
				// it rather than widen an unrelated sibling.
				if len(field.Names) != 1 || !fields[field.Names[0].Name] {
					continue
				}
				if ident, ok := field.Type.(*ast.Ident); ok {
					field.Type = &ast.StarExpr{X: ident}
					changed = true
				}
			}
		}
	}
	if !changed {
		return nil
	}

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// asfLicenseHeader is the Apache source header as Go line comments. go-jsonschema
// emits none, so the gen tool adds it (the template-generated files carry it
// inline) to keep models.gen.go self-contained and reproducible from go generate.
const asfLicenseHeader = `// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License.`

// ensureLicenseHeader inserts the Apache header into models.gen.go right after
// go-jsonschema's "Code generated" line when absent, matching the layout of the
// other generated Go files. It is idempotent so re-running gen is a no-op.
func ensureLicenseHeader(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if bytes.Contains(src, []byte("Licensed to the Apache Software Foundation")) {
		return nil
	}
	lines := strings.Split(string(src), "\n")
	insertAt := 0
	for i, l := range lines {
		if strings.HasPrefix(l, "// Code generated") {
			insertAt = i + 1
			break
		}
	}
	out := append([]string{}, lines[:insertAt]...)
	out = append(out, strings.Split(asfLicenseHeader, "\n")...)
	out = append(out, lines[insertAt:]...)
	return os.WriteFile(path, []byte(strings.Join(out, "\n")), 0o644)
}

// identUseCounts counts identifier occurrences across the file in one pass. A
// dead typedef's name occurs once (its declaration); used names occur more.
func identUseCounts(file *ast.File) map[string]int {
	counts := map[string]int{}
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			counts[id.Name]++
		}
		return true
	})
	return counts
}

type schemaProp struct {
	Const   string          `json:"const"`
	Default json.RawMessage `json:"default"`
}

type schemaDoc struct {
	Defs map[string]struct {
		Properties map[string]schemaProp `json:"properties"`
	} `json:"$defs"`
}

func loadSchema(path string) (*schemaDoc, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc schemaDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

type binding struct {
	Struct string // generated Go struct name, e.g. "GetConnection"
	Const  string // wire value of the "type" property's const
}

// writeDiscriminators pairs each schema "type" const with its generated struct
// and emits the Type<Struct> constants plus EnsureType.
func writeDiscriminators(doc *schemaDoc, outPath, pkg string, structByKey map[string]string) error {
	var bindings []binding
	for defName, def := range doc.Defs {
		c := def.Properties["type"].Const
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

type postFill struct {
	Field string // Go field name of a nullable interface{} field
	Tag   string // its msgpack wire key, used to test presence on the wire
	Lit   string // Go literal for its schema default
}

type decoder struct {
	Struct    string     // generated Go struct name
	PreInits  string     // composite-literal seeds for concrete fields, e.g. `MapIndex: -1, Queue: "default"`
	PostFills []postFill // interface{} fields set after decode when still nil
}

// writeDefaults emits a DecodeMsgpack per struct with at least one seedable
// schema default (see defaultLiteral). A concrete field is pre-seeded in the
// composite literal so msgpack overwrites only keys present on the wire; a
// nullable interface{} field cannot be pre-seeded (msgpack decodes a present key
// in place and panics on the unaddressable interface value), so its default is
// applied after decode only when its wire key was absent, leaving an explicit
// null as nil (decoding to nil and "absent" are otherwise indistinguishable).
func writeDefaults(
	doc *schemaDoc,
	outPath, pkg string,
	structs map[string][]fieldInfo,
	structByKey map[string]string,
	pointerized map[string]map[string]bool,
) error {
	var decoders []decoder
	for defName, def := range doc.Defs {
		structName, ok := structByKey[strings.ToUpper(defName)]
		if !ok {
			continue
		}
		fieldByTag := indexFieldsByTag(structs[structName])
		var preSeeds []string
		var postFills []postFill
		for propName, prop := range def.Properties {
			if propName == "type" {
				continue // discriminator const, owned by EnsureType
			}
			f, ok := fieldByTag[propName]
			if !ok {
				continue
			}
			if pointerized[structName][f.GoName] {
				continue // widened to a pointer; its default is nil (absent)
			}
			lit, ok := defaultLiteral(f.GoType, prop.Default)
			if !ok {
				continue
			}
			if f.GoType == "any" {
				postFills = append(postFills, postFill{Field: f.GoName, Tag: propName, Lit: lit})
			} else {
				preSeeds = append(preSeeds, f.GoName+": "+lit)
			}
		}
		if len(preSeeds) == 0 && len(postFills) == 0 {
			continue
		}
		sort.Strings(preSeeds)
		sort.Slice(
			postFills,
			func(i, j int) bool { return postFills[i].Field < postFills[j].Field },
		)
		decoders = append(decoders, decoder{
			Struct:    structName,
			PreInits:  strings.Join(preSeeds, ", "),
			PostFills: postFills,
		})
	}
	sort.Slice(decoders, func(i, j int) bool { return decoders[i].Struct < decoders[j].Struct })

	var buf bytes.Buffer
	if err := defaultsTmpl.Execute(&buf, map[string]any{
		"Package":  pkg,
		"Decoders": decoders,
	}); err != nil {
		return err
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("formatting generated source: %w", err)
	}
	return os.WriteFile(outPath, formatted, 0o644)
}

// defaultLiteral renders raw as a Go literal to seed a field of type goType, or
// reports false when there is nothing to seed: no default, a null default, an
// unsupported kind, or a builtin scalar whose default already equals its Go zero.
// A nullable field (go-jsonschema's interface{}) and a named enum field have a
// nil/typed-zero Go default, so a 0/false/"" schema default on them is still
// seeded to stay distinct from an absent value.
func defaultLiteral(goType string, raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil || v == nil {
		return "", false
	}
	switch goType {
	case "int", "int8", "int16", "int32", "int64":
		f, ok := v.(float64)
		if !ok || int64(f) == 0 {
			return "", false
		}
		return strconv.FormatInt(int64(f), 10), true
	case "float32", "float64":
		f, ok := v.(float64)
		if !ok || f == 0 {
			return "", false
		}
		return strconv.FormatFloat(f, 'g', -1, 64), true
	case "string":
		s, ok := v.(string)
		if !ok || s == "" {
			return "", false
		}
		return strconv.Quote(s), true
	case "bool":
		b, ok := v.(bool)
		if !ok || !b {
			return "", false
		}
		return "true", true
	case "any":
		return interfaceLiteral(v)
	case "":
		return "", false
	default:
		// Named enum type (e.g. EmailRequestEmailType): seed a non-empty string
		// default through a conversion that compiles for any string-based enum.
		s, ok := v.(string)
		if !ok || s == "" {
			return "", false
		}
		return goType + "(" + strconv.Quote(s) + ")", true
	}
}

// interfaceLiteral renders a scalar default for an interface{} field. Its Go zero
// is nil, so unlike a typed scalar a 0/false/"" default is still seeded. An
// integral number renders as an int literal (the common case: map_index = -1).
func interfaceLiteral(v any) (string, bool) {
	switch x := v.(type) {
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10), true
		}
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case string:
		return strconv.Quote(x), true
	case bool:
		return strconv.FormatBool(x), true
	default:
		return "", false
	}
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

import "reflect"

// Message-type discriminator constants, generated from each body's "type" const
// in the supervisor wire-schema: the single source of truth for the "type"
// field's value, so callers never hand-write strings that could drift.
const (
{{- range .Bindings}}
	Type{{.Struct}} = "{{.Const}}"
{{- end}}
)

// EnsureType returns m with its "type" discriminator set to the constant bound to
// its Go type, dereferencing a pointer body first; non-body values pass through.
// The frame encoder runs every outbound body through it, so the binding lives
// only here and call sites can't pair the wrong constant with a struct.
func EnsureType(m any) any {
	switch b := m.(type) {
{{- range .Bindings}}
	case {{.Struct}}:
		b.Type = Type{{.Struct}}
		return b
{{- end}}
	default:
		// Stamp the pointee of a pointer body (e.g. &GetVariable{}) so it isn't
		// silently left without a discriminator.
		if rv := reflect.ValueOf(m); rv.Kind() == reflect.Pointer && !rv.IsNil() {
			return EnsureType(rv.Elem().Interface())
		}
		return m
	}
}
`),
)

var defaultsTmpl = template.Must(
	template.New("def").Parse(`// Code generated by genmodels/gen, DO NOT EDIT.
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
{{if .Decoders}}
import "github.com/vmihailenco/msgpack/v5"
{{range .Decoders}}
// DecodeMsgpack applies {{.Struct}}'s schema defaults that msgpack would otherwise skip.
func (m *{{.Struct}}) DecodeMsgpack(dec *msgpack.Decoder) error {
	type alias {{.Struct}}
	v := alias{ {{.PreInits}} }
{{- if .PostFills}}
	// Decode once into raw bytes so a nullable default applies only on an absent
	// wire key, never overwriting an explicit null (both decode to nil).
	var raw msgpack.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	if err := msgpack.Unmarshal(raw, &v); err != nil {
		return err
	}
	var present map[string]msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &present); err != nil {
		return err
	}
{{- range .PostFills}}
	if _, ok := present["{{.Tag}}"]; !ok {
		v.{{.Field}} = {{.Lit}}
	}
{{- end}}
{{- else}}
	if err := dec.Decode(&v); err != nil {
		return err
	}
{{- end}}
	*m = {{.Struct}}(v)
	return nil
}
{{end}}{{end}}`),
)
