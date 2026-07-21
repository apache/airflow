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

// Command gen generates spec.gen.go from the Airflow dag-serialization schema
// (airflow-core/src/airflow/serialization/schema.json).
//
// It reads the "operator" definition and emits the TaskSpec struct plus its
// SchemaFields method, so the exposed fields, their Go types, and the
// omit-if-default rules the serializer relies on cannot drift from the schema.
// The field set is derived from the schema rather than hand-listed: every
// scalar property (string/integer/number/boolean, or a timedelta/datetime ref)
// becomes a TaskSpec field, in schema order, unless one of these rules skips
// it:
//
//   - "_"-prefixed keys are serializer internals (_task_module, _is_mapped, ...)
//   - keys in the definition's "required" list are always written by the
//     serializer itself (task_type, ui_color, ...), except those re-exposed
//     as identity fields by a fieldOverrides entry (task_id)
//   - "has_on_*" keys are flags derived from Python callbacks, not settable
//   - non-scalar keys (arrays, objects, refs other than timedelta/datetime,
//     anyOf) cannot be expressed as a scalar spec field
//   - excludedFields entries are Python-only concerns, documented per key
//
// A new scalar key added to the schema therefore shows up in the regenerated
// spec — visible in the spec.gen.go diff — or must gain an excludedFields
// entry, instead of going silently missing.
//
// Field defaults are read from the schema, never hard-coded here: a field is
// serialized only when it is set (non-zero) and differs from its schema
// default, mirroring Python BaseSerialization's behaviour of omitting values
// the scheduler will re-derive. Booleans whose schema default is true become
// *bool so nil can mean "unset" while an explicit false still serializes.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"math"
	"os"
	"strings"
)

// excludedFields lists the scalar "operator" keys deliberately not exposed on
// TaskSpec, each with the reason; every other eligible scalar key becomes a
// field. An entry that no longer matches an eligible schema key fails
// generation, so the list cannot go stale.
var excludedFields = map[string]string{
	"doc":                    "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
	"doc_json":               "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
	"doc_yaml":               "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
	"doc_rst":                "legacy doc attribute; the UI renders Markdown, so only doc_md is exposed",
	"allow_nested_operators": "Python runtime concern: warns when an operator executes inside another operator",
	"multiple_outputs":       "TaskFlow (@task) dict-unpacking concern, meaningless outside Python",
	"start_from_trigger":     "deferrable-operator machinery; its start_trigger_args counterpart is an object the spec cannot express",
	"is_setup":               "setup/teardown flags carry trigger-rule invariants the SDK does not model yet",
	"is_teardown":            "setup/teardown flags carry trigger-rule invariants the SDK does not model yet",
	"on_failure_fail_dagrun": "only valid on teardown tasks, which the SDK does not model yet",
}

// fieldOverride carries the per-key tweaks the schema cannot express. goType
// forces the Go type when the schema type is too loose. goName overrides the
// mechanical snake_case→CamelCase name; doc overrides the generated field
// comment. identity marks a field that names the task rather than configuring
// it: it is exposed on the spec even though the schema lists it as required
// (serializer-owned), and it is kept out of SchemaFields because the
// serializer emits it from the registry's resolved TaskInfo.ID instead.
type fieldOverride struct {
	goType   string
	goName   string
	doc      string
	identity bool
}

// fieldOverrides is keyed by schema key; an entry that no longer matches a
// generated field fails generation. retry_exponential_backoff is "number"
// with an integral default, but Python declares it float (a backoff
// multiplier), so the mechanical mapping would pick int.
var fieldOverrides = map[string]fieldOverride{
	"retry_exponential_backoff": {goType: "float64"},
	"task_id": {
		goName:   "TaskId",
		identity: true,
		doc:      "TaskId sets the task id explicitly; empty derives it from the task function's name",
	},
}

// initialisms maps snake_case segments that must keep non-Title capitalization
// in Go names (do_xcom_push -> DoXComPush, doc_md -> DocMD).
var initialisms = map[string]string{
	"id":   "ID",
	"md":   "MD",
	"ui":   "UI",
	"uri":  "URI",
	"xcom": "XCom",
}

type propSchema struct {
	Type    any    `json:"type"`
	Ref     string `json:"$ref"`
	Default any    `json:"default"`
}

// orderedProps keeps schema property order, which encoding/json's map
// decoding discards; the generated struct follows the schema's field order.
type orderedProps struct {
	keys  []string
	props map[string]propSchema
}

func (o *orderedProps) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &o.props); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if _, err := dec.Token(); err != nil {
		return err
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		o.keys = append(o.keys, tok.(string))
		var skipped json.RawMessage
		if err := dec.Decode(&skipped); err != nil {
			return err
		}
	}
	return nil
}

type definition struct {
	Properties orderedProps `json:"properties"`
	Required   []string     `json:"required"`
}

type schemaDoc struct {
	Definitions map[string]definition `json:"definitions"`
}

// field is one resolved spec field: schema key + default joined with the Go
// name and type the generated struct uses.
type field struct {
	key      string
	goName   string
	goType   string
	doc      string // doc-comment override (empty = generated wording)
	identity bool   // named-identity field, excluded from SchemaFields
	def      any    // schema default (nil when absent)
	hasDef   bool   // schema declares a default
	refName  string
}

func main() {
	schemaPath := flag.String("schema", "", "path to the dag-serialization schema.json")
	outPath := flag.String("out", "spec.gen.go", "path to write the generated spec file")
	pkg := flag.String("package", "bundlev1", "package name for the generated file")
	flag.Parse()

	if *schemaPath == "" {
		log.Fatal("gen: -schema is required")
	}
	raw, err := os.ReadFile(*schemaPath)
	if err != nil {
		log.Fatalf("gen: reading schema: %v", err)
	}
	var doc schemaDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		log.Fatalf("gen: parsing schema: %v", err)
	}
	operator, ok := doc.Definitions["operator"]
	if !ok {
		log.Fatal(`gen: schema has no "operator" definition`)
	}

	fields, err := selectFields(operator)
	if err != nil {
		log.Fatalf("gen: %v", err)
	}

	src, err := render(*pkg, fields)
	if err != nil {
		log.Fatalf("gen: rendering: %v", err)
	}
	if err := os.WriteFile(*outPath, src, 0o644); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
}

// selectFields derives the TaskSpec fields from the operator definition, in
// schema order, applying the selection rules in the package comment.
func selectFields(op definition) ([]field, error) {
	required := make(map[string]bool, len(op.Required))
	for _, key := range op.Required {
		required[key] = true
	}

	excludedSeen := make(map[string]bool, len(excludedFields))
	fields := make([]field, 0, len(op.Properties.keys))
	for _, key := range op.Properties.keys {
		serializerOwned := strings.HasPrefix(key, "_") || required[key] ||
			strings.HasPrefix(key, "has_on_")
		if serializerOwned && !fieldOverrides[key].identity {
			continue
		}
		if _, ok := excludedFields[key]; ok {
			excludedSeen[key] = true
			continue
		}
		f, ok := resolveField(key, op.Properties.props[key])
		if !ok {
			continue
		}
		fields = append(fields, f)
	}

	for key := range excludedFields {
		if !excludedSeen[key] {
			return nil, fmt.Errorf(
				"excludedFields entry %q matches no eligible schema property; remove or fix it",
				key,
			)
		}
	}
	for key := range fieldOverrides {
		if !containsKey(fields, key) {
			return nil, fmt.Errorf(
				"fieldOverrides entry %q matches no generated field; remove or fix it",
				key,
			)
		}
	}
	return fields, nil
}

func containsKey(fields []field, key string) bool {
	for _, f := range fields {
		if f.key == key {
			return true
		}
	}
	return false
}

// resolveField maps one schema property to a spec field; ok is false when the
// property is not a scalar the spec can express (array, object, anyOf, or a
// ref other than timedelta/datetime).
func resolveField(key string, prop propSchema) (field, bool) {
	ov := fieldOverrides[key]
	f := field{
		key:      key,
		goName:   ov.goName,
		doc:      ov.doc,
		identity: ov.identity,
		def:      prop.Default,
		hasDef:   prop.Default != nil,
	}
	if f.goName == "" {
		f.goName = goName(key)
	}
	if ref := strings.TrimPrefix(prop.Ref, "#/definitions/"); ref != prop.Ref {
		f.refName = ref
	}

	switch {
	case ov.goType != "":
		f.goType = ov.goType
	case f.refName == "timedelta":
		f.goType = "time.Duration"
	case f.refName == "datetime":
		f.goType = "time.Time"
	default:
		typ, _ := prop.Type.(string)
		switch typ {
		case "string":
			f.goType = "string"
		case "integer":
			f.goType = "int"
		case "number":
			f.goType = "int"
			if n, ok := prop.Default.(float64); ok && n != math.Trunc(n) {
				f.goType = "float64"
			}
		case "boolean":
			f.goType = "bool"
			if d, ok := prop.Default.(bool); ok && d {
				// A plain bool cannot distinguish "unset" from an explicit
				// false when the schema default is true.
				f.goType = "*bool"
			}
		default:
			return field{}, false
		}
	}
	return f, true
}

func goName(key string) string {
	parts := strings.Split(key, "_")
	for i, p := range parts {
		if init, ok := initialisms[p]; ok {
			parts[i] = init
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, "")
}

const licenseHeader = `// Licensed to the Apache Software Foundation (ASF) under one
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
`

func render(pkg string, fields []field) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(
		"// Code generated by bundlev1/gen from airflow-core/src/airflow/serialization/schema.json, DO NOT EDIT.\n",
	)
	b.WriteString(licenseHeader)
	fmt.Fprintf(&b, "\npackage %s\n\n", pkg)
	b.WriteString("import \"time\"\n\n")

	b.WriteString(`// TaskSpec describes a task at registration time. Every field is optional:
// the zero value (nil for the *bool fields) means "unset" and the scheduler
// falls back to the schema default. Each field maps to the same-named key of
// the "operator" definition in
// airflow-core/src/airflow/serialization/schema.json.
type TaskSpec struct {
`)
	for _, f := range fields {
		if f.doc != "" {
			fmt.Fprintf(&b, "\t// %s. Maps to the schema key %q.\n", f.doc, f.key)
		} else {
			fmt.Fprintf(&b, "\t// %s maps to the schema key %q", f.goName, f.key)
			if f.hasDef {
				fmt.Fprintf(&b, " (schema default %s)", defaultDoc(f))
			}
			b.WriteString(".\n")
		}
		fmt.Fprintf(&b, "\t%s %s\n", f.goName, f.goType)
	}
	b.WriteString("}\n\n")

	b.WriteString(`// SchemaFields returns the schema-keyed value of every configuration field
// that is set and differs from its schema default, mirroring Python
// BaseSerialization's omission of values the scheduler re-derives. Identity
// fields (TaskId) are excluded: the serializer emits the id from the
// registry's resolved TaskInfo. time.Duration and time.Time values are
// returned as-is; the serializer owns the wire encoding.
func (s TaskSpec) SchemaFields() map[string]any {
	m := map[string]any{}
`)
	for _, f := range fields {
		if f.identity {
			continue
		}
		emitCondition(&b, f)
	}
	b.WriteString("\treturn m\n}\n")

	return format.Source(b.Bytes())
}

// defaultDoc renders a field's schema default for its doc comment.
func defaultDoc(f field) string {
	if f.refName == "timedelta" {
		return fmt.Sprintf("%v seconds", f.def)
	}
	if s, ok := f.def.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%v", f.def)
}

// emitCondition writes the "set and not schema default" guard for one field.
func emitCondition(b *bytes.Buffer, f field) {
	name := "s." + f.goName
	key := f.key
	switch {
	case f.goType == "time.Time":
		fmt.Fprintf(b, "\tif !%s.IsZero() {\n\t\tm[%q] = %s\n\t}\n", name, key, name)
	case f.goType == "time.Duration":
		cond := fmt.Sprintf("%s != 0", name)
		if secs, ok := f.def.(float64); ok && secs != 0 {
			cond += fmt.Sprintf(" && %s != %s", name, durationLit(secs))
		}
		fmt.Fprintf(b, "\tif %s {\n\t\tm[%q] = %s\n\t}\n", cond, key, name)
	case f.goType == "*bool":
		notDefault := "*" + name
		if def, _ := f.def.(bool); def {
			notDefault = "!*" + name
		}
		fmt.Fprintf(
			b,
			"\tif %s != nil && %s {\n\t\tm[%q] = *%s\n\t}\n",
			name,
			notDefault,
			key,
			name,
		)
	case f.goType == "bool":
		notDefault := name
		if def, _ := f.def.(bool); def {
			notDefault = "!" + name
		}
		fmt.Fprintf(b, "\tif %s {\n\t\tm[%q] = %s\n\t}\n", notDefault, key, name)
	case f.goType == "string":
		cond := fmt.Sprintf("%s != \"\"", name)
		if def, ok := f.def.(string); ok && def != "" {
			cond += fmt.Sprintf(" && %s != %q", name, def)
		}
		fmt.Fprintf(b, "\tif %s {\n\t\tm[%q] = %s\n\t}\n", cond, key, name)
	default: // int / float64
		cond := fmt.Sprintf("%s != 0", name)
		if def, ok := f.def.(float64); ok && def != 0 {
			cond += fmt.Sprintf(" && %s != %s", name, numLit(def, f.goType))
		}
		fmt.Fprintf(b, "\tif %s {\n\t\tm[%q] = %s\n\t}\n", cond, key, name)
	}
}

// durationLit renders a schema timedelta default (seconds) as a Go duration
// expression, e.g. 300 -> "300 * time.Second".
func durationLit(secs float64) string {
	if secs == math.Trunc(secs) {
		return fmt.Sprintf("%d * time.Second", int64(secs))
	}
	return fmt.Sprintf("time.Duration(%v * float64(time.Second))", secs)
}

func numLit(v float64, goType string) string {
	if goType == "int" {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%v", v)
}
