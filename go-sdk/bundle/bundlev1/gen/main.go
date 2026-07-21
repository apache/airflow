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
// It reads the "operator" and "dag" definitions and emits the TaskSpec and
// DagSpec structs plus their SchemaFields methods, so the exposed fields,
// their Go types, and the emit rules the serializer relies on cannot drift
// from the schema. Each field set is derived from its definition rather than
// hand-listed: every scalar property (string/integer/number/boolean, or a
// timedelta/datetime ref) becomes a spec field, in schema order, unless one
// of these rules skips it:
//
//   - "_"-prefixed keys are serializer internals (_task_module, _concurrency, ...)
//   - keys in the definition's "required" list are always written by the
//     serializer itself (task_type, task_id, dag_id, fileloc, ...)
//   - "has_on_*" keys are flags derived from Python callbacks, not settable
//   - non-scalar keys (arrays, objects, refs other than timedelta/datetime,
//     anyOf) cannot be expressed as a scalar spec field, unless the config's
//     typeOverrides maps them explicitly (the dag "tags" key -> []string)
//   - the config's excluded entries are Python-only or serializer-owned
//     concerns, documented per key
//
// A new scalar key added to the schema therefore shows up in the regenerated
// spec — visible in the spec.gen.go diff — or must gain an excluded entry,
// instead of going silently missing.
//
// Field defaults are read from the schema, never hard-coded here: a field is
// serialized only when it is set (non-zero) and differs from its schema
// default, mirroring Python BaseSerialization's behaviour of omitting values
// the scheduler will re-derive. Booleans whose schema default is true become
// *bool so nil can mean "unset" while an explicit false still serializes.
//
// Emit rules the schema cannot express live in each config's behaviors table
// and are validated against the schema on every run (a stale entry fails
// generation): always-emit keys have no schema default and Python's
// serializer always writes their resolved value, some falling back to a
// [core] config default when unset; emit-when-set booleans also have no
// schema default but are nullable, becoming *bool where nil omits the key
// while an explicit false still serializes. Extra fields (DagSpec.Schedule)
// are SDK-only concepts with no schema key; they are declared in the config
// and never emitted by SchemaFields.
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

type behaviorKind int

const (
	// behaviorAlwaysEmit marks a key with no schema default that Python's
	// serializer always writes: the field is emitted unconditionally, with
	// zeroFallback (when set) substituted for the zero value.
	behaviorAlwaysEmit behaviorKind = iota + 1
	// behaviorEmitWhenSet marks a nullable boolean with no schema default:
	// the field becomes *bool, nil omits the key, and an explicit false
	// still serializes.
	behaviorEmitWhenSet
)

// fieldBehavior is one behaviors-table entry: an emit rule the schema cannot
// express, keyed by schema property name and validated for staleness.
type fieldBehavior struct {
	kind behaviorKind
	// zeroFallback is the literal emitted when an always-emit field is zero,
	// e.g. "16"; fallbackDoc names its source, e.g. the [core] config option.
	zeroFallback string
	fallbackDoc  string
}

// extraField is a spec field with no schema key at all (an SDK-only concept
// like DagSpec.Schedule); it is rendered first and never emitted by
// SchemaFields.
type extraField struct {
	goName string
	goType string
	doc    []string
}

// specConfig describes one schema definition rendered into spec.gen.go.
type specConfig struct {
	defName  string
	typeName string
	// structDoc is the doc comment above the generated struct, pre-wrapped.
	structDoc string
	// excluded lists the eligible scalar keys deliberately not exposed, each
	// with the reason; every other eligible scalar key becomes a field. An
	// entry that no longer matches an eligible schema key fails generation,
	// so the list cannot go stale.
	excluded map[string]string
	// typeOverrides forces the Go type for keys the mechanical mapping gets
	// wrong or cannot express; entries matching no field fail generation.
	typeOverrides map[string]string
	// behaviors holds the emit rules the schema cannot express; entries
	// matching no field fail generation.
	behaviors map[string]fieldBehavior
	// extraFields are SDK-only fields with no schema key.
	extraFields []extraField
}

// specConfigs lists the definitions rendered into spec.gen.go, in output
// order.
var specConfigs = []specConfig{
	{
		defName:  "operator",
		typeName: "TaskSpec",
		structDoc: `TaskSpec is the optional configuration applied to a task at registration
time. Every field is optional: the zero value (nil for the *bool fields)
means "unset" and the scheduler falls back to the schema default. Each
field maps to the same-named key of the "operator" definition in
airflow-core/src/airflow/serialization/schema.json.`,
		excluded: map[string]string{
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
		},
		// retry_exponential_backoff is "number" with an integral default, but
		// Python declares it float (a backoff multiplier), so the mechanical
		// mapping would pick int.
		typeOverrides: map[string]string{
			"retry_exponential_backoff": "float64",
		},
	},
	{
		defName:  "dag",
		typeName: "DagSpec",
		structDoc: `DagSpec is the optional configuration applied to a Dag at registration
time. Every field is optional: the zero value (nil for the *bool fields)
means "unset", which either omits the key or, for the always-serialized
keys, emits its resolved default. Except for Schedule, each field maps to
the same-named key of the "dag" definition in
airflow-core/src/airflow/serialization/schema.json.`,
		excluded: map[string]string{
			"relative_fileloc": "computed by the serializer from fileloc and the bundle path",
		},
		// tags is a bare "array" in the schema; Python stores a set of
		// strings, expressed here as []string and serialized sorted.
		typeOverrides: map[string]string{
			"tags": "[]string",
		},
		behaviors: map[string]fieldBehavior{
			"catchup":                         {kind: behaviorAlwaysEmit},
			"disable_bundle_versioning":       {kind: behaviorAlwaysEmit},
			"max_consecutive_failed_dag_runs": {kind: behaviorAlwaysEmit},
			"max_active_tasks": {
				kind:         behaviorAlwaysEmit,
				zeroFallback: "16",
				fallbackDoc:  "[core] max_active_tasks_per_dag",
			},
			"max_active_runs": {
				kind:         behaviorAlwaysEmit,
				zeroFallback: "16",
				fallbackDoc:  "[core] max_active_runs_per_dag",
			},
			"is_paused_upon_creation": {kind: behaviorEmitWhenSet},
		},
		extraFields: []extraField{
			{
				goName: "Schedule",
				goType: "string",
				doc: []string{
					`Schedule is "@once", "@continuous", a cron expression, or "" for`,
					`NullTimetable (no schedule). It has no schema key: the serializer`,
					`derives the timetable object from it, so SchemaFields never emits it.`,
				},
			},
		},
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
	key     string
	goName  string
	goType  string
	def     any  // schema default (nil when absent)
	hasDef  bool // schema declares a default
	refName string
}

// resolvedSpec pairs a config with the fields selected from its definition.
type resolvedSpec struct {
	cfg    specConfig
	fields []field
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

	specs := make([]resolvedSpec, 0, len(specConfigs))
	for _, cfg := range specConfigs {
		def, ok := doc.Definitions[cfg.defName]
		if !ok {
			log.Fatalf("gen: schema has no %q definition", cfg.defName)
		}
		fields, err := selectFields(cfg, def)
		if err != nil {
			log.Fatalf("gen: %s: %v", cfg.typeName, err)
		}
		specs = append(specs, resolvedSpec{cfg: cfg, fields: fields})
	}

	src, err := render(*pkg, specs)
	if err != nil {
		log.Fatalf("gen: rendering: %v", err)
	}
	if err := os.WriteFile(*outPath, src, 0o644); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
}

// selectFields derives the spec fields from the definition, in schema order,
// applying the selection rules in the package comment.
func selectFields(cfg specConfig, def definition) ([]field, error) {
	required := make(map[string]bool, len(def.Required))
	for _, key := range def.Required {
		required[key] = true
	}

	excludedSeen := make(map[string]bool, len(cfg.excluded))
	fields := make([]field, 0, len(def.Properties.keys))
	for _, key := range def.Properties.keys {
		if strings.HasPrefix(key, "_") || required[key] || strings.HasPrefix(key, "has_on_") {
			continue
		}
		if _, ok := cfg.excluded[key]; ok {
			excludedSeen[key] = true
			continue
		}
		f, ok := resolveField(cfg, key, def.Properties.props[key])
		if !ok {
			continue
		}
		fields = append(fields, f)
	}

	for key := range cfg.excluded {
		if !excludedSeen[key] {
			return nil, fmt.Errorf(
				"excluded entry %q matches no eligible schema property; remove or fix it",
				key,
			)
		}
	}
	for key := range cfg.typeOverrides {
		if !containsKey(fields, key) {
			return nil, fmt.Errorf(
				"typeOverrides entry %q matches no generated field; remove or fix it",
				key,
			)
		}
	}
	if err := validateBehaviors(cfg, fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// validateBehaviors rejects behaviors-table entries that match no generated
// field or whose kind cannot apply to the field's resolved type.
func validateBehaviors(cfg specConfig, fields []field) error {
	byKey := make(map[string]field, len(fields))
	for _, f := range fields {
		byKey[f.key] = f
	}
	for key, beh := range cfg.behaviors {
		f, ok := byKey[key]
		if !ok {
			return fmt.Errorf(
				"behaviors entry %q matches no generated field; remove or fix it",
				key,
			)
		}
		switch beh.kind {
		case behaviorAlwaysEmit:
			if strings.HasPrefix(f.goType, "*") || strings.HasPrefix(f.goType, "[]") {
				return fmt.Errorf(
					"behaviors entry %q: always-emit requires a non-pointer scalar field, got %s",
					key,
					f.goType,
				)
			}
			if beh.zeroFallback != "" && f.goType != "int" && f.goType != "float64" {
				return fmt.Errorf(
					"behaviors entry %q: zeroFallback requires a numeric field, got %s",
					key,
					f.goType,
				)
			}
		case behaviorEmitWhenSet:
			if beh.zeroFallback != "" {
				return fmt.Errorf(
					"behaviors entry %q: zeroFallback is only valid with always-emit",
					key,
				)
			}
			if f.goType != "*bool" {
				return fmt.Errorf(
					"behaviors entry %q: emit-when-set requires a boolean key, got %s",
					key,
					f.goType,
				)
			}
		}
	}
	return nil
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
// ref other than timedelta/datetime) and no typeOverrides entry rescues it.
func resolveField(cfg specConfig, key string, prop propSchema) (field, bool) {
	f := field{
		key:    key,
		goName: goName(key),
		def:    prop.Default,
		hasDef: prop.Default != nil,
	}
	if ref := strings.TrimPrefix(prop.Ref, "#/definitions/"); ref != prop.Ref {
		f.refName = ref
	}

	switch {
	case cfg.typeOverrides[key] != "":
		f.goType = cfg.typeOverrides[key]
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
	if beh, ok := cfg.behaviors[key]; ok && beh.kind == behaviorEmitWhenSet && f.goType == "bool" {
		// Nullable in Python: nil must mean "unset" while an explicit false
		// still serializes.
		f.goType = "*bool"
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

func render(pkg string, specs []resolvedSpec) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(
		"// Code generated by bundlev1/gen from airflow-core/src/airflow/serialization/schema.json, DO NOT EDIT.\n",
	)
	b.WriteString(licenseHeader)
	fmt.Fprintf(&b, "\npackage %s\n\n", pkg)
	writeImports(&b, specs)

	for i, sp := range specs {
		if i > 0 {
			b.WriteString("\n")
		}
		renderSpec(&b, sp)
	}
	return format.Source(b.Bytes())
}

// writeImports emits the import block the resolved field types need.
func writeImports(b *bytes.Buffer, specs []resolvedSpec) {
	usesTime, usesSlices := false, false
	for _, sp := range specs {
		for _, f := range sp.fields {
			switch f.goType {
			case "time.Duration", "time.Time":
				usesTime = true
			case "[]string":
				usesSlices = true
			}
		}
	}
	switch {
	case usesTime && usesSlices:
		b.WriteString("import (\n\t\"slices\"\n\t\"time\"\n)\n\n")
	case usesTime:
		b.WriteString("import \"time\"\n\n")
	case usesSlices:
		b.WriteString("import \"slices\"\n\n")
	}
}

// renderSpec writes one config's struct and SchemaFields method.
func renderSpec(b *bytes.Buffer, sp resolvedSpec) {
	cfg := sp.cfg
	for _, line := range strings.Split(cfg.structDoc, "\n") {
		fmt.Fprintf(b, "// %s\n", line)
	}
	fmt.Fprintf(b, "type %s struct {\n", cfg.typeName)
	for _, ef := range cfg.extraFields {
		for _, line := range ef.doc {
			fmt.Fprintf(b, "\t// %s\n", line)
		}
		fmt.Fprintf(b, "\t%s %s\n", ef.goName, ef.goType)
	}
	for _, f := range sp.fields {
		writeFieldDoc(b, cfg, f)
		fmt.Fprintf(b, "\t%s %s\n", f.goName, f.goType)
	}
	b.WriteString("}\n\n")

	b.WriteString(`// SchemaFields returns the schema-keyed value of every field that is set and
// differs from its schema default, mirroring Python BaseSerialization's
// omission of values the scheduler re-derives. time.Duration and time.Time
// values are returned as-is; the serializer owns the wire encoding.
`)
	if extra := schemaFieldsExtraDoc(cfg, sp.fields); extra != "" {
		b.WriteString("//\n")
		for _, line := range wrapComment(extra, 76) {
			fmt.Fprintf(b, "// %s\n", line)
		}
	}
	fmt.Fprintf(
		b,
		"func (s %s) SchemaFields() map[string]any {\n\tm := map[string]any{}\n",
		cfg.typeName,
	)
	for _, f := range sp.fields {
		emitCondition(b, cfg, f)
	}
	b.WriteString("\treturn m\n}\n")
}

// writeFieldDoc renders a field's doc comment: the plain schema-key line, or
// a wrapped multi-line comment when a behavior or override adds policy text.
func writeFieldDoc(b *bytes.Buffer, cfg specConfig, f field) {
	text := fmt.Sprintf("%s maps to the schema key %q", f.goName, f.key)
	if f.hasDef {
		text += fmt.Sprintf(" (schema default %s)", defaultDoc(f))
	}
	extra := fieldDocExtra(cfg, f)
	if extra == "" {
		fmt.Fprintf(b, "\t// %s.\n", text)
		return
	}
	for _, line := range wrapComment(text+extra+".", 72) {
		fmt.Fprintf(b, "\t// %s\n", line)
	}
}

// fieldDocExtra renders the policy sentence a behavior or override entry adds
// to a field's doc comment.
func fieldDocExtra(cfg specConfig, f field) string {
	if beh, ok := cfg.behaviors[f.key]; ok {
		switch beh.kind {
		case behaviorAlwaysEmit:
			extra := "; it has no schema default and is always serialized"
			if beh.zeroFallback != "" {
				extra += fmt.Sprintf(
					", falling back to %s (the %s default) when unset",
					beh.zeroFallback,
					beh.fallbackDoc,
				)
			}
			return extra
		case behaviorEmitWhenSet:
			return `; it has no schema default: nil means "unset", Bool(true) / Bool(false) set it explicitly`
		}
	}
	if f.goType == "[]string" {
		return "; serialized as a sorted copy (Python stores this value in a set)"
	}
	return ""
}

// schemaFieldsExtraDoc renders the SchemaFields doc sentences for the emit
// policies that deviate from plain omit-if-default, derived from the same
// tables that emit the code so the two cannot drift.
func schemaFieldsExtraDoc(cfg specConfig, fields []field) string {
	var always, fallbacks, whenSet, sorted []string
	for _, f := range fields {
		if beh, ok := cfg.behaviors[f.key]; ok {
			switch beh.kind {
			case behaviorAlwaysEmit:
				always = append(always, f.key)
				if beh.zeroFallback != "" {
					fallbacks = append(fallbacks, f.key)
				}
			case behaviorEmitWhenSet:
				whenSet = append(whenSet, f.key)
			}
			continue
		}
		if f.goType == "[]string" {
			sorted = append(sorted, f.key)
		}
	}
	var parts []string
	if len(always) > 0 {
		s := fmt.Sprintf("Keys with no schema default (%s) are always emitted", joinAnd(always))
		if len(fallbacks) > 0 {
			s += fmt.Sprintf(
				"; %s fall back to their [core] config defaults when unset",
				joinAnd(fallbacks),
			)
		}
		parts = append(parts, s+".")
	}
	if len(whenSet) > 0 {
		parts = append(parts, fmt.Sprintf(
			"%s is emitted whenever non-nil, so an explicit false still serializes.",
			joinAnd(whenSet),
		))
	}
	if len(sorted) > 0 {
		parts = append(parts, fmt.Sprintf(
			"%s is emitted as a sorted copy, matching Python's set-backed serialization.",
			joinAnd(sorted),
		))
	}
	for _, ef := range cfg.extraFields {
		parts = append(
			parts,
			fmt.Sprintf("%s has no schema key and is never emitted here.", ef.goName),
		)
	}
	return strings.Join(parts, " ")
}

func joinAnd(items []string) string {
	if len(items) <= 1 {
		return strings.Join(items, "")
	}
	return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
}

// wrapComment breaks text on spaces into lines of at most width characters
// for comment rendering.
func wrapComment(text string, width int) []string {
	var lines []string
	cur := ""
	for _, word := range strings.Fields(text) {
		switch {
		case cur == "":
			cur = word
		case len(cur)+1+len(word) > width:
			lines = append(lines, cur)
			cur = word
		default:
			cur += " " + word
		}
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
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

// emitCondition writes one field's SchemaFields statement: its behavior-table
// rule when one exists, otherwise the "set and not schema default" guard.
func emitCondition(b *bytes.Buffer, cfg specConfig, f field) {
	name := "s." + f.goName
	key := f.key
	if beh, ok := cfg.behaviors[key]; ok {
		switch beh.kind {
		case behaviorAlwaysEmit:
			if beh.zeroFallback == "" {
				fmt.Fprintf(b, "\tm[%q] = %s\n", key, name)
				return
			}
			fmt.Fprintf(
				b,
				"\tif %s != 0 {\n\t\tm[%q] = %s\n\t} else {\n\t\tm[%q] = %s // %s\n\t}\n",
				name,
				key,
				name,
				key,
				beh.zeroFallback,
				beh.fallbackDoc,
			)
		case behaviorEmitWhenSet:
			fmt.Fprintf(b, "\tif %s != nil {\n\t\tm[%q] = *%s\n\t}\n", name, key, name)
		}
		return
	}
	switch {
	case f.goType == "[]string":
		fmt.Fprintf(
			b,
			"\tif len(%s) > 0 {\n\t\tm[%q] = slices.Sorted(slices.Values(%s))\n\t}\n",
			name,
			key,
			name,
		)
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
