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

// Command gen generates bundle specs from the Dag serialization schema.
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
	behaviorAlwaysEmit behaviorKind = iota + 1
	behaviorEmitWhenSet
)

type fieldBehavior struct {
	kind         behaviorKind
	zeroFallback string
	fallbackDoc  string
}

type extraField struct {
	goName string
	goType string
	doc    []string
}

type specConfig struct {
	defName       string
	typeName      string
	structDoc     string
	excluded      map[string]string
	typeOverrides map[string]string
	behaviors     map[string]fieldBehavior
	extraFields   []extraField
}

var specConfigs = []specConfig{
	{
		defName:   "operator",
		typeName:  "TaskSpec",
		structDoc: `TaskSpec configures a registered task. Zero values use schema defaults.`,
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
		// Python treats this integral schema default as a float multiplier.
		typeOverrides: map[string]string{
			"retry_exponential_backoff": "float64",
		},
	},
	{
		defName:   "dag",
		typeName:  "DagSpec",
		structDoc: `DagSpec configures a registered Dag. Zero values use schema defaults.`,
		excluded: map[string]string{
			"relative_fileloc": "computed by the serializer from fileloc and the bundle path",
		},
		// Python stores tags as a set.
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
					`Schedule accepts "@once", "@continuous", a cron expression, or "".`,
				},
			},
		},
	},
}

// initialisms preserves Go capitalization in schema names.
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

// orderedProps preserves schema field order.
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

type field struct {
	key     string
	goName  string
	goType  string
	def     any
	hasDef  bool
	refName string
}

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

// resolveField rejects unsupported schema properties.
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
				// Preserve false against a true default.
				f.goType = "*bool"
			}
		default:
			return field{}, false
		}
	}
	if beh, ok := cfg.behaviors[key]; ok && beh.kind == behaviorEmitWhenSet && f.goType == "bool" {
		// Preserve explicit false.
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

	b.WriteString("// SchemaFields returns set fields that differ from schema defaults.\n")
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

func writeFieldDoc(b *bytes.Buffer, cfg specConfig, f field) {
	text := fmt.Sprintf("%s maps to the schema key %q", f.goName, f.key)
	if f.hasDef {
		text += fmt.Sprintf(" (schema default %s)", defaultDoc(f))
	}
	for _, line := range wrapComment(text+fieldDocExtra(cfg, f)+".", 72) {
		fmt.Fprintf(b, "\t// %s\n", line)
	}
}

func fieldDocExtra(cfg specConfig, f field) string {
	if beh, ok := cfg.behaviors[f.key]; ok {
		switch beh.kind {
		case behaviorAlwaysEmit:
			extra := "; always serialized"
			if beh.zeroFallback != "" {
				extra += fmt.Sprintf(
					", defaulting to %s from %s",
					beh.zeroFallback,
					beh.fallbackDoc,
				)
			}
			return extra
		case behaviorEmitWhenSet:
			return `; nil omits it`
		}
	}
	if f.goType == "[]string" {
		return "; serialized sorted"
	}
	return ""
}

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

func defaultDoc(f field) string {
	if f.refName == "timedelta" {
		return fmt.Sprintf("%v seconds", f.def)
	}
	if s, ok := f.def.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%v", f.def)
}

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
