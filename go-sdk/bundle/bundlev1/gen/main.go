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
// SchemaFields method, so the Go field types and the omit-if-default rules the
// serializer relies on cannot drift from the schema. Only the keys listed in
// taskSpecFields are emitted: they are the task attributes a bundle author may
// set from Go, a deliberate subset of the schema (the rest of the "operator"
// keys are serializer internals such as task_type or template_fields, or
// Python-only concerns such as templating and callbacks).
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

// fieldDef selects one schema property for the generated spec struct. goType
// overrides the mechanical schema→Go type mapping for the few keys whose
// schema type is too loose (e.g. "number" that is semantically a float).
type fieldDef struct {
	key    string
	goType string
}

// taskSpecFields lists the "operator" keys exposed on TaskSpec, in struct
// order. Adding a task attribute means adding its key here and regenerating.
var taskSpecFields = []fieldDef{
	{key: "queue"},
	{key: "pool"},
	{key: "pool_slots"},
	{key: "retries"},
	{key: "retry_delay"},
	{key: "max_retry_delay"},
	{key: "retry_exponential_backoff", goType: "float64"},
	{key: "priority_weight"},
	{key: "weight_rule"},
	{key: "trigger_rule"},
	{key: "owner"},
	{key: "execution_timeout"},
	{key: "executor"},
	{key: "start_date"},
	{key: "end_date"},
	{key: "depends_on_past"},
	{key: "wait_for_downstream"},
	{key: "do_xcom_push"},
	{key: "email_on_failure"},
	{key: "email_on_retry"},
	{key: "doc_md"},
	{key: "map_index_template"},
	{key: "max_active_tis_per_dag"},
	{key: "max_active_tis_per_dagrun"},
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

type schemaDoc struct {
	Definitions map[string]struct {
		Properties map[string]propSchema `json:"properties"`
	} `json:"definitions"`
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

	fields := make([]field, 0, len(taskSpecFields))
	for _, fd := range taskSpecFields {
		prop, ok := operator.Properties[fd.key]
		if !ok {
			log.Fatalf("gen: schema operator definition has no property %q", fd.key)
		}
		f, err := resolveField(fd, prop)
		if err != nil {
			log.Fatalf("gen: property %q: %v", fd.key, err)
		}
		fields = append(fields, f)
	}

	src, err := render(*pkg, fields)
	if err != nil {
		log.Fatalf("gen: rendering: %v", err)
	}
	if err := os.WriteFile(*outPath, src, 0o644); err != nil {
		log.Fatalf("gen: writing %s: %v", *outPath, err)
	}
}

func resolveField(fd fieldDef, prop propSchema) (field, error) {
	f := field{
		key:    fd.key,
		goName: goName(fd.key),
		def:    prop.Default,
		hasDef: prop.Default != nil,
	}
	if ref := strings.TrimPrefix(prop.Ref, "#/definitions/"); ref != prop.Ref {
		f.refName = ref
	}

	switch {
	case fd.goType != "":
		f.goType = fd.goType
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
			return field{}, fmt.Errorf("unsupported schema type %v (ref %q)", prop.Type, prop.Ref)
		}
	}
	return f, nil
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

	b.WriteString(`// TaskSpec is the optional configuration applied to a task at registration
// time. Every field is optional: the zero value (nil for the *bool fields)
// means "unset" and the scheduler falls back to the schema default. Each
// field maps to the same-named key of the "operator" definition in
// airflow-core/src/airflow/serialization/schema.json.
type TaskSpec struct {
`)
	for _, f := range fields {
		fmt.Fprintf(&b, "\t// %s maps to the schema key %q", f.goName, f.key)
		if f.hasDef {
			fmt.Fprintf(&b, " (schema default %s)", defaultDoc(f))
		}
		b.WriteString(".\n")
		fmt.Fprintf(&b, "\t%s %s\n", f.goName, f.goType)
	}
	b.WriteString("}\n\n")

	b.WriteString(`// SchemaFields returns the schema-keyed value of every field that is set and
// differs from its schema default, mirroring Python BaseSerialization's
// omission of values the scheduler re-derives. time.Duration and time.Time
// values are returned as-is; the serializer owns the wire encoding.
func (s TaskSpec) SchemaFields() map[string]any {
	m := map[string]any{}
`)
	for _, f := range fields {
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
