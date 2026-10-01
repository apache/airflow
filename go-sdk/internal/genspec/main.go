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

// Command genspec rewrites Airflow core's Dag serialization schema
// (airflow-core/src/airflow/serialization/schema.json) into the schema the airflow
// package's DagSpec and TaskSpec generate from, so that the two structs are not
// hand-maintained.
//
// The schema is owned by Python and stays untouched; the rewritten copy is a build
// artifact. genspec rewrites it in two passes: shapeForAuthoring turns the
// serialized shape into the authoring shape, and normalize makes what is left
// something go-jsonschema can read. Each change is documented on the function that
// makes it.
//
// With -license it inserts the Apache header into an already generated file
// instead, which is what go-jsonschema leaves out.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/airflow/go-sdk/internal/genlicense"
)

func main() {
	schemaPath := flag.String(
		"schema",
		"",
		"path to airflow-core's Dag serialization schema.json",
	)
	outPath := flag.String("out", "", "path to write the rewritten schema to")
	licensePath := flag.String(
		"license",
		"",
		"path to a generated Go file to insert the Apache license header into, instead of rewriting a schema",
	)
	flag.Parse()

	if *licensePath != "" {
		if *schemaPath != "" || *outPath != "" {
			log.Fatal("genspec: -license rewrites no schema, so it takes neither -schema nor -out")
		}
		if err := genlicense.EnsureHeader(*licensePath); err != nil {
			log.Fatalf("genspec: adding the license header to %s: %v", *licensePath, err)
		}
		return
	}
	if *schemaPath == "" {
		log.Fatal("genspec: -schema is required")
	}
	if *outPath == "" {
		log.Fatal("genspec: -out is required")
	}

	doc, err := readSchema(*schemaPath)
	if err != nil {
		log.Fatalf("genspec: reading %s: %v", *schemaPath, err)
	}
	if err := shapeForAuthoring(doc, authoringShapes); err != nil {
		log.Fatalf("genspec: shaping %s for authoring: %v", *schemaPath, err)
	}
	if err := normalize(doc, specTitles); err != nil {
		log.Fatalf("genspec: normalizing %s: %v", *schemaPath, err)
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		log.Fatalf("genspec: encoding the normalized schema: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("genspec: creating the directory of %s: %v", *outPath, err)
	}
	if err := os.WriteFile(*outPath, append(out, '\n'), 0o644); err != nil {
		log.Fatalf("genspec: writing %s: %v", *outPath, err)
	}
}

func readSchema(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	// A schema default such as retry_delay's 300.0 would otherwise decode to
	// float64 and re-encode as 300, and go-jsonschema reads a default's JSON type.
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}
