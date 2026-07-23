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

// spec.gen.go is generated from the Airflow dag-serialization schema by the
// local gen tool: the TaskSpec and DagSpec field sets, their Go types, and
// their SchemaFields emit rules derive from the "operator" and "dag"
// definitions, so they cannot drift from what the scheduler deserializes.
// Don't edit spec.gen.go by hand; change the schema (or the per-definition
// config in ./gen) and re-run `just generate-specs` (go generate). What the
// schema cannot express is declared in that config and validated against the
// schema on every run: the always-emit dag keys with their [core]-config
// fallbacks, the nullable is_paused_upon_creation *bool, sorted tags, and
// the SDK-only Schedule field the serializer turns into the timetable
// object. Only the registration Info structs stay hand-written in spec.go:
// they carry Go-side identity the schema knows nothing about.
package bundlev1

//go:generate go run ./gen -schema ../../../airflow-core/src/airflow/serialization/schema.json -out spec.gen.go
