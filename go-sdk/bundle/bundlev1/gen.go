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
// local gen tool: the TaskSpec struct and its SchemaFields omit-if-default
// rules come from the "operator" definition, so they cannot drift from what
// the scheduler deserializes. Don't edit spec.gen.go by hand; change the
// schema (or the field allowlist in ./gen) and re-run `just generate-specs`
// (go generate). DagSpec and the registration Info structs stay hand-written
// in spec.go: Schedule is an SDK-level concept the schema has no scalar for,
// and several dag keys are always-emitted with [core]-config fallbacks the
// schema cannot express.
package bundlev1

//go:generate go run ./gen -schema ../../../airflow-core/src/airflow/serialization/schema.json -out spec.gen.go
