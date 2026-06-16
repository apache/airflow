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

// Package genmodels holds the Go data models for the messages exchanged on the
// coordinator-protocol IPC channel (msgpack-over-socket) between the Airflow
// supervisor and a Go SDK bundle running in coordinator mode.
//
// models.gen.go holds the struct types; discriminators.gen.go holds the
// Type<Name> message-type constants. Both are generated directly from the
// supervisor wire-schema snapshot owned by the Python Task SDK
// (task-sdk/src/airflow/sdk/execution_time/schema/schema.json), referenced by
// relative path from the monorepo. Do not edit either file by hand; change the
// Pydantic models on the Python side, let the generate-supervisor-schemas-snapshot
// prek hook regenerate that snapshot, then re-run `go generate ./...` (or
// `just generate-models`) here.
//
// The first directive runs go-jsonschema; the second runs the local gen tool,
// which strips go-jsonschema's dead anyOf-branch typedefs from models.gen.go and
// emits discriminators.gen.go from the schema's "type" consts.
package genmodels

//go:generate go run github.com/atombender/go-jsonschema@v0.23.1 --only-models --struct-name-from-title --tags msgpack --capitalization ID --capitalization URI --capitalization TI -p genmodels -o models.gen.go ../../../../task-sdk/src/airflow/sdk/execution_time/schema/schema.json
//go:generate go run ./gen -schema ../../../../task-sdk/src/airflow/sdk/execution_time/schema/schema.json -models models.gen.go -out discriminators.gen.go
