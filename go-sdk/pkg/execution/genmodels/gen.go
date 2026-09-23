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

// Package genmodels holds the Go data models for the coordinator-protocol IPC
// messages (msgpack-over-socket) exchanged between the Airflow supervisor and a
// Go SDK bundle in coordinator mode.
//
// These files are generated from the supervisor wire-schema snapshot owned by the
// Python Task SDK (task-sdk/src/airflow/sdk/execution_time/schema/schema.json):
// models.gen.go holds the struct types, discriminators.gen.go the Type<Name>
// constants and EnsureType, defaults.gen.go the DecodeMsgpack methods that seed
// non-zero schema defaults. Don't edit them by hand; change the Pydantic models,
// let the generate-supervisor-schemas-snapshot prek hook refresh the snapshot,
// then re-run `just generate-models` (go generate).
//
// go generate runs go-jsonschema, then the local gen tool, which strips
// go-jsonschema's dead anyOf-branch typedefs, widens concrete int/float/bool
// fields whose schema default their Go zero value does not satisfy to pointers
// (so an unset value is omitted on the wire and the supervisor reapplies the
// default, while an explicit 0/false still encodes), and emits
// discriminators.gen.go and defaults.gen.go.
package genmodels

//go:generate go run github.com/atombender/go-jsonschema@v0.23.1 --only-models --struct-name-from-title --tags msgpack --capitalization ID --capitalization URI --capitalization TI -p genmodels -o models.gen.go ../../../../task-sdk/src/airflow/sdk/execution_time/schema/schema.json
//go:generate go run ./gen -schema ../../../../task-sdk/src/airflow/sdk/execution_time/schema/schema.json -models models.gen.go -out discriminators.gen.go -defaults defaults.gen.go
