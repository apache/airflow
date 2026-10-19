<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 5. Retire the Go Edge Worker

Date: 2026-08-20

## Status

Accepted. Supersedes the dual-runtime portion of
[ADR 0003](0003-coordinator-protocol-msgpack-ipc.md).

## Context

The Go SDK supported two task-execution architectures. The standalone Go Edge
Worker polled the Edge Executor API, started bundle subprocesses through
HashiCorp `go-plugin` and gRPC, and communicated with the Execution API itself.
The coordinator architecture instead lets the existing Python supervisor start
one bundle subprocess per task and proxy all Airflow service calls.

Maintaining both paths duplicated task lifecycle, logging, configuration,
authentication, bundle discovery, and transport code. The standalone path also
lacked capabilities already supplied by the supervisor, including remote task
logging, alternate XCom backends, and the complete task-state lifecycle.

## Decision

Remove the standalone Go Edge Worker and its plugin protocol. The Go SDK uses
`ExecutableCoordinator` as its only task-execution architecture.

This removes the worker command, Edge API client, worker runtime, bundle
discovery and handshake code, gRPC protocol, worker log server, and their
configuration and dependencies. Bundle binaries require paired `--comm` and
`--logs` arguments for execution; `--airflow-metadata` remains available to the
bundle packer.

## Consequences

- Go task execution inherits the Python supervisor's lifecycle, logging, XCom,
  and Execution API behavior.
- Deployments no longer install or configure a separate Go worker process.
- Existing standalone deployments must rebuild their bundle packages with
  `airflow-go-pack` unless they are already packed, move the resulting bundles into an
  `ExecutableCoordinator.executables_root`, and route the Go task queue through
  the coordinator. The metadata footer on a packed bundle is required for
  coordinator discovery.
- Bundle providers can remove the now-unused `GetBundleVersion` method;
  `RegisterDags` and the `airflow-go-pack` workflow otherwise remain unchanged.
- The Go SDK no longer carries a direct Execution API client or depends on
  `go-plugin`, gRPC, protobuf, Viper, Resty, or the worker-only logging and
  authentication libraries.
