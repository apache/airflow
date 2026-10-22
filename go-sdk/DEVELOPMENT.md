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

# Developing the Go SDK

This guide is for contributors to the Go SDK. To use the SDK, see the [README](./README.md).

## Building and testing

The [`Justfile`](./Justfile) has the common tasks:

```bash
just test              # run the unit tests
just build             # pack the example bundles
just docs              # serve the package docs locally
just generate-models   # regenerate the coordinator-protocol models
just generate-specs    # regenerate the Dag, task and task group specs
```

## How it works

The bundle binary speaks the coordinator protocol when Airflow launches it with `--comm` and `--logs`.

```
Python supervisor / task runner
        │  finds + validates the bundle, then forks it:
        ▼
  <bundle binary> --comm=127.0.0.1:P1 --logs=127.0.0.1:P2
        │  binary dials BACK over TCP loopback (msgpack-over-IPC)
        ▼
  GetConnection / GetVariable / GetXCom / SetXCom ... → SucceedTask / TaskState
```

- The Python `ExecutableCoordinator` forks the bundle binary with `--comm`/`--logs` addresses it is already
  listening on. The binary dials back (it never listens) and speaks a length-prefixed msgpack-over-IPC wire
  protocol on the comm socket, with structured JSON-line logs on the logs socket.
- The Python runtime is the worker. It proxies every `GetConnection` / `GetVariable` / `GetXCom` /
  `SetXCom` call through to the Execution API. The Go binary just runs the task function.
- When the first frame on the comm socket is a `DagFileParseRequest` from the Dag processor, the
  binary runs no task. It answers with one `DagFileParsingResult` that holds the Dags from
  `airflow.Dag` that the binary registered. The Dags are serialized as
  [Serializing a native Dag](#serializing-a-native-dag) describes. The binary then exits.

The Go side of the protocol is implemented in `pkg/execution/`. On the Python side it is the
`ExecutableCoordinator` in `task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py`.

## The vendored schemas

`schema/dag-schema.json` and `schema/supervisor-schema.json` are byte-for-byte copies of schemas
airflow-core and task-sdk own. The generators read the copies, not the originals, so a generated file
is explainable from a file inside this module and a standalone checkout — a published Go module, an
ASF source release — can regenerate and test without the monorepo around it. `ts-sdk` and `java-sdk`
vendor theirs the same way.

Two prek hooks split the work that keeps the copies honest. `sync-go-sdk-schemas` copies an original
over its copy when the two differ and fails, since copying is mechanical; it is a **manual** hook, so
re-vendoring is a deliberate step here rather than something a change to a Python schema forces on
whoever made it — which also means nothing announces on its own that a copy has gone stale.
`check-go-sdk-generated-drift` runs on every commit and regenerates from the copies, failing when the
committed Go differs, since what to do about a new schema construct — a generator rule, an authoring
exclusion — is a decision.

## Regenerating the coordinator-protocol models

The types in `pkg/execution/genmodels/` are generated from `schema/supervisor-schema.json`, this
module's vendored copy of the supervisor schema snapshot the Python Task SDK owns
(`task-sdk/src/airflow/sdk/execution_time/schema/schema.json`); do not edit either by hand. To move
the SDK to a newer schema version:

1. Refresh the copy: `prek run sync-go-sdk-schemas --hook-stage manual`, which overwrites
   `schema/supervisor-schema.json` from task-sdk's and fails so the change lands in review.
2. Set `SupervisorSchemaVersion` in [`pkg/execution/messages.go`](./pkg/execution/messages.go) to the
   copy's `api_version` date.
3. Run `just generate-models`, and commit the copy with what it generated.

`TestSupervisorSchemaVersionMatchesSnapshot` fails when the constant and the copy's `api_version`
drift, so a missed bump is caught by `go test`. A snapshot can also grow a field without the
`api_version` moving, which leaves the models behind with nothing failing — msgpack drops a field the
struct does not declare — so the `check-go-sdk-generated-drift` prek hook regenerates them and fails
when the committed files differ.

[`airflow/enums.go`](./airflow/enums.go) declares the `genmodels.DagRunState` constants again, so that
a Dag author does not import `genmodels`. `TestDagRunStateMatchesGenmodels` fails when regenerating
the models adds, renames or removes a `DagRunState` constant. The test keeps failing until
`airflow/enums.go` declares the same constants as `genmodels`.

## Regenerating the Dag, task and task group specs

`airflow.DagSpec`, `airflow.TaskSpec` and `airflow.TaskGroupSpec` in
[`airflow/spec.gen.go`](./airflow/spec.gen.go) are generated from `schema/dag-schema.json`, this
module's vendored copy of airflow-core's Dag serialization schema
(`airflow-core/src/airflow/serialization/schema.json`), which Python owns; do not edit any of them
by hand. Refresh the copy with `prek run sync-go-sdk-schemas --hook-stage manual`,
then run `just generate-specs` after changing the schema or the generator.

The schema is the serialized shape rather than the authoring one, so
[`internal/genspec/authoring.go`](./internal/genspec/authoring.go) holds the three tables that turn
it into the authoring shape, each entry carrying the reason it exists:

- **exclusions** — properties an author never sets, such as the paths the bundle fills in and the
  template fields of a Python operator class. A property *not* excluded generates, so one added on
  the Python side surfaces in review instead of vanishing.
- **type overrides** — the schema types a moment in time and a duration as a number of seconds, and
  an integral count as a JSON number.
- **injections** — `Schedule`, which stands in for the serialized `timetable`, and the `Queue` of
  `DagSpec`, for which the schema has no Dag-level property. Injecting into the schema rather than
  hand-writing the field keeps every field in one struct declaration, which is what lets
  `TaskSpec` implement the sealed `TaskOption`.

The schema types `trigger_rule` and `weight_rule` as plain strings and does not list their values.
`TriggerRule`, `WeightRule` and their constants are therefore hand-written in
[`airflow/enums.go`](./airflow/enums.go). `TestRuleConstantsMatchPython` checks those constants
against the Python enums in airflow-core.

`just generate-specs` also writes [`airflow/spec_fields.gen.go`](./airflow/spec_fields.gen.go), which
holds the schema key and the schema default of each field, keyed by the name of the Go field.

The `check-go-sdk-generated-drift` prek hook regenerates both files and fails when a committed one
differs, so a schema change that never reached Go cannot merge.

## Serializing a native Dag

[`airflow/serialize.go`](./airflow/serialize.go) writes a registered `airflow.Dag` as the Dag JSON
that Airflow stores. Python's `DagSerialization` writes the same shape for a Python Dag. Each field
of `DagSpec`, `TaskSpec` and `TaskGroupSpec` has an entry in a generated table, which names the
schema property that the field sets and its schema default. The serializer leaves out a value equal to
the default. `serialize.go` lists only the fields that the generated tables cannot describe: the ones
it writes in its own way, and the lists that Python keeps in a set.

`airflow.Inputs` takes the results of tasks and `airflow.Literal` values, one for each parameter after
the `Context`. A task becomes an `xcom` argument binding and a literal becomes a `literal` one:

```go
dag.Task(load, airflow.Inputs(transformed, airflow.Literal("s3://bucket/out")))
```

```json
[
  {"name": "arg0", "kind": "xcom", "task_id": "transform"},
  {"name": "arg1", "kind": "literal", "value": "s3://bucket/out"}
]
```

A literal must be JSON that decodes into its parameter type, and it adds no edge to the Dag.

The `check-go-sdk-serialization-conformance` prek hook builds the Dags of
[`scripts/ci/lang_sdk_serialization/test_dags.yaml`](../scripts/ci/lang_sdk_serialization/test_dags.yaml)
twice: with this SDK, and with Python and Airflow's own serializer. It loads the Go output with
Airflow's deserializer and compares the two serializations field by field. Run it from the
repository root:

```bash
prek run check-go-sdk-serialization-conformance --all-files
```

The hook runs every Dag of the file, including the ones that `requires` a feature that only some SDKs
have. [`go_native.json`](../airflow-core/tests/unit/dag_processing/lang_sdk_fixtures/go_native.json)
holds the Go output for the same Dags, so that the Airflow core tests check that Airflow loads it.
`TestSerializeConformanceDags` says how to rewrite it.

## Architectural decisions

The [`adr/`](./adr) directory records the design decisions behind the SDK:

- [ADR 0001](./adr/0001-bundle-packing-options.md): bundle-packing options.
- [ADR 0002](./adr/0002-use-go-tool-directive-for-bundle-packer.md): deliver the bundle packer via the
  Go 1.24 `tool` directive.
- [ADR 0003](./adr/0003-coordinator-protocol-msgpack-ipc.md): the msgpack-over-IPC coordinator protocol.
- [ADR 0004](./adr/0004-self-contained-executable-bundle.md): the self-contained executable bundle, where
  the executable *is* the bundle.
- [ADR 0005](./adr/0005-retire-go-edge-worker.md): retire the standalone Go Edge Worker and make the
  coordinator the only execution path.
- [ADR 0006](./adr/0006-cross-language-argument-binding.md): cross-language TaskFlow argument binding,
  from serialized literals and XCom references to typed Go parameters.
- [ADR 0007](./adr/0007-mixed-lang-task-handler-interface.md): bundle registration and the Mixed Lang
  task handler interface — `airflow.Bundle`/`Register`/`Serve`; Cross language TaskFlow: flat positional binding, `arg:` tagged structs, and the untagged folded-name fallback.
- [ADR 0008](./adr/0008-native-dag-interface.md): the proposed Native Dag interface (`airflow.Dag`/
  `dag.Task`/`airflow.Inputs`/`Before`-`After`)

Cross-cutting Lang-SDK decisions — the coordinator architecture and how non-Python tasks integrate with
Airflow core surfaces — are recorded in [`airflow-core/adr/lang-sdk/`](../airflow-core/adr/lang-sdk).
Two of them shape the interfaces above:
[ADR-0008](../airflow-core/adr/lang-sdk/0008-control-flow-constructs.md) for grouping, conditions,
branching, and triggering a Dag run, and
[ADR-0009](../airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md) for reaching
Python provider operators from a native Dag.

The normative, language-agnostic on-disk bundle format (the footer layout, manifest fields, and what the
`ExecutableCoordinator` reads) is specified in
[`executable-bundle-spec.rst`](https://github.com/apache/airflow/blob/main/task-sdk/docs/executable-bundle-spec.rst).
`airflow-go-pack` produces bundles conforming to that spec.

## Future Direction

This is more of an "it would be nice to have" than any plan or commitment, and a place to record ideas.

- The ability to run Airflow tasks "in" an existing code base, i.e. defining an Airflow task function that
  runs (in a goroutine) inside an existing app.
- Doing the task function reflection ahead of time, rather than for each Execute call.
