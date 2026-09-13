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

# ADR-0008: Lang-SDK Parse Protocol — Handler Messages and Coordinator Verbs

## Status

Proposed

## Context

The Dag processor needs two different answers from a Lang-SDK runtime, and they
are not variations of one question:

- **Which Dags does this artifact define?** Answered by the runtime standing in
  for the parser process, over the manager ↔ parser messages
  [ADR-0004](0004-dag-parsing.md) already defines.
- **Which task handlers does this artifact register for a `dag_id` Python
  already owns?** A `TaskHandler` registration carries no Dag
  ([ADR-0010](0010-mixed-language-dag-processing.md)), so the answer is not a
  serialized Dag and there is nothing in the existing messages to carry it.

This ADR defines the channel and payload for the second question, and names
both parse-side entry points on the coordinator.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec
version `1.0`): `Dag`, `TaskHandler`, `DagRef`, `TaskHandlerRef`, `bundle`,
`register`, `serve`.

## Decision

### Handler Declarations Travel on Their Own Channel

Handler declarations are carried by a dedicated message pair in their own
discriminated unions — `ToRuntime` / `ToCoordinator` — not by a field added to
`DagFileParseRequest`.

Union membership is the only registration step in the supervisor schema
package: it is what puts a body into the generated snapshot, and therefore into
every SDK's generated models. So the choice of union *is* the wire-contract
decision. Neither existing pair fits, because the runtime plays two roles. When
it parses native Dags it **replaces** the parser process and answers the
manager, which the manager ↔ parser unions already describe. When it answers a
handler query it is a short-lived child of the Python parser, and the manager is
not a peer at all. Reusing the parser unions there would make one union mean two
recipients.

The payload is deliberately a stronger contract than the one it parallels.
`DagFileParsingResult.serialized_dags` is `list[LazyDeserializedDAG]`, which
reduces in the snapshot to an opaque object — which is why every SDK
reimplements DagSerialization by hand and validates it against shared fixtures
([ADR-0004](0004-dag-parsing.md)). A handler declaration carries no Dag, so it
is fully typed, code-generated, and schema-validated in every SDK. **The
mixed-language path never asks a Lang SDK to implement DagSerialization at
all.**

Parameter schemas reuse the existing `ArgValueSchema` definition that
`arg_bindings` already carries
([ADR-0007](0007-taskflow-across-language-boundary.md)), so validation compares
like against like rather than translating between two vocabularies. Appendix A
gives the message definitions and the two properties of that field —
nullability and ordering — that validation has to respect.

### Two Parse Verbs, Because There Are Two Conversations

`BaseCoordinator` gains `parse_dag` and `parse_task_handler` alongside the
shipped `execute_task`. They are **not symmetric siblings**, which is the
reason not to collapse them into one `parse(request)`:

- `parse_dag` is **bridge mode**. The coordinator becomes the parser process
  and raw-forwards the manager's request and the runtime's reply. It never
  reads the payload.
- `parse_task_handler` is **query mode**. The coordinator is itself the peer:
  it sends one request and decodes one reply on the channel above.

One collapsed method would also erase a real deployment distinction — a
coordinator can serve mixed-language handlers with no interest in native Dag
parsing, and an absent method says so better than a runtime rejection.
Appendix B gives the layering and the command hooks.

## Consequences

- **The handler channel is typed where the Dag channel is opaque.** Every SDK
  gets generated models for the declaration shape, and nothing on this path
  requires a DagSerialization implementation.
- Each Lang-SDK runtime implements a second request type rather than a new field
  on the existing one. That is the cost of handlers not being Dags: a `DagRef`
  and a `TaskHandlerRef` have different shapes, so one request/result pair could
  not have carried both.
- Adding `ToRuntime` / `ToCoordinator` extends the set of unions the supervisor
  schema package introspects, and regenerating the snapshot is what propagates
  the new bodies into each SDK's generated models.
- Naming supersedes ADR-0004's `run_dag_parsing` / `dag_parsing_cmd`, matching
  the shipped `execute_task` / `_build_execute_task_command` pair instead.
- Terms here track Language SDK spec `1.0`. A spec rename of `TaskHandler`
  lands in this ADR too.

## References

- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge,
  `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` /
  `TaskArgBinding` / `ArgValueSchema`
- [ADR-0009](0009-native-dag-processing.md) — who calls `parse_dag`
- [ADR-0010](0010-mixed-language-dag-processing.md) — who calls
  `parse_task_handler`, and what it compares the reply against
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- `task-sdk/src/airflow/sdk/execution_time/schema/` — the supervisor schema
  version bundle, the union registry, and the generated snapshot
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix — For Implementation

### Appendix A — Message Definitions

```
ToRuntime      = TaskHandlerParseRequest      coordinator → runtime
ToCoordinator  = TaskHandlerParsingResult     runtime → coordinator
```

```
class TaskHandlerParseRequest:
    file: str                     # the artifact the coordinator resolved
    dag_id: str                   # scope: handlers bound to this dag_id only
    type: Literal["TaskHandlerParseRequest"]

class TaskHandlerParsingResult:
    fileloc: str
    task_handlers: list[TaskHandlerDeclaration]
    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["TaskHandlerParsingResult"]

class TaskHandlerDeclaration:
    dag_id: str
    task_id: str
    params: list[TaskHandlerParam]     # ordered — arg_bindings are positional

class TaskHandlerParam:
    name: str
    value_schema: ArgValueSchema | None = None
    required: bool                     # the handler declares no default
```

Two properties of `value_schema` are load-bearing for validation:

- **It is nullable on both sides.** An unannotated `@task.stub` parameter
  produces `value_schema: null` today, so validation compares schemas only
  where neither side is null and falls back to name-and-arity otherwise.
  Without that, every untyped stub argument becomes a parse error.
- **`params` is ordered.** `LiteralArgBinding` and `XComArgBinding` are each
  documented as "one positional stub-task argument", so position is part of the
  contract rather than incidental.

A declaration carries no class, method, or source location.
[ADR-0006](0006-no-lang-sdk-source-display.md) rules out Lang-SDK source
display, and putting it on the wire would invite a consumer to render it.

Registration mechanics: `registered_models_by_name()` in
`task-sdk/src/airflow/sdk/execution_time/schema/` introspects a fixed set of
unions — `ToTask` / `ToSupervisor` and `ToManager` / `ToDagProcessor` — so
adding `ToRuntime` / `ToCoordinator` extends that set. Two prek hooks guard the
snapshot, and their interaction on a first-introduction body needs checking
against the hooks rather than against that package's `AGENTS.md`, which says no
`VersionChange` is required while the second hook fails when the snapshot moves
with nothing under `versions/` touched.

### Appendix B — Coordinator Interface

```
BaseCoordinator                          execution_time/coordinator.py
  ├── execute_task                       (shipped)
  ├── parse_dag                          (new — native Dags, ADR-0009)
  └── parse_task_handler                 (new — handlers, ADR-0010)
        │
SubprocessCoordinator                    coordinators/_subprocess.py
  implements all three; each resolves (command, subprocess_schema_version)
  from a hook and owns the socket lifecycle:
  ├── _build_execute_task_command         (shipped)
  ├── _build_parse_dag_command            (new)
  └── _build_parse_task_handler_command   (new)
        │
JavaCoordinator · ExecutableCoordinator · NodeCoordinator
  supply the three commands; no socket or protocol code
```

Each hook returns its own `subprocess_schema_version`, so handler parsing
negotiates the supervisor schema through the mechanism task execution already
uses, and `None` disables migration exactly as it does today.
`can_handle_dag_file` still gates native Dag parsing only; handler queries
resolve through the coordinator registry instead.

Artifact roots are resolved per mode by `_init_root_source`, but published
through `_get_scan_roots()`, which is scoped to an active task and raises
outside one. Both parse-side commands need the same roots with no
`TaskInstance` in hand, so the scope that publishes them has to open for a
parse as well as for a task. [ADR-0009](0009-native-dag-processing.md) covers
the modes themselves.
