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

The Dag processor asks a Lang-SDK runtime two different questions. "Which Dags does this artifact define?" is answered by the runtime standing in for the parser process, over the
messages [ADR-0004](0004-dag-parsing.md) already defines. "Which task handlers does this artifact register for a `dag_id` Python already owns?" has no answer in those messages,
because a `TaskHandler` registration carries no Dag ([ADR-0010](0010-mixed-language-dag-processing.md)).

This ADR defines the channel and payload for the second question, and names both parse-side entry points on the coordinator.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec version `1.0`).

## Decision

### Two conversations, two verbs

```
parse_dag — bridge mode                      parse_task_handler — query mode

  manager                                      coordinator
     │  DagFileParseRequest                       │  TaskHandlerParseRequest
     ▼                                            ▼
  coordinator   (raw byte forward)             runtime
     ▼                                            │  TaskHandlerParsingResult
  runtime                                         ▼
     │  DagFileParsingResult                   coordinator
     ▼
  manager
```

`parse_dag` forwards bytes and never reads the payload. `parse_task_handler` is itself the peer: it sends one request and decodes one reply. They are two methods, not one
`parse(request)`, because a coordinator can serve handlers without serving native Dag parsing. Appendix A has the longer argument.

### Handler declarations get their own unions

```
ToRuntime      = TaskHandlerParseRequest       coordinator → runtime
ToCoordinator  = TaskHandlerParsingResult      runtime → coordinator
```

Not a field on `DagFileParseRequest`. Union membership is the only registration step in the supervisor schema package, so picking the union is the wire-contract decision. The
existing pairs describe a runtime answering the manager; here the runtime answers the coordinator.

### Message shapes

```
class TaskHandlerParseRequest:
    file: str                          # the artifact the coordinator resolved
    dag_id: str                        # scope: handlers bound to this dag_id only
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

`value_schema` reuses the `ArgValueSchema` definition `arg_bindings` already carries ([ADR-0007](0007-taskflow-across-language-boundary.md)), so both sides of a comparison are the
same type. Two properties matter to validation: the field is nullable on both sides, and `params` is ordered. Appendix B says what that forces.

`task_handlers` is the counterpart to `DagFileParsingResult.serialized_dags`, but fully typed. `serialized_dags` is `list[LazyDeserializedDAG]`, which is an opaque object in the
schema snapshot. A handler declaration carries no Dag, so it code-generates and schema-validates in every SDK, and nothing on this path needs a DagSerialization implementation.

### Coordinator interface

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

Names follow the shipped `execute_task` / `_build_execute_task_command` pair and supersede ADR-0004's `run_dag_parsing` / `dag_parsing_cmd`. Each hook returns its own
`subprocess_schema_version`, so handler parsing negotiates the schema the same way task execution does.

## Consequences

- The handler channel is typed; the Dag channel is opaque. Every SDK gets generated models for the declaration shape.
- Each runtime implements a second request type instead of a new field on the existing one. A `DagRef` and a `TaskHandlerRef` have different shapes, so one request/result pair
  could not carry both.
- `ToRuntime` / `ToCoordinator` extends the set of unions the supervisor schema package introspects. Regenerating the snapshot is what propagates the bodies into each SDK's
  generated models.
- `can_handle_dag_file` still gates native Dag parsing only. Handler queries resolve through the coordinator registry.
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler` lands here too.

## References

- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge, `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `TaskArgBinding` / `ArgValueSchema`
- [ADR-0009](0009-native-dag-processing.md) — who calls `parse_dag`
- [ADR-0010](0010-mixed-language-dag-processing.md) — who calls `parse_task_handler`, and what it compares the reply against
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- `task-sdk/src/airflow/sdk/execution_time/schema/` — supervisor schema version bundle, union registry, generated snapshot
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A — Why two verbs and a separate channel

The runtime plays two roles, and they differ in who it is talking to. Parsing native Dags, it replaces the parser process and answers the manager, which is exactly what the manager
↔ parser unions describe. Answering a handler query, it is a short-lived child of the Python parser, and the manager is not a peer at all. Reusing the parser unions for both would
make one union mean two different recipients, and a runtime could not tell from the union which role it was in.

The same split is why the verbs stay separate. Collapsing them into one `parse(request)` would hide a real deployment distinction: a coordinator can serve mixed-language handlers
with no interest in native Dag parsing, and an absent method states that better than a runtime rejection does. The two also differ in mechanism — one is a byte forwarder that never
decodes the payload, the other decodes a reply it asked for — so the shared code is the subprocess plumbing underneath, not the public method.

A boolean on `DagFileParseRequest` was the alternative. It cannot work: a `DagRef` and a `TaskHandlerRef` are different payloads, not two subsets of one, so the flag would select
between shapes the result type cannot both hold.

### Appendix B — What the nullable, ordered parameter list forces

`value_schema` is nullable on both sides. An unannotated `@task.stub` parameter produces `value_schema: null` today, so validation compares schemas only where neither side is null,
and falls back to name-and-arity otherwise. A strict comparison would turn every untyped stub argument into a parse error.

`params` is ordered because `LiteralArgBinding` and `XComArgBinding` are each documented as "one positional stub-task argument". Position is part of the contract, not incidental,
and both sides bind positionally.

A declaration carries no class, method, or source location. [ADR-0006](0006-no-lang-sdk-source-display.md) rules out Lang-SDK source display, and putting it on the wire would
invite a consumer to render it.

### Appendix C — Schema registration mechanics

`registered_models_by_name()` in `task-sdk/src/airflow/sdk/execution_time/schema/` introspects a fixed set of unions — `ToTask` / `ToSupervisor` and `ToManager` / `ToDagProcessor`
— so adding `ToRuntime` / `ToCoordinator` extends that set.

Two prek hooks guard the generated snapshot. Their interaction on a first-introduction body needs checking against the hooks rather than against that package's `AGENTS.md`: the doc
says no `VersionChange` is required for a new body, while `check-supervisor-schemas-versions` fails when the snapshot moves and nothing under `versions/` was touched.

Artifact roots are resolved per mode by `_init_root_source`, but published through `_get_scan_roots()`, which is scoped to an active task and raises outside one. Both parse-side
commands need those roots with no `TaskInstance` in hand, so the scope that publishes them has to open for a parse as well as for a task. [ADR-0009](0009-native-dag-processing.md)
covers the modes themselves.
