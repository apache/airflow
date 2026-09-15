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

# ADR-0010: Lang-SDK Parse Protocol — Handler Messages and Coordinator Verbs

## Status

Proposed

## Context

The Dag processor asks a Lang-SDK runtime two different questions. "Which Dags does this artifact define?" is answered over the messages [ADR-0004](0004-dag-parsing.md) already
defines. "Which task handlers does this artifact register for a `dag_id` Python already owns?" has no answer in those messages, because a `TaskHandler` registration carries no Dag
([ADR-0009](0009-mixed-language-dag-processing.md)).

This ADR defines the request that carries the second question, the subprocess classes that carry both, and the two parse-side entry points on the coordinator.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec version `1.0`).

## Decision

### One channel shape, two request types

```
parse_dag                                      parse_task_handler

  parent process                                 parent process
     │  DagFileParseRequest                          │  TaskHandlerParseRequest
     ▼     (ToDagProcessor)                          ▼     (ToSDKTaskHandlerProcessor)
  coordinator    (raw byte forward)               coordinator    (raw byte forward)
     ▼                                               ▼
  runtime                                         runtime
     │  DagFileParsingResult                          │  TaskHandlerParsingResult
     ▼     (ToManager)                                ▼     (ToManager)
  parent process                                 parent process
```

Both verbs are byte forwarders: the coordinator spawns the runtime, wires `fd 0` to the comm socket, and never decodes the payload. The process that spawned the parse decodes the
reply. They stay two methods, not one `parse(request)`, because a coordinator can serve handlers without serving native Dag parsing. Appendix A has the longer argument.

### The reply travels on `ToManager`

```
_ParseSideResponses =                       shared tail — same members, same `type` discriminator
    ConnectionResult | VariableResult | VariableKeysResult | TaskStatesResult
  | PreviousDagRunResult | PreviousTIResult | PrevSuccessfulDagRunResult
  | ErrorResponse | OKResponse | XComCountResponse | XComResult
  | XComSequenceIndexResult | XComSequenceSliceResult

ToDagProcessor             = DagFileParseRequest     | _ParseSideResponses      parent → child
ToSDKTaskHandlerProcessor  = TaskHandlerParseRequest | _ParseSideResponses      parent → child   (new)

ToManager                  = DagFileParsingResult | TaskHandlerParsingResult    child → parent
                           | GetConnection | GetVariable | … | MaskSecret
```

`ToSDKTaskHandlerProcessor` is the only new union; `ToManager` gains one member. The two parent → child unions differ in exactly one member, because the child's questions about
connections, variables and XComs do not depend on which parse it was asked for.

`ToManager` is named for the process that usually holds the other end, but the role it describes is "whoever spawned this parse". The Dag processor manager fills it for
`DagFileProcessorProcess`; a Dag-parsing child fills it for the two processes below, relaying anything that is not a parsing result up its own `ToManager` channel unchanged. That
relay is only type-safe because both hops speak the same pair, which is the reason not to mint a separate `ToCoordinator`.

### Message shapes

```
class TaskHandlerParseRequest:
    file: str                          # the artifact resolved for this coordinator
    dag_ids: list[str]                 # every Dag in the parsed file with stub tasks that resolved here
    bundle_path: Path
    bundle_name: str
    type: Literal["TaskHandlerParseRequest"]

class TaskHandlerParsingResult:
    fileloc: str
    task_handlers: dict[str, list[TaskHandlerDeclaration]]   # dag_id → its declarations
    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["TaskHandlerParsingResult"]

class TaskHandlerDeclaration:
    task_id: str
    params: list[TaskHandlerParam]     # ordered — arg_bindings are positional

class TaskHandlerParam:
    name: str
    value_schema: ArgValueSchema | None = None
    required: bool                     # the handler declares no default
```

One request carries every `dag_id` that resolved to the same artifact under the same coordinator, so a file whose stubs all target one runtime costs one process. A `dag_id` the
artifact registers nothing for is **omitted** from `task_handlers` rather than returned empty: the key set is not required to match `dag_ids`, because it is the union across
coordinators that has to cover the stubs ([ADR-0009](0009-mixed-language-dag-processing.md)).

`value_schema` reuses the `ArgValueSchema` definition `arg_bindings` already carries ([ADR-0007](0007-taskflow-across-language-boundary.md)), so both sides of a comparison are the
same type. Two properties matter to validation: the field is nullable on both sides, and `params` is ordered. Appendix B says what that forces.

`task_handlers` is the counterpart to `DagFileParsingResult.serialized_dags`, but fully typed. `serialized_dags` is `list[LazyDeserializedDAG]`, which is an opaque object in the
schema snapshot. A handler declaration carries no Dag, so it code-generates and schema-validates in every SDK, and nothing on this path needs a DagSerialization implementation.

### Parse processes

```
WatchedSubprocess
  └── BaseParsingProcess                       socket lifecycle · ToManager decoding · Get* handling
        │                                      · log forwarding under dag_processor.*
        ├── DagFileProcessorProcess                                   (shipped, now a subclass)
        │     │   target = _parse_file_entrypoint
        │     │   DagFileParseRequest → DagFileParsingResult
        │     │
        │     └── LangSDKDagFileProcessorProcess                      (new — ADR-0008)
        │           target = _parse_lang_sdk_dag_entrypoint
        │             └── coordinator.parse_dag() — spawn runtime, forward fd 0 ⇄ comm socket
        │           same request and result types as its base class
        │
        └── SDKTaskHandlerProcessorProcess                            (new — ADR-0009)
              target = _parse_task_handler_entrypoint
                └── coordinator.parse_task_handler() — same forwarding
              TaskHandlerParseRequest → TaskHandlerParsingResult
```

`BaseParsingProcess` is `DagFileProcessorProcess` minus the Dag-specific request and result: the comm socket, the `ToManager` decode, the `Get*` dispatch, and the
`task.` → `dag_processor.` log-forwarder rename. The subclasses supply the first message they send, the result they collect, and the target the child runs.

`LangSDKDagFileProcessorProcess` differs from its base in the target callable alone. Everything else — the request, the result, the socket, the logging — is inherited, because a
native Lang-SDK Dag answers the same question a Python file does.

Answering `Get*` needs a `Client`, which only the manager holds. `BaseParsingProcess` therefore resolves a request one of two ways: directly against `self.client` when the manager
is the parent, or by relaying it up `SUPERVISOR_COMMS` when a Dag-parsing child is.

### Coordinator interface

```
BaseCoordinator                          execution_time/coordinator.py
  ├── execute_task                       (shipped)
  ├── parse_dag                          (new — native Dags, ADR-0008)
  └── parse_task_handler                 (new — handlers, ADR-0009)
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
- `ToSDKTaskHandlerProcessor` extends the set of unions the supervisor schema package introspects, from four to five. It adds two union members — `TaskHandlerParseRequest` and
  `TaskHandlerParsingResult`, the latter carrying `TaskHandlerDeclaration` and `TaskHandlerParam` as nested definitions — because the shared responses are classes the registry
  already holds.
- Nothing in the protocol distinguishes a coordinator-backed parse from a Python one. A runtime's `Get*` request is answered by the same handlers that answer a Python parser's,
  through however many relay hops lie between it and the manager.
- `DagFileProcessorProcess` becomes a subclass. Its public surface does not move, but the shipped `_handle_request` and socket code shifts to `BaseParsingProcess`.
- Neither verb is reached through ADR-0004's `can_handle_dag_file` scan. `parse_dag` is reached through the importer registered for the artifact's extension
  ([ADR-0008](0008-native-dag-processing.md)); `parse_task_handler` through `queue → coordinator` ([ADR-0009](0009-mixed-language-dag-processing.md)).
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler` lands here too.

## References

- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge, `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `TaskArgBinding` / `ArgValueSchema`
- [ADR-0008](0008-native-dag-processing.md) — who calls `parse_dag`
- [ADR-0009](0009-mixed-language-dag-processing.md) — who calls `parse_task_handler`, and what it compares the reply against
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- `airflow-core/src/airflow/dag_processing/processor.py` — `DagFileProcessorProcess`, `ToManager` / `ToDagProcessor`
- `task-sdk/src/airflow/sdk/execution_time/schema/` — supervisor schema version bundle, union registry, generated snapshot
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A — Why two verbs, and why one reply union

The two verbs differ in what they ask for and in which command starts the runtime, not in how they talk. Collapsing them into one `parse(request)` would hide a real deployment
distinction: a coordinator can serve mixed-language handlers with no interest in native Dag parsing, and an absent method states that better than a runtime rejection does.

The reply direction does not need the same split. An earlier draft gave handler parsing its own `ToRuntime` / `ToCoordinator` pair on the theory that the runtime was answering the
coordinator rather than the manager. It is not: the coordinator forwards bytes in both directions and decodes nothing, so the peer at the far end of the socket is whichever process
spawned the parse. Giving that peer two unions to decode would mean two `CommsDecoder` configurations, two relay paths for the identical `Get*` traffic, and two registry entries for
bodies that never differ. Reusing `ToManager` leaves one reply union with one new member.

A boolean on `DagFileParseRequest` was the other alternative. It cannot work: a `DagRef` and a `TaskHandlerRef` are different payloads, not two subsets of one, so the flag would
select between shapes the result type cannot both hold.

### Appendix B — What the nullable, ordered parameter list forces

`value_schema` is nullable on both sides. An unannotated `@task.stub` parameter produces `value_schema: null` today, so validation compares schemas only where neither side is null,
and falls back to name-and-arity otherwise. A strict comparison would turn every untyped stub argument into a parse error.

`params` is ordered because `LiteralArgBinding` and `XComArgBinding` are each documented as "one positional stub-task argument". Position is part of the contract, not incidental,
and both sides bind positionally.

A declaration carries no class, method, or source location. [ADR-0006](0006-no-lang-sdk-source-display.md) rules out Lang-SDK source display, and putting it on the wire would
invite a consumer to render it. It carries no `dag_id` either — the `task_handlers` key supplies it, so a declaration cannot disagree with the bucket it arrived in.

### Appendix C — Schema registration mechanics

`registered_models_by_name()` in `task-sdk/src/airflow/sdk/execution_time/schema/` introspects a fixed set of unions — `ToTask` / `ToSupervisor` and `ToManager` / `ToDagProcessor`
— so adding `ToSDKTaskHandlerProcessor` extends that set to five. The shared responses appear in two unions now; the registry keys by class name and rejects two *distinct* classes
under one name, so a member reached twice is not a clash.

Two prek hooks guard the generated snapshot. Their interaction on a first-introduction body needs checking against the hooks rather than against that package's `AGENTS.md`: the doc
says no `VersionChange` is required for a new body, while `check-supervisor-schemas-versions` fails when the snapshot moves and nothing under `versions/` was touched.

Artifact roots are resolved per mode by `_init_root_source`, but published through `_get_scan_roots()`, which is scoped to an active task and raises outside one. Both parse-side
commands need those roots with no `TaskInstance` in hand, so the scope that publishes them has to open for a parse as well as for a task. [ADR-0008](0008-native-dag-processing.md)
covers the modes themselves.
