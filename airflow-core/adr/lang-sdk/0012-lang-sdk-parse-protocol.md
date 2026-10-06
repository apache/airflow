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

# ADR-0012: Lang-SDK Parse Protocol — Handler Messages and Coordinator Verbs

## Status

Proposed

## Context

The Dag processor asks a Lang-SDK runtime two different questions. "Which Dags does this artifact define?" is answered over the messages [ADR-0004](0004-dag-parsing.md) already
defines. "Which task handlers does this artifact register, each for a `dag_id` Python already owns?" has no answer in those messages,
because a `TaskHandler` registration carries no Dag ([ADR-0011](0011-mixed-language-dag-processing.md)).

This ADR defines the request that carries the second question, the subprocess classes that carry both, and the two parse-side entry points on the coordinator.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec version `1.0`).

## Decision

### One channel shape, two request types

```
parse_dag                                      parse_task_handler

  parent process                                 parent process
     │  DagFileParseRequest                          │  TaskHandlerParseRequest
     ▼     (ToDagProcessor)                          ▼     (ToSDKTaskHandlerProcessor)
  coordinator    (execs the runtime)              coordinator    (execs the runtime)
     ▼                                               ▼
  runtime                                         runtime
     │  DagFileParsingResult                          │  TaskHandlerParsingResult
     ▼     (ToManager)                                ▼     (ToManager)
  parent process                                 parent process
```

Neither verb forwards bytes: in a child of the process that asked, the coordinator builds the command and execs the runtime, which connects back to that process.
The coordinator never decodes the payload, and the process that spawned the parse decodes the reply. They stay two methods, not one `parse(request)`,
because a coordinator can serve handlers without serving native Dag parsing. Appendix A has the longer argument.

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
`DagFileProcessorProcess` and `LangSDKDagFileProcessorProcess`; a Dag-parsing child fills it for `LangSDKTaskHandlerProcessorProcess`,
relaying each request that needs a client up its own `ToManager` channel unchanged.
That relay is only type-safe because both hops speak the same pair, which is the reason not to mint a separate `ToCoordinator`.

### Message shapes

```
class TaskHandlerParseRequest:
    file: str                          # the artifact resolved for this coordinator
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
    binding: Literal["positional", "named"]   # how stub-task arguments bind to params
    params: list[TaskHandlerParam] | None     # ordered; the order matters only for "positional". None: the runtime cannot list them

class TaskHandlerParam:
    name: str | None                   # None: the runtime has no name for this positional parameter
    value_schema: ArgValueSchema | None = None
    exact_name: bool = False           # match as spelled, not case-insensitively with underscores ignored
```

The request names no Dags. The runtime answers with every task handler the artifact registers, keyed by `dag_id`, and with `{}` when it registers none.
The answer must depend only on the artifact, never on the request, so one answer serves every Dag whose stub tasks resolve to that artifact.
Its keys need not match the parsed file's Dags and can include Dags of other files.
Each stub task is checked only against the answer for its own coordinator and Dag ([ADR-0011](0011-mixed-language-dag-processing.md)).

`value_schema` reuses the `ArgValueSchema` definition `arg_bindings` already carries ([ADR-0007](0007-taskflow-across-language-boundary.md)), so both sides of a comparison are the
same type. Two properties matter to validation: the field is nullable on both sides, and each declaration names its binding mode. Appendix B says what that forces.

`task_handlers` is the counterpart to `DagFileParsingResult.serialized_dags`, but fully typed. `serialized_dags` is `list[LazyDeserializedDAG]`, which is an opaque object in the
schema snapshot. A handler declaration carries no Dag, so it code-generates and schema-validates in every SDK, and nothing on this path needs a DagSerialization implementation.

### Parse processes

```
WatchedSubprocess
  └── BaseDagFileProcessorProcess              socket lifecycle · ToManager decoding · Get* handling
        │                                      · log forwarding under dag_processor.*
        ├── DagFileProcessorProcess                                   (shipped)
        │     target = _parse_file_entrypoint
        │     DagFileParseRequest → DagFileParsingResult
        │
        ├── LangSDKDagFileProcessorProcess                            (new, ADR-0010)
        │     target = _start_runtime_entrypoint
        │       └── coordinator.parse_dag(): exec the runtime, which connects back
        │     DagFileParseRequest → DagFileParsingResult
        │
        └── LangSDKTaskHandlerProcessorProcess                        (new, ADR-0011)
              target = _start_task_handler_runtime_entrypoint
                └── coordinator.parse_task_handler(): the same exec
              TaskHandlerParseRequest → TaskHandlerParsingResult
```

`BaseDagFileProcessorProcess` holds what every parse process shares: the comm socket, the `ToManager` decode, the `Get*` dispatch,
and the `task.` → `dag_processor.` log-forwarder rename. Each subclass supplies the first message it sends, the result it collects, and the target its child runs.

The two Lang-SDK processes have one shape.
Their child finds the coordinator, reports the runtime's schema version and execs the runtime, which connects back to two sockets the process owns and answers the request itself.

Answering `Get*` needs a `Client`, which only the manager holds. A process with a client answers directly, and one without answers with an error.
The exception is `LangSDKTaskHandlerProcessorProcess`: it runs in a Dag-parsing child, so it relays each such request up `SUPERVISOR_COMMS`.

### Coordinator interface

```
BaseCoordinator                          execution_time/coordinator.py
  └── execute_task                       (shipped)
        │
SubprocessCoordinator                    coordinators/_subprocess.py
  implements the three verbs; each resolves (command, subprocess_schema_version)
  from a command builder, and execute_task also owns the socket lifecycle:
  ├── execute_task                        (shipped)
  ├── parse_dag                           (new, native Dags, ADR-0010)
  ├── parse_task_handler                  (new, handlers, ADR-0011)
  ├── _build_execute_task_command         (shipped)
  ├── _build_parse_dag_command            (new)
  ├── _build_parse_task_handler_command   (new)
  └── _find_task_handler_artifact         (new, called by the Dag processor: the artifact a stub task of a Dag runs, found as execute_task finds it)
        │
JavaCoordinator · ExecutableCoordinator · NodeCoordinator
  supply the hooks; no socket or protocol code
```

Names follow the shipped `execute_task` / `_build_execute_task_command` pair and supersede ADR-0004's `run_dag_parsing` / `dag_parsing_cmd`. Each command builder returns its own
`subprocess_schema_version`, so handler parsing negotiates the schema the same way task execution does.

Only a `SubprocessCoordinator` can parse, so `BaseCoordinator` gains no parse verb.
`parse_task_handler` refuses a runtime whose schema version is older than `TASK_HANDLER_PARSING_SCHEMA_VERSION`, since it cannot answer the request.

## Consequences

- The handler channel is typed; the Dag channel is opaque. Every SDK gets generated models for the declaration shape.
- Each runtime implements a second request type instead of a new field on the existing one. A `DagRef` and a `TaskHandlerRef` have different shapes, so one request/result pair
  could not carry both.
- `ToSDKTaskHandlerProcessor` extends the set of unions the supervisor schema package introspects, from four to five. It adds two union members — `TaskHandlerParseRequest` and
  `TaskHandlerParsingResult`, the latter carrying `TaskHandlerDeclaration` and `TaskHandlerParam` as nested definitions — because the shared responses are classes the registry
  already holds.
- Nothing in the protocol distinguishes a coordinator-backed parse from a Python one. A runtime's `Get*` request is answered by the same handlers that answer a Python parser's,
  through however many relay hops lie between it and the manager.
- `DagFileProcessorProcess` becomes a subclass. Its public surface does not move, but the shipped `_handle_request` and socket code shifts to `BaseDagFileProcessorProcess`.
- Neither verb is reached through ADR-0004's `can_handle_dag_file` scan. `parse_dag` is reached through the importer registered for the artifact's extension
  ([ADR-0010](0010-native-dag-processing.md)); `parse_task_handler` through `queue → coordinator` ([ADR-0011](0011-mixed-language-dag-processing.md)).
- Where the Dag processor starts its children with exec instead of fork (the default on macOS),
  a probe started from a Dag-parsing child inherits that child's ORM-blocking environment and dies at `import airflow`,
  so the check logs a warning and its stub tasks stay unchecked.
- The parent death signal reaches only the exec'd runtime, so a runtime must exec and leave no children: what it starts survives an abrupt kill of the Dag-parsing child,
  though normal exits and timeouts kill what it leaves in its process group.
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler` lands here too.

## References

- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge, `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `TaskArgBinding` / `ArgValueSchema`
- [ADR-0010](0010-native-dag-processing.md) — who calls `parse_dag`
- [ADR-0011](0011-mixed-language-dag-processing.md) — who calls `parse_task_handler`, and what it compares the reply against
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- `airflow-core/src/airflow/dag_processing/processor.py` — `DagFileProcessorProcess`, `ToManager` / `ToDagProcessor`
- `task-sdk/src/airflow/sdk/execution_time/schema/` — supervisor schema version bundle, union registry, generated snapshot
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A — Why two verbs, and why one reply union

The two verbs differ in what they ask for and in which command starts the runtime, not in how they talk. Collapsing them into one `parse(request)` would hide a real deployment
distinction: a coordinator can serve mixed-language handlers with no interest in native Dag parsing, and an absent method states that better than a runtime rejection does.

The reply direction does not need the same split. An earlier draft gave handler parsing its own `ToRuntime` / `ToCoordinator` pair on the theory that the runtime was answering the
coordinator rather than the manager. It is not: the runtime connects back to whichever process spawned the parse, and the coordinator decodes nothing.
Giving that peer two unions to decode would mean two `CommsDecoder` configurations, two relay paths for the identical `Get*` traffic,
and two registry entries for bodies that never differ. Reusing `ToManager` leaves one reply union with one new member.

A boolean on `DagFileParseRequest` was the other alternative. It cannot work: a `DagRef` and a `TaskHandlerRef` are different payloads, not two subsets of one, so the flag would
select between shapes the result type cannot both hold.

### Appendix B — What the nullable, ordered parameter list forces

`value_schema` is nullable on both sides. An unannotated `@task.stub` parameter produces `value_schema: null` today, so validation compares schemas only where neither side is null,
and otherwise checks only how the argument binds, by position or by name. A strict comparison would turn every untyped stub argument into a parse error.

The stub side always has names and positions: `LiteralArgBinding` and `XComArgBinding` are each documented as "one positional stub-task argument".
The handler side binds the way its runtime does, so each declaration names its `binding` and the check follows it:

- `positional`: by position. Names are informative only, and absent where the runtime has none (Go flat params). Java's `TaskArgs` binds this way although it has names.
  An argument count that matches `params` neither with every argument nor after dropping the defaulted ones, or a value type a parameter does not accept, is an import error.
- `named`: by name in any order, case-insensitively with underscores ignored unless `exact_name` is set (Go `arg:` tags, explicit Java names). A Go struct, tagged or untagged,
  and Java's `TaskInput` bind this way. An argument no parameter takes, or a parameter no argument fills, is logged as a warning, and the task still runs: the runtimes allow both,
  and an unfilled field keeps its default. When no parameter matches and exactly one argument was passed, it may be the whole value and is not warned about,
  unless no parameter is declared, a parameter sets `exact_name` (a Go `arg:` tag), or the argument cannot be an object.
  A value type a parameter does not accept is an import error.

`params` is `None` when the runtime cannot list a handler's parameters, and then only the handler's presence is checked.
TypeScript declares `named` with `params: None`: its types are erased, so a handler cannot list what it takes.

A declaration carries no class, method, or source location. [ADR-0006](0006-no-lang-sdk-source-display.md) rules out Lang-SDK source display, and putting it on the wire would
invite a consumer to render it. It carries no `dag_id` either — the `task_handlers` key supplies it, so a declaration cannot disagree with the bucket it arrived in.

### Appendix C — Schema registration mechanics

`registered_models_by_name()` in `task-sdk/src/airflow/sdk/execution_time/schema/` introspects a fixed set of unions — `ToTask` / `ToSupervisor` and `ToManager` / `ToDagProcessor`
— so adding `ToSDKTaskHandlerProcessor` extends that set to five. The shared responses appear in two unions now; the registry keys by class name and rejects two *distinct* classes
under one name, so a member reached twice is not a clash.

Two prek hooks guard the generated snapshot. The rule for a new body is documented in `task-sdk/src/airflow/sdk/execution_time/schema/AGENTS.md`.

Command builders read artifact roots through `_get_scan_roots()`, which raises outside the scope `_set_scan_roots` opens.
`execute_task` opens it with the roots it resolves for its mode, and `parse_dag` and `parse_task_handler` with the root of the Dag bundle that holds the file,
so both parse-side commands get roots with no `TaskInstance` in hand. [ADR-0010](0010-native-dag-processing.md) covers the modes themselves.
