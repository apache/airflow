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

# ADR-0002: DAG Parsing — Language-Specific DAG File Processing

## Status

Accepted

## Context

Airflow's standard DAG file processor only understands Python files. To support DAGs defined in other languages (Java, Go, Rust, etc.), the pipeline needs an extension point where a language-specific processor can intercept the parsing request, delegate to an external runtime, and return a result in the same format the Airflow scheduler expects.

This ADR details the DAG parsing side of the coordinator architecture described in [ADR-0001](0001-java-sdk-airflow-integration.md). It starts with the generic model — the abstract contracts and expected behavior that any language must implement — then walks through Java as a concrete example.

## Decision

### Extension Point: `BaseCoordinator`

A single abstract base class — `BaseCoordinator` — handles both DAG parsing and task execution. It is registered in `provider.yaml` under `coordinators`. For DAG parsing, a subclass must implement two methods:

| Method | Signature | Responsibility |
|---|---|---|
| `can_handle_dag_file` | `(bundle_name, path) -> bool` | Return `True` if this coordinator should handle the given file. Default returns `False`; subclasses add language-specific checks (e.g., "is this a JAR with a Main-Class?"). |
| `dag_parsing_cmd` | `(dag_file_path, bundle_name, bundle_path, comm_addr, logs_addr) -> list[str]` | Return the full command to launch the language runtime. `comm_addr` and `logs_addr` are `host:port` strings the process must connect to. |

### Registration

In the provider's `provider.yaml`:

```yaml
process-coordinators:
  - airflow.providers.sdk.<lang>.coordinator.<LangCoordinator>
```

A single registration covers both DAG parsing and task execution — there are no separate `dag-file-processors` or `task-coordinators` keys.

### Discovery: `_resolve_processor_target()`

When `DagFileProcessorProcess.start()` needs to parse a file:

```
_resolve_processor_target(path, bundle_name, bundle_path)
  for each class_path in ProvidersManager().coordinators:
    coordinator_cls = import_string(class_path)
    if coordinator_cls.can_handle_dag_file(bundle_name, path):
      return functools.partial(coordinator_cls.run_dag_parsing, path=..., bundle_name=..., bundle_path=...)
  return None  # fall back to default Python parser
```

The first coordinator whose `can_handle_dag_file()` returns `True` wins. If none match, the default Python `_parse_file_entrypoint` runs.

### What the Base Class Handles Automatically

The matched coordinator's `run_dag_parsing()` (a concrete method on `BaseCoordinator`) delegates to `_runtime_subprocess_entrypoint()`, which handles all the TCP/process plumbing:

1. Creates two TCP servers on `127.0.0.1` with random ports (comm + logs)
2. Creates a stderr socketpair
3. Calls `dag_parsing_cmd()` to get the command
4. Spawns the subprocess with `stdin=DEVNULL` (does NOT inherit fd 0)
5. Accepts TCP connections from the subprocess
6. Wraps fd 0 as `supervisor_comm` via `os.dup(0)`
7. Runs `_bridge()` — a raw byte forwarder between fd 0 and the TCP comm socket

### Expected E2E Flow

```
Airflow Dag-Processor
  │
  ▼
DagFileProcessorProcess.start(path, bundle_name, bundle_path)
  │
  ├─ _resolve_processor_target()
  │   └─ iterates process-coordinators from provider.yaml
  │   └─ first can_handle_dag_file() == True wins
  │
  ▼
WatchedSubprocess.start(target=coordinator.run_dag_parsing)
  │
  [fork — child process gets fd 0 as Unix domain socket to supervisor]
  │
  ▼ (in child)
<Lang>Coordinator.run_dag_parsing(path, bundle_name, bundle_path)
  │
  ▼
BaseCoordinator._runtime_subprocess_entrypoint(DagParsingInfo)
  │
  ├─ 1. Create TCP comm_server + logs_server on 127.0.0.1:random
  ├─ 2. Create stderr socketpair
  ├─ 3. Call dag_parsing_cmd() → get launch command
  ├─ 4. Popen(cmd, stdin=DEVNULL, stderr=child_stderr)
  ├─ 5. Accept TCP connections from the language runtime
  ├─ 6. supervisor_comm = socket(fileno=os.dup(0))
  └─ 7. _bridge() — raw byte forwarding until process exits
```

### Expected Message Sequence

Once the bridge is running, the Airflow supervisor and the language runtime communicate directly through the bridge (raw bytes, no re-encoding):

```
Airflow Supervisor                    Bridge              Language Runtime
      │                                 │                       │
      ├── DagFileParseRequest ──────────┼──────────────────────►│
      │   [4-byte len][msgpack frame]   │  raw byte forward     │
      │                                 │                       │
      │                                 │                       ├── parse DAGs from
      │                                 │                       │   bundle/file
      │                                 │                       │
      │◄── DagFileParsingResult ────────┼───────────────────────┤
      │   [4-byte len][msgpack frame]   │  raw byte forward     │
      │                                 │                       │
      │                                 │                       └── exit(0)
      │                                 │
      │                                 └── drain remaining bytes (5s deadline)
      │                                     close all sockets
```

### DagFileParsingResult Format

The language runtime must produce a `DagFileParsingResult` that matches Python Airflow's DagSerialization format exactly. The Airflow scheduler deserializes this into its internal model — any divergence causes parsing failures.

**Envelope:**

```
{
  "type": "DagFileParsingResult",
  "fileloc": "<source file path>",
  "serialized_dags": [
    {
      "data": {
        "__version": 3,
        "dag": { <serialized DAG> }
      }
    },
    ...
  ]
}
```

**Serialized DAG structure** (version 3):

| Field | Type | Required | Description |
|---|---|---|---|
| `dag_id` | string | yes | Unique identifier |
| `fileloc` | string | yes | Source file path (can be empty) |
| `relative_fileloc` | string | yes | Relative source path (can be empty) |
| `timezone` | string | yes | Always `"UTC"` |
| `timetable` | `{__type, __var}` | yes | Schedule timetable (see below) |
| `tasks` | list | yes | Serialized task list |
| `dag_dependencies` | list | yes | Empty list for non-Python DAGs |
| `task_group` | map | yes | Flat root task group |
| `edge_info` | map | yes | Empty map |
| `params` | list | yes | DAG-level parameters |
| `description` | string | if set | |
| `start_date` | float (epoch) | if set | Unwrapped from `__type`/`__var` |
| `end_date` | float (epoch) | if set | Unwrapped from `__type`/`__var` |
| `tags` | list | if non-empty | Unwrapped from `__type`/`__var` |
| `catchup` | bool | if `true` | |
| `max_active_tasks` | int | if non-default | |
| `max_active_runs` | int | if non-default | |

**Timetable encoding:**

| Schedule | `__type` | `__var` |
|---|---|---|
| `null` | `airflow.timetables.simple.NullTimetable` | `{}` |
| `@once` | `airflow.timetables.simple.OnceTimetable` | `{}` |
| `@continuous` | `airflow.timetables.simple.ContinuousTimetable` | `{}` |
| cron expr | `airflow.timetables.trigger.CronTriggerTimetable` | `{expression, timezone, interval, run_immediately}` |

**Task encoding:**

```
{
  "__type": "operator",
  "__var": {
    "task_id": "<id>",
    "task_type": "<class simple name>",
    "_task_module": "<fully qualified package>",
    "downstream_task_ids": ["<sorted dependent ids>"]  // only if non-empty
  }
}
```

**Value type encoding** (for complex fields):

| Type | Encoding |
|---|---|
| datetime | `{"__type": "datetime", "__var": <epoch_seconds_float>}` |
| timedelta | `{"__type": "timedelta", "__var": <total_seconds_float>}` |
| dict | `{"__type": "dict", "__var": {k: serialize(v), ...}}` |
| set | `{"__type": "set", "__var": [sorted_items]}` |
| list | `[serialize(item), ...]` (no wrapper) |
| primitives | pass through unchanged |

**Non-decorated vs decorated fields:** Some fields (like `start_date`, `end_date`, `tags`) are "non-decorated" — they are serialized with `__type`/`__var` wrapping but then unwrapped to just the `__var` value before inclusion in the DAG dict. Other fields (like `default_args`, `access_control`) are "decorated" — they keep the `__type`/`__var` wrapper. This matches Python's `serialize_to_json` behavior.

### What a Language Provider Must Implement

For DAG parsing, a new language provider needs:

1. **A `BaseCoordinator` subclass** with:
   - `can_handle_dag_file()` — language-specific file detection (e.g., "is this a JAR?", "is this a .go file?")
   - `dag_parsing_cmd()` — returns the command to launch the runtime

2. **A runtime process** that:
   - Accepts `--comm=host:port` and `--logs=host:port` CLI arguments
   - Connects to both TCP addresses
   - Reads a `DagFileParseRequest` msgpack frame from the comm channel
   - Parses the DAGs from the bundle
   - Serializes the result to DagSerialization v3 format
   - Sends back a `DagFileParsingResult` msgpack frame
   - Exits

3. **Registration** in `provider.yaml` under `process-coordinators`

### Java as a Concrete Example

**JavaCoordinator:**

The Java provider implements all DAG-parsing contracts in a single `BaseCoordinator` subclass:

```python
# providers/sdk/java/coordinator.py
class JavaCoordinator(BaseCoordinator):
    sdk = "java"

    @classmethod
    def can_handle_dag_file(cls, bundle_name, path) -> bool:
        # Returns True when path is a JAR with a Main-Class manifest entry
        with contextlib.suppress(FileNotFoundError):
            return find_main_class(Path(path)) is not None
        return False

    @classmethod
    def dag_parsing_cmd(cls, *, dag_file_path, bundle_name, bundle_path, comm_addr, logs_addr):
        main_class = find_main_class(Path(dag_file_path))
        return [
            "java",
            "-classpath",
            f"{bundle_path}/*",
            main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
```

`can_handle_dag_file()` checks that the file is a JAR with a `Main-Class` in its manifest. This ensures the coordinator only claims files it can actually handle.

The classpath is `<bundle_path>/*` — a wildcard that includes all JARs in the directory (the application JAR plus its dependencies).

No separate `JavaDagFileProcessor` class is needed — `BaseCoordinator` consolidates file detection, DAG parsing, and task execution into a single extension point.

**Java SDK Bundle Process:**

The Java bundle process (`Server.kt`) starts, connects to both TCP servers, and enters `CoordinatorComm.startProcessing()`. When it receives a `DagFileParseRequest`:

```
CoordinatorComm.handleIncoming(frame)
  │
  ├── frame.body is DagFileParseRequest
  │     file: String  ← the path from the request
  │
  ▼
DagParser(request.file).parse(bundle)
  │
  ├── Returns DagParsingResult(fileloc=file, dags=bundle.dags)
  │   The DAGs were already loaded into the Bundle at startup
  │   via DagBundle.getDags()
  │
  ▼
sendMessage(frame.id, result)
  │
  ├── CoordinatorComm.encode(OutgoingFrame(id, result))
  │     ├── detects DagParsingResult type
  │     └── calls result.serialize()  ← Serde.kt
  │
  ├── DagParsingResult.serialize()
  │     ├── Wraps each DAG: {"data": {"__version": 3, "dag": dag.serialize(id)}}
  │     ├── Dag.serialize() produces the full v3 format:
  │     │     timetable, tasks, task_group, params, optional fields...
  │     ├── Task.serialize() wraps as {"__type": "operator", "__var": {...}}
  │     └── serializeValue() handles datetime/timedelta/dict/set encoding
  │
  ├── TaskSdkFrames.encodeRequest(id, serializedMap)
  │     ├── Converts map to msgpack: [id, body]
  │     └── Returns byte array
  │
  └── Writes [4-byte length prefix][msgpack payload] to comm channel

shutDownRequested = true  ← one-shot, process will exit
```

**Java SDK DagBundle Interface:**

Bundle authors implement `DagBundle` to define their DAGs:

```java
public class JavaExample implements DagBundle {
    @Override
    public List<Dag> getDags() {
        var dag = new Dag("java_example", null, "@daily");
        dag.addTask("extract", Extract.class, List.of());
        dag.addTask("transform", Transform.class, List.of("extract"));
        dag.addTask("load", Load.class, List.of("transform"));
        return List.of(dag);
    }

    public static void main(String[] args) {
        var example = new JavaExample();
        var bundle = new Bundle(
            JavaExample.class.getPackage().getImplementationVersion(),
            example.getDags()
        );
        Server.create(args).serve(bundle);
    }
}
```

The `Dag` class provides a fluent API:

- `dagId`, `description`, `schedule` (cron or preset), `startDate`, `endDate`, and all standard Airflow DAG parameters
- `addTask(id, taskClass, dependsOn)` — registers a task and its upstream dependencies
- Dependencies are stored as a `dependants` map (parent → set of children), serialized as `downstream_task_ids`

**Java SDK Serialization Compatibility:**

The serialization in `Serde.kt` is validated against Python's output:

```bash
# 1. Java generates serialized output
./gradlew sdk:test
# → writes validation/serialization/serialized_java.json

# 2. Python generates the same DAGs
uv run validation/serialization/serialize_python.py \
    validation/serialization/test_dags.yaml \
    validation/serialization/serialized_python.json

# 3. Field-by-field comparison
uv run validation/serialization/compare.py \
    validation/serialization/serialized_python.json \
    validation/serialization/serialized_java.json
```

Both share test cases defined in `test_dags.yaml`, ensuring the Java SDK produces byte-identical output to Python's `DagSerialization.serialize_dag()` for the same inputs.

## Consequences

- The DAG file processor can be extended to any language without modifying Airflow Core — only a provider with a `BaseCoordinator` subclass is needed.
- The language runtime must produce exact DagSerialization v3 JSON, requiring cross-language validation infrastructure (e.g., `test_dags.yaml` + `compare.py`).
- The base class absorbs all TCP/process plumbing, so language providers only implement two methods for DAG parsing.
- The subprocess bridge adds latency and a process boundary; DAG parsing for non-Python files is inherently slower than in-process Python parsing.
