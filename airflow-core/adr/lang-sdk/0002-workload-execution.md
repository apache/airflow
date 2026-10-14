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

# ADR-0002: Workload Execution — Language-Specific Task Execution

## Status

Accepted

## Context

Airflow's standard task runner executes Python callables. To support tasks written in other languages, the pipeline needs an extension point where a language-specific coordinator can intercept the execution, delegate to an external runtime process, and bridge the Task SDK protocol so the external process can access Airflow services (connections, variables, XCom) during execution.

This ADR details the task execution side of the coordinator architecture described in
[ADR-0001](0001-java-sdk-airflow-integration.md). It starts with the generic model — the
abstract contracts and expected behavior that any language must implement — then walks through
Java as a concrete example.

The Python-side `BaseCoordinator` interface, `CoordinatorManager`, and the supervisor changes
needed to support them are implemented in the Task SDK alongside the Java coordinator.

## Decision

### Extension Point: `BaseCoordinator`

`BaseCoordinator` (in `task-sdk/src/airflow/sdk/execution_time/coordinator.py`) exposes a
single `execute_task` method. Subclasses implement this to start the language-specific
subprocess and block until it completes, returning an `ExecutionResult` (exit code + final
task state string).

There is no lower-level `task_execution_cmd` hook; each coordinator implementation is free to
start its subprocess however it likes. For details on the built-in Python coordinator and how
the Java coordinator reuses `ActivitySubprocess` infrastructure, see
[ADR-0001 — Coordinator Interface and Code Reuse](0001-java-sdk-airflow-integration.md#coordinator-interface-and-code-reuse).

Coordinators contribute to the `airflow.sdk.coordinators` namespace package and are activated
through `[sdk] coordinators` in `airflow.cfg` — there is no `provider.yaml` involvement.

### Registration and Discovery

Coordinators are registered in `[sdk] coordinators` and routed via `[sdk] queue_to_coordinator`
in `airflow.cfg`. See [ADR-0001 — Java Coordinator Configuration](0001-java-sdk-airflow-integration.md#java-coordinator-configuration)
for a configuration example, and [ADR-0005](0005-coordinator-packaging.md) for the full
configuration schema and rationale.

`CoordinatorManager.for_queue(ti.queue)` resolves the queue to a coordinator instance (or falls
back to `_PythonCoordinator`) and returns it to the supervisor, which then calls
`coordinator.execute_task(...)`. See
[ADR-0001 — The Coordinator Layer](0001-java-sdk-airflow-integration.md#the-coordinator-layer)
for how `CoordinatorManager` loads and caches coordinator instances.

### Expected E2E Flow

```
Airflow Executor (dispatches task)
  │
  ▼
supervise_task()                      ← supervisor process entry point
  │
  ├─ coordinator = CoordinatorManager.for_queue(ti.queue)
  │   └─ returns JavaCoordinator (or _PythonCoordinator as fallback)
  │
  ▼
coordinator.execute_task(what, dag_rel_path, bundle_info, client, ...)
  │
  │ [Python path: _PythonCoordinator]
  ├─ ActivitySubprocess.start()
  │   ├─ create UNIX domain socketpairs (requests on fd 0, stdout/stderr on fd 1/2)
  │   ├─ fork child Python process
  │   ├─ child runs task_runner main function
  │   └─ supervisor drives event loop (heartbeats, API proxying)
  │
  │ [Java path: JavaCoordinator]
  └─ _JavaActivitySubprocess.start()
      ├─ create TCP servers on 127.0.0.1:random (comm + logs)
      ├─ spawn Java bundle process via subprocess.Popen
      ├─ accept TCP connections from Java process
      ├─ send StartupDetails to Java process over comm socket
      └─ supervisor drives the same event loop as the Python path
```

For the transport details (UNIX socketpairs for Python, TCP loopback for Java) and why they
differ, see [ADR-0001 — Supervisor–Subprocess Communication](0001-java-sdk-airflow-integration.md#supervisorsubprocess-communication).

### Expected Message Sequence

Task execution is a multi-round conversation. The supervisor and the language runtime exchange
msgpack-framed messages directly over their shared channel (a UNIX socket for Python, a TCP
socket for Java):

```
Airflow Supervisor                              Language Runtime
      │                                                │
      ├── StartupDetails ────────────────────────────►│
      │                                                │
      │                                                ├── Look up task from bundle
      │                                                │
      │                          ┌────────────────────┤
      │◄── GetConnection(conn_id)┤  Task code runs    │
      ├── ConnectionResult ─────►│  and may request:  │
      │◄── GetVariable(key) ─────┤                    │
      ├── VariableResult ───────►│                    │
      │◄── GetXCom(key, ...) ────┤                    │
      ├── XComResult ───────────►│                    │
      │◄── SetXCom(key, value..) ┤                    │
      ├── (empty response) ─────►│                    │
      │                          └────────────────────┤
      │                                                │
      │◄── SucceedTask / TaskState ───────────────────┤
      │   (terminal — no response)                    │
      │                                                └── exit(0)
```

### Task SDK Protocol Messages

The language runtime exchanges these message types with the Airflow supervisor:

**Runtime → Supervisor (requests):**

| Message | Fields | Purpose |
|---|---|---|
| `GetConnection` | `conn_id` | Fetch an Airflow connection by ID |
| `GetVariable` | `key` | Fetch an Airflow variable by key |
| `GetXCom` | `key`, `dag_id`, `task_id`, `run_id`, `map_index?`, `include_prior_dates?` | Fetch an XCom value |
| `SetXCom` | `key`, `value`, `dag_id`, `task_id`, `run_id`, `map_index`, `mapped_length?` | Store an XCom value |
| `SucceedTask` | `end_date`, `task_outlets?`, `outlet_events?` | Terminal: task succeeded |
| `TaskState` | `state` (`"failed"`, `"removed"`, `"skipped"`), `end_date` | Terminal: task ended non-successfully |

**Supervisor → Runtime (responses):**

| Message | Fields | In response to |
|---|---|---|
| `ConnectionResult` | `conn_id`, `conn_type`, `host`, `schema`, `login`, `password`, `port`, `extra` | `GetConnection` |
| `VariableResult` | `key`, `value` | `GetVariable` |
| `XComResult` | `key`, `value` | `GetXCom` |
| (empty) | | `SetXCom` |
| `ErrorResponse` | `error`, `detail` | Any request that failed server-side |

**Framing:** Every message is a length-prefixed msgpack frame. Requests are `[id, body]` (2-element array); responses are `[id, body, error]` (3-element array). The `id` field correlates request/response pairs.

### Request/Response Semantics

The task execution follows a synchronous request/response pattern from the runtime's perspective:

1. The runtime sends a request frame (e.g., `GetVariable`) with an incrementing `id`
2. The supervisor reads the frame, fulfills the request (e.g., calls the Execution API), and sends back a response with the same `id`
3. The runtime blocks until it receives the response
4. This repeats for each Airflow service call the task code makes
5. When the task finishes, the runtime sends a terminal message (`SucceedTask` or `TaskState`) — no response is expected, and the process exits

### IPC Forward-Compatibility Contract

The supervisor-to-runtime IPC schema (the messages enumerated above plus `StartupDetails`) is shared between Airflow Core (Python) and every language SDK. A formal AIP for this protocol is expected as follow-up work; until then, this section pins down the rules that the Java SDK assumes and that any future SDK (Go, Rust, …) must follow.

**Codec rule (load-bearing).** Every SDK MUST configure its decoder to ignore unknown fields:

- Python side: `msgspec` / Pydantic models are forward-compatible by default.
- Java side: `TaskSdkFrames.kt` configures the Jackson `ObjectMapper` with `FAIL_ON_UNKNOWN_PROPERTIES = false`. A short comment at that call site documents that this is contract, not preference — flipping it back to the Jackson default would break forward compatibility with Core.
- Any new SDK: pick a codec configuration that mirrors this (silent drop of unknown fields).

This rule is what makes additive Core changes safe to ship without bumping a version on every SDK. The analogous trap — generated clients that emit their *own* allowlist check before the configured mapper sees the bytes — has bitten downstream Java consumers in unrelated systems; flagging the contract here makes it visible to future SDK authors.

**Change classification.**

| Change to a message | Status | Required action |
|---|---|---|
| Add a new optional field | **Non-breaking.** Decoders ignore it; old SDKs unaffected. | None. Just ship it. |
| Add a new required field | Breaking. | Deprecation cycle: ship as optional first, populate from Core, wait for SDKs to consume it, then tighten. |
| Rename a field | Breaking. | Deprecation cycle: emit both names from Core during transition. |
| Change a field's type | Breaking. | Deprecation cycle, typically via a new field name + parallel emission. |
| Remove a required field | Breaking. **Especially dangerous in Java**: `lateinit var` properties on `StartupDetails` deserialize silently and only throw `UninitializedPropertyAccessException` on first access, so the failure surfaces inside user task code rather than at the protocol boundary. | Deprecation cycle. Prefer making the field optional first, then remove after a release in which all SDKs have absorbed the change. |

**Recommended testing.** A small contract test on the SDK side should feed the decoder synthetic frames that exercise the rules above — an unknown field, a missing optional field, a `null` in an optional position — so that a future codec-config regression is caught before it reaches users. Such IPC-envelope tests are currently in the follow-up bucket.

### Runtime Lifecycle and Worker Capability

The language runtime is **ephemeral and one-process-per-task**:

- Each task instance launches its own `java -classpath <bundle>/* <MainClass> --comm=… --logs=…` (or the equivalent for another language). The lifetime of that process is the lifetime of the task. There is no pooling or warm-pool reuse.
- Parallelism on a single worker therefore equals the number of concurrently running task processes. Five concurrent Java tasks on one worker means five JVMs.
- DAG parsing has the same shape: each `DagFileProcessorProcess` child handles one parse request and exits. The language runtime spawned underneath it inherits that ephemerality.

**Worker capability is opt-in.** A worker can run a non-Python task only if the Task SDK (which includes the language coordinator module) is installed, the matching coordinator instance is declared in `[sdk] coordinators`, and the language toolchain (e.g., a JRE) is on the host. There is no requirement that every worker support every language. Routing relies on:

| Layer | Mechanism |
|---|---|
| Author intent | `@task.stub` declares `queue="java"` (or any custom queue) |
| Worker selection | The executor (Celery, Kubernetes, etc.) routes the task to a worker that consumes that queue, exactly as it does for Python tasks today |
| Runtime selection | Inside the task runner, `[sdk] queue_to_coordinator` maps the queue name to a coordinator instance name; that name is resolved against `[sdk] coordinators` to obtain the configured class and its `kwargs`; `CoordinatorManager.for_queue` instantiates the coordinator and `execute_task` is called |

The deployment model is the same one that already applies to Python providers: install what your DAGs need, on the hosts they run on. Multi-language workers are possible (install both providers and both toolchains) but not required.

**JAR / artifact version compatibility.** The Java SDK embeds its version in the bundle JAR via the `Airflow-Java-SDK-Version` manifest attribute. Validating that a bundle's SDK version matches the installed `JavaCoordinator` version at execution time is planned but not yet wired in; this is a follow-up to add before promoting the SDK out of preview.

### StartupDetails

The first message the runtime receives is `StartupDetails`, which provides full context for the task:

| Field | Type | Description |
|---|---|---|
| `ti` | `TaskInstance` | id, task_id, dag_id, run_id, try_number, dag_version_id, map_index, context_carrier |
| `dag_rel_path` | string | Relative path to the DAG file / bundle |
| `bundle_info` | `BundleInfo` | name, version |
| `start_date` | datetime | When this task attempt started |
| `ti_context` | `TIRunContext` | DAG run context (logical date, data interval, etc.) |
| `sentry_integration` | string | Sentry DSN for error reporting (optional) |

### What a Language SDK Must Implement

For task execution, a new language SDK needs:

1. **A `BaseCoordinator` subclass** with:
   - An `__init__` that accepts the kwargs declared in `[sdk] coordinators` (e.g., interpreter path, language-specific runtime flags)
   - `execute_task(...)` — starts the language-specific subprocess, drives the `ActivitySubprocess` event loop, and returns when the task finishes

2. **A runtime process** that:
   - Accepts `--comm=host:port` and `--logs=host:port` CLI arguments
   - Connects to both TCP addresses
   - Reads a `StartupDetails` msgpack frame from the comm channel
   - Looks up the task to execute from its bundle using `ti.dag_id` and `ti.task_id`
   - Executes the task, making `GetConnection`/`GetVariable`/`GetXCom`/`SetXCom` requests as needed
   - Sends `SucceedTask` on success or `TaskState("failed")` on failure
   - Exits

3. **A task interface** that user code implements (analogous to Python's `@task` decorator or `BaseOperator`)

4. **A client API** that wraps the socket protocol behind a simple interface (get_connection, get_variable, get_xcom, set_xcom) so task authors don't deal with framing

5. **Distribution** under `airflow.sdk.coordinators.<lang>` — currently shipped as part of the Task SDK; a standalone distribution is possible in the future without changing the import path

### Java as a Concrete Example

**JavaCoordinator (Python side):**

See [ADR-0001 — Coordinator Interface and Code Reuse](0001-java-sdk-airflow-integration.md#coordinator-interface-and-code-reuse)
for how `JavaCoordinator` implements `execute_task`, why it uses `_JavaActivitySubprocess`,
and how the Python and Java paths share `ActivitySubprocess` infrastructure. For configuration
parameters and an `airflow.cfg` example, see
[ADR-0001 — Java Coordinator Configuration](0001-java-sdk-airflow-integration.md#java-coordinator-configuration).

**Java SDK Task Interface:**

User task code implements a single-method interface:

```java
// sdk: org.apache.airflow.sdk.Task
public interface Task {
    void execute(Client client) throws Exception;
}
```

The `Client` provides access to Airflow services:

```java
// sdk: org.apache.airflow.sdk.Client
public class Client {
    // Access task metadata
    public StartupDetails getDetails();

    // Airflow services
    public Connection getConnection(String id);
    public Object getVariable(String key);
    public Object getXCom(String key, String dagId, String taskId, String runId, ...);
    public void setXCom(String key, Object value);  // defaults: key="return_value", dagId/taskId/runId from current task
}
```

**Java SDK Task Execution Flow:**

When the bundle process receives `StartupDetails`:

```
CoordinatorComm.handleIncoming(frame)
  │
  ├── frame.body is StartupDetails
  │     ti: TaskInstance (id, dagId, taskId, runId, tryNumber, ...)
  │     dagRelPath, bundleInfo, startDate, tiContext
  │
  ▼
TaskRunner.run(bundle, request, comm)
  │
  ├── Create Client(request, CoordinatorClient(comm))
  │     CoordinatorClient wraps the comm channel behind the Client interface
  │
  ├── Look up task class:
  │     bundle.dags[request.ti.dagId]?.tasks[request.ti.taskId]
  │     └── if not found → return TaskState("removed")
  │
  ├── Instantiate task:
  │     task.getDeclaredConstructor().newInstance()
  │
  ├── Execute:
  │     try {
  │       instance.execute(client)  ← USER TASK CODE RUNS HERE
  │       return SucceedTask()
  │     } catch (Exception e) {
  │       return TaskState("failed")
  │     }
  │
  ▼
sendMessage(frame.id, result)  ← sends SucceedTask or TaskState back
shutDownRequested = true       ← one-shot, process will exit
```

**Java SDK Airflow Service Access:**

When user task code calls `client.getVariable("my_key")`, the call chain is:

```
client.getVariable("my_key")                          // Client.kt (public SDK)
  │
  └── impl.getVariable("my_key")                      // CoordinatorClient (execution)
        │
        └── runBlocking {                              // blocks the calling thread
              comm.communicate<VariableResponse>(       // CoordinatorComm
                GetVariable(key = "my_key")
              )
            }
              │
              ├── sendMessage(nextId++, GetVariable)   // encode + write to comm socket
              │     ├── encode: [id, {"type": "GetVariable", "key": "my_key"}]
              │     └── write: [4-byte len][msgpack]
              │
              ├── processOnce(::handle)                // block until response arrives
              │     ├── read 4-byte length prefix
              │     ├── read payload
              │     └── decode: [id, {"type": "VariableResult", ...}, null]
              │
              └── return response.value                // unwrap VariableResponse
```

This is fully synchronous from the task code's perspective — `getVariable()` blocks until the supervisor responds.

**Java SDK Example Task Implementation:**

```java
public static class Extract implements Task {
    public void execute(Client client) throws Exception {
        // Read XCom from a Python task in the same DAG
        var pythonXcom = client.getXCom("python_task_1");

        // Access Airflow connections
        var connection = client.getConnection("test_http");

        // Do work...
        Thread.sleep(6000);

        // Push XCom for downstream tasks (Java or Python)
        client.setXCom(new Date().getTime());
    }
}

public static class Transform implements Task {
    public void execute(Client client) {
        // Read XCom from upstream Java task
        var extractXcom = client.getXCom("extract");

        // Access Airflow variables
        var variable = client.getVariable("my_variable");

        // Push XCom (readable by downstream Python tasks)
        client.setXCom(new Date().getTime());
    }
}

public static class Load implements Task {
    public void execute(Client client) {
        var xcom = client.getXCom("transform");
        throw new RuntimeException("I failed");
        // Exception → TaskRunner catches → sends TaskState("failed")
    }
}
```

**Java SDK Complete Bundle Entry Point:**

See [ADR-0001 — Writing a Non-Python Task](0001-java-sdk-airflow-integration.md#writing-a-non-python-task)
for the full `BundleBuilder` / `Server.create(args).serve(bundle)` pattern. From the task
execution perspective, `main()` is the JVM entry point the coordinator launches; `StartupDetails`
is the first message received, which triggers `runTask()`, and the process exits after the
terminal `SucceedTask`/`TaskState` response.

## Consequences

- Task execution for any language reuses the same coordinator pattern, keeping the extension surface small.
- The multi-round protocol (GetConnection, GetVariable, etc.) means the language runtime has full access to Airflow services without reimplementing them — they stay in Python.
- The synchronous request/response model is simple for language SDK authors but adds a round-trip per service call.
- Task authors interact with a simple `Client` interface, completely abstracted from the underlying socket protocol.
