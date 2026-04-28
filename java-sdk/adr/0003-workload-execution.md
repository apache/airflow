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

# ADR-0003: Workload Execution — Language-Specific Task Execution

## Status

Accepted

## Context

Airflow's standard task runner executes Python callables. To support tasks written in other languages, the pipeline needs an extension point where a language-specific coordinator can intercept the execution, delegate to an external runtime process, and bridge the Task SDK protocol so the external process can access Airflow services (connections, variables, XCom) during execution.

This ADR details the task execution side of the coordinator architecture described in [ADR-0001](0001-java-sdk-airflow-integration.md). It starts with the generic model — the abstract contracts and expected behavior that any language must implement — then walks through Java as a concrete example.

## Decision

### Extension Point: `BaseCoordinator`

The same `BaseCoordinator` base class that handles DAG parsing also handles task execution. It is registered in `provider.yaml` under `coordinators`. For task execution, a subclass must implement:

| Method | Signature | Responsibility |
|---|---|---|
| `task_execution_cmd` | `(what, dag_rel_path, bundle_info, comm_addr, logs_addr) -> list[str]` | Return the full command to launch the language runtime for task execution. `comm_addr` and `logs_addr` are `host:port` strings the process must connect to. |

The base class provides `run_task_execution()` as a concrete method that handles all TCP/process plumbing automatically (same pattern as `run_dag_parsing()` for the DAG parsing side).

**Parameters passed to `run_task_execution()`:**

| Parameter | Type | Description |
|---|---|---|
| `what` | `TaskInstance` | The task instance to execute (id, dag_id, task_id, run_id, try_number, etc.) |
| `dag_rel_path` | `str \| PathLike` | Relative path to the DAG file / bundle within the bundle root |
| `bundle_info` | `BundleInfo` | Bundle name and version |
| `startup_details` | `StartupDetails` | Full startup context (task instance, DAG rel path, bundle info, run context, start date) — already consumed from fd 0 |

### Registration

The same `coordinators` entry in `provider.yaml` covers both DAG parsing and task execution — no separate registration needed:

```yaml
coordinators:
  - airflow.providers.sdk.<lang>.coordinator.<LangCoordinator>
```

### Discovery: `_resolve_runtime_entrypoint()`

When `task_runner.main()` starts, before any Python task execution:

```
task_runner.main()
  → startup_details = get_startup_details()   # reads from fd 0
  → _resolve_runtime_entrypoint(startup_details)
      for each class_path in ProvidersManagerTaskRuntime().process_coordinators:
        coordinator_cls = import_string(class_path)
        if not hasattr(coordinator_cls, "run_task_execution"):
          continue
        return functools.partial(coordinator_cls.run_task_execution,
            what=..., dag_rel_path=..., bundle_info=..., startup_details=...)
      return None  # fall back to default Python execution

  → if runtime_entrypoint is not None:
      runtime_entrypoint()   # language-specific execution
      return                # short-circuit — skip Python execution entirely
```

> **Note:** Currently the first coordinator with `run_task_execution` wins. `QueueToCoordinatorMapper` maps the task's `queue` to the correct coordinator via the `[sdk] queue_to_sdk` configuration.

### Expected E2E Flow

```
Airflow Executor (dispatches task)
  │
  ▼
WatchedSubprocess.start(target=task_runner.main)
  │
  [fork — child process gets fd 0 as Unix domain socket to supervisor]
  │
  ▼ (in child)
task_runner.main()
  │
  ├─ get_startup_details()           ← reads StartupDetails from fd 0
  │
  ├─ _resolve_runtime_entrypoint()
  │   └─ iterates coordinators from provider.yaml
  │   └─ first with run_task_execution wins
  │
  ▼
<Lang>Coordinator.run_task_execution(what, dag_rel_path, bundle_info, startup_details)
  │
  ▼
BaseCoordinator._runtime_subprocess_entrypoint(TaskExecutionInfo)
  │
  ├─ 1. Create TCP comm_server + logs_server on 127.0.0.1:random
  ├─ 2. Create stderr socketpair
  ├─ 3. Call task_execution_cmd() → get launch command
  ├─ 4. Popen(cmd, stdin=DEVNULL, stderr=child_stderr)
  ├─ 5. Accept TCP connections from the language runtime
  ├─ 6. _send_startup_details(runtime_comm, startup_details)
  │     └─ re-serializes with model_dump(mode="json") to avoid
  │        msgpack extension types non-Python decoders can't handle
  ├─ 7. supervisor_comm = socket(fileno=os.dup(0))
  └─ 8. _bridge() — raw byte forwarding until process exits
```

Key difference from DAG parsing: In task execution, `task_runner.main()` has already consumed `StartupDetails` from fd 0. The bridge must re-send `StartupDetails` to the language runtime over TCP before starting the byte-forwarding bridge. This is done via `_send_startup_details()`, which re-serializes using JSON mode to avoid msgpack extension types (like `Timestamp`) that non-Python decoders may not support.

### Expected Message Sequence

Task execution is a multi-round conversation, unlike DAG parsing's single request/response:

```
Airflow Supervisor                    Bridge              Language Runtime
      │                                 │                       │
      │     [StartupDetails sent by bridge directly]            │
      │                                 ├── StartupDetails ────►│
      │                                 │                       │
      │                                 │                       ├── Look up task
      │                                 │                       │   from bundle
      │                                 │                       │
      │                                 │   ┌───────────────────┤
      │                                 │   │ Task code runs    │
      │                                 │   │ and may request:  │
      │                                 │   │                   │
      │◄── GetConnection(conn_id) ──────┼───┤                   │
      │                                 │   │                   │
      ├── ConnectionResult ─────────────┼──►│                   │
      │                                 │   │                   │
      │◄── GetVariable(key) ────────────┼───┤                   │
      │                                 │   │                   │
      ├── VariableResult ───────────────┼──►│                   │
      │                                 │   │                   │
      │◄── GetXCom(key, dag_id, ...) ───┼───┤                   │
      │                                 │   │                   │
      ├── XComResult ───────────────────┼──►│                   │
      │                                 │   │                   │
      │◄── SetXCom(key, value, ...) ────┼───┤                   │
      │                                 │   │                   │
      ├── (empty response) ─────────────┼──►│                   │
      │                                 │   │                   │
      │                                 │   └───────────────────┤
      │                                 │                       │
      │◄── SucceedTask / TaskState ─────┼───────────────────────┤
      │   (terminal — no response)      │                       │
      │                                 │                       └── exit(0)
      │                                 │
      │                                 └── drain, close sockets
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

### What a Language Provider Must Implement

For task execution, a new language provider needs:

1. **A `BaseCoordinator` subclass** with:
   - `task_execution_cmd()` — returns the command to launch the runtime
   - (This is the same subclass that implements `can_handle_dag_file()` and `dag_parsing_cmd()` for DAG parsing — one class covers both)

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

5. **Registration** in `provider.yaml` under `coordinators` (same entry as DAG parsing — no separate registration)

### Java as a Concrete Example

**JavaCoordinator (Python side):**

The same `JavaCoordinator` that handles DAG parsing also handles task execution — no separate `JavaTaskCoordinator` class is needed:

```python
# providers/sdk/java/coordinator.py
class JavaCoordinator(BaseCoordinator):
    sdk = "java"

    @classmethod
    def can_handle_dag_file(cls, bundle_name, path) -> bool:
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

    @classmethod
    def task_execution_cmd(cls, *, what, dag_rel_path, bundle_info, comm_addr, logs_addr):
        jar_path = Path(dag_rel_path)
        main_class = find_main_class(jar_path)
        return [
            "java",
            "-classpath",
            f"{jar_path.parent}/*",
            main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
```

One class, one `provider.yaml` entry, covers both DAG parsing and task execution.

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
        var bundle = new Bundle(
            JavaExample.class.getPackage().getImplementationVersion(),
            new JavaExample().getDags()
        );
        Server.create(args).serve(bundle);  // parses --comm/--logs, connects, enters message loop
    }
}
```

The same `main()` entry point handles both DAG parsing and task execution — the first message received (`DagFileParseRequest` or `StartupDetails`) determines the mode.

**Java SDK Java-side Supervisor (Alternative Execution Path):**

The Java SDK also provides `Supervisor.kt` for execution contexts where there is no Python process (e.g., the Edge Worker). In this path, the Supervisor terminates the protocol directly instead of bridging:

```
Supervisor.run(request)
  │
  ├── Create TCP comm + logs servers
  ├── Spawn Java bundle process with --comm/--logs
  ├── Accept connections
  ├── HTTP PATCH task → running state
  ├── Send StartupDetails to bundle via comm socket
  │
  └── serveTaskSdkRequests() loop:
        Read frame from bundle
        ├── GetConnection → HTTP GET /connections/{id} → send response
        ├── GetVariable → HTTP GET /variables/{key} → send response
        ├── GetXCom → HTTP GET /xcom/... → send response
        ├── SetXCom → HTTP POST /xcom/... → send response
        └── SucceedTask/TaskState → HTTP PATCH terminal state → exit loop
```

The bundle process behaves identically in both paths — it is unaware of whether its comm channel leads to a Python bridge or a Java Supervisor. This is the core design invariant of the Java SDK.

## Consequences

- Task execution for any language reuses the same coordinator + bridge pattern as DAG parsing, keeping the extension surface small.
- The multi-round protocol (GetConnection, GetVariable, etc.) means the language runtime has full access to Airflow services without reimplementing them — they stay in Python.
- The synchronous request/response model is simple for language SDK authors but adds a round-trip per service call.
- The Java-side Supervisor (`Supervisor.kt`) provides an alternative execution path for environments without Python, but requires the Java SDK to implement HTTP calls to the Execution API directly.
- Task authors interact with a simple `Client` interface, completely abstracted from the underlying socket protocol.
