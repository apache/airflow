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

# ADR-0001: Java SDK Airflow Integration

## Status

Accepted

## Context

Airflow's current execution model is Python-only: DAGs are Python files, tasks are Python
callables, and the supervisor communicates with the forked task process via UNIX domain
socketpairs. To support tasks authored in other languages (starting with Java), we need an
architecture that:

- Allows non-Python tasks to coexist with Python tasks in the same DAG via `@task.stub`.
- Reuses the existing task-runner infrastructure so Airflow extensions (XCom backends,
  connections, variables) stay in Python.
- Is extensible to other languages (Go, TypeScript, etc.) without per-language changes to
  Airflow Core.

The only missing piece is a way for the task runner to hand off execution to a
foreign-language process and still drive the same API-call lifecycle.

## Decision

### Writing a Non-Python Task

There are two ways to author a Java task, both producing a task class the SDK runtime can
discover and execute.

#### Interface-Based

Implement the `Task` interface with `execute(Context context, Client client)`. `Context`
provides static run-time data (logical date, run ID, etc.), and `Client` provides access to
Airflow services (connections, variables, XCom):

```java
public static class Extract implements Task {
    public void execute(Context context, Client client) throws Exception {
        var connection = client.getConnection("test_http");
        client.setXCom(new Date().getTime());
    }
}

public static class Transform implements Task {
    public void execute(Context context, Client client) {
        var extractXcom = client.getXCom("extract");
        client.setXCom(new Date().getTime());
    }
}

public static Dag build() {
    var dag = new Dag("java_interface_example");
    dag.addTask("extract", Extract.class);
    dag.addTask("transform", Transform.class);
    return dag;
}
```

#### Annotation-Based

Use `@Builder.Dag`, `@Builder.Task`, and `@Builder.XCom` annotations on a class and its
methods. The SDK's annotation processor generates a `<ClassName>Builder` class with `Task`
implementations at build time. XCom inputs declared via `@Builder.XCom` are fetched
automatically; non-`void` return values are pushed as XCom.

```java
@Builder.Dag(id = "java_annotation_example")
public class AnnotationExample {

    @Builder.Task(id = "extract")
    public long extractValue(Client client) throws InterruptedException {
        var connection = client.getConnection("test_http");
        Thread.sleep(6000);
        return new Date().getTime();  // automatically pushed as XCom
    }

    @Builder.Task(id = "transform")
    public long transformValue(Client client,
                               @Builder.XCom(task = "extract") long extracted) {
        // `extracted` is pulled from the "extract" task XCom automatically
        return new Date().getTime();
    }

    @Builder.Task
    public void load(@Builder.XCom(task = "transform") long transformed) {
        throw new RuntimeException("I failed");
    }
}
```

The generated `AnnotationExampleBuilder.build()` returns a fully configured `Dag`. The
annotation-based interface is generally preferred for new code because it eliminates
boilerplate and makes XCom data-flow explicit in method signatures.

Both approaches register tasks with `BundleBuilder.getDags()` and are served by the same
`Server` entry point:

```java
public class ExampleBundleBuilder implements BundleBuilder {
    @Override
    public Iterable<Dag> getDags() {
        return List.of(InterfaceExampleBuilder.build(), AnnotationExampleBuilder.build());
    }

    public static void main(String[] args) {
        var bundle = new ExampleBundleBuilder().build();
        Server.create(args).serve(bundle);
    }
}
```

### Integrating Non-Python Tasks into a DAG: `@task.stub`

DAG authors declare a non-Python task in a Python DAG file using `@task.stub` and specify a
queue. Python and Java tasks coexist in the same pipeline; the DAG remains defined in Python:

```python
@task()
def python_task_1():
    return "value_from_python_task_1"


@task.stub(queue="java")
def extract(): ...


@task.stub(queue="java")
def transform(): ...


@task()
def python_task_2(transformed):
    print(transformed)


@dag(dag_id="java_interface_example")
def simple_dag():
    python_task_1() >> extract() >> transform() >> python_task_2()
```

The `@task.stub` declarations carry no Python implementation — execution is delegated to
the coordinator identified by the task's `queue`.

### Public API Surface: `Client` and `Context`

The Java task interface is `void execute(Context context, Client client)`. Two design choices
warrant explanation.

**Why both `Context` and `Client`?** The Java SDK exposes two objects, mirroring the Go SDK:

| Object | Holds | Lifecycle |
|---|---|---|
| `Context` | Static run-time data (`ds`, `ti`, logical date, run-id, etc.) | Populated once from `StartupDetails`, read-only during execution |
| `Client` | Active accessors that perform Execution API calls (connections, variables, XCom) | Each method call is a synchronous request/response over the comm channel |

In Python, magic objects on the context (e.g., `outlet_events`) can perform Execution API
calls transparently because of the language's flexibility. Java is more rigid; making
`Context` itself perform background API calls would require significantly more wiring without
much user-visible benefit. Splitting the two surfaces makes the API call boundary explicit at
the type level.

**Why is `execute` `void`?** Returning a value from `execute` would imply an automatic XCom
push. Java's static type system does not have a clean equivalent of Python's "return any
object, get a default-keyed XCom" pattern, and explicit `client.setXCom(...)` calls keep the
wire-level behavior obvious. (The annotation-based interface infers XCom pushes from
non-`void` return types, providing the same convenience without losing type clarity.)

### Coordinator Interface and Code Reuse

`BaseCoordinator`, defined in
`task-sdk/src/airflow/sdk/execution_time/coordinator.py`, exposes a single `execute_task`
method. Subclasses implement this to start the language-specific subprocess and return when
it finishes (with exit code and final task state).

The Task SDK provides two coordinator implementations alongside `BaseCoordinator`:

- **`_PythonCoordinator`** (built-in, not user-configurable) — implements `execute_task` by
  calling `ActivitySubprocess.start()`, which creates UNIX domain socketpairs and forks a
  child Python process. The child inherits the request socket on fd 0 and uses the existing
  task-runner main function. This is the path taken for all Python tasks today.

- **`JavaCoordinator`** (in `airflow.sdk.coordinators.java`) — implements `execute_task` by
  creating two TCP server sockets on `127.0.0.1`, spawning the JVM bundle process via
  `subprocess.Popen`, and waiting for the Java process to connect back to those servers.
  It uses `_JavaActivitySubprocess`, a subclass of `ActivitySubprocess`, so the request
  handling, heartbeating, and state management logic is fully shared with the Python path.

The key design benefit: `ActivitySubprocess` owns the supervisor-side event loop
(heartbeating, API request proxying, state management). Both `_PythonCoordinator` and
`JavaCoordinator` create a subprocess and hand it an `ActivitySubprocess` instance; only the
subprocess start-up and socket establishment differ. Adding a third language requires
implementing `execute_task` in a new `BaseCoordinator` subclass, with no changes to Airflow
Core.

The `client` parameter passed to `execute_task` is the already-authenticated Execution API
client. It is passed through to `ActivitySubprocess`, which uses it to forward the subprocess's
API requests (getVariable, getConnection, setXCom, etc.) to the API server.

### Supervisor–Subprocess Communication

Python tasks and Java tasks use different channels between the supervisor and the task
subprocess:

**Python tasks** — the supervisor creates UNIX domain socketpairs before forking. The child
process inherits the request socket on fd 0 (and stdout/stderr on fd 1/2 via separate
socketpairs). No network stack is involved.

**Java tasks** — the coordinator creates two TCP server sockets on `127.0.0.1` (one for the
msgpack comm channel, one for structured logs), then spawns the JVM process via
`subprocess.Popen`. The Java process connects *back* to those servers. From that point on,
the supervisor drives the same msgpack-framed request/response exchange as with Python tasks,
over TCP instead of UNIX sockets.

The Java SDK process is agnostic to transport — it sees a TCP socket carrying msgpack frames
and behaves identically regardless of whether the other end is a Python supervisor or any
other implementation of the same protocol.

### The Coordinator Layer

When a task is dispatched, `CoordinatorManager` (in the same module as `BaseCoordinator`)
resolves the task's `queue` to a registered coordinator instance and calls `execute_task`.

The Java coordinator ships as part of the Task SDK and is importable as
`airflow.sdk.coordinators.java.JavaCoordinator`. The `airflow.sdk.coordinators` namespace
package is structured to allow future separation into standalone distributions without
changing import paths. For packaging and registration details, see
[ADR-0005](0005-coordinator-packaging.md).

### Architecture Overview

```
        Airflow Backend                           Language Runtime Subprocess (Java in this example)
        ───────────────                           ──────────────────────────────────────────────────

┌──────────────────────────────┐
│  DAG File (Python)           │
│                              │
│  @task.stub(queue="java")    │
│  def my_java_task():         │
│      ...                     │
└──────────────┬───────────────┘
               │ (standard Python parsing)
┌──────────────▼───────────────┐
│  Metadata DB                 │
│                              │
│  task_instance.queue = "java"│
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│  Scheduler                   │
│                              │
│  Reads queue from TI         │
│  ──► ExecuteTask workload    │
│      (includes queue)        │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐                    ┌──────────────────────────────┐
│  Execution API               │                    │  Runtime Subprocess (Java)   │
│                              │                    │                              │
│  TI.queue ──► Startup        │                    │  execute_task() starts JVM   │
│                   Details    │                    │  process, accepts TCP conn   │
└──────────────┬───────────────┘                    │                              │
               │                                    └──────────────▲───────────────┘
┌──────────────▼───────────────┐                                   │ TCP
│  Supervisor                  │                                   │
│                              │                                   │
│  CoordinatorManager          │                                   │
│  resolves queue via          │                                   │
│  [sdk] queue_to_coordinator  ┼───────────────────────────────────┘
│  → JavaCoordinator           │
└──────────────────────────────┘
```

### Java Coordinator Configuration

`JavaCoordinator` (in `task-sdk/src/airflow/sdk/coordinators/java/coordinator.py`) accepts
three configuration parameters via `kwargs`:

| Parameter | Default | Description |
|---|---|---|
| `java_executable` | `"java"` | Path to the `java` binary |
| `jvm_args` | `[]` | Extra JVM arguments (e.g. `["-Xmx1024m"]`) |
| `jars_root` | `[]` | Directories scanned for the bundle JAR (`Main-Class` manifest entry is the entry point) |

### Integration Points — Required Changes

**1. Decorator — DAG Author Interface**

DAG authors declare a non-Python task using `@task.stub` and specify a queue:

```python
@task.stub(queue="java")
def my_java_task(): ...
```

**2. Execution API — Task Queues Routed to the Worker**

`[sdk] coordinators` is a JSON object keyed by coordinator name. Each entry supplies a
`classpath` (resolved via `import_string`) and free-form `kwargs` passed to the class
constructor. `[sdk] queue_to_coordinator` maps queue names to those keys:

```ini
[sdk]
coordinators = {
    "jdk-17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {"java_executable": "java", "jars_root": ["/opt/airflow/jars"]}
    }
}
queue_to_coordinator = {"java": "jdk-17"}
```

Tasks on the `java` queue are routed to the entry named `jdk-17`. Multiple entries with
the same `classpath` (e.g. `jdk-11` and `jdk-17`) are independent instances with different
`kwargs`; there is no subclassing needed for per-runtime variants.

For the full configuration schema and multi-JDK examples, see
[ADR-0005](0005-coordinator-packaging.md).

### Implementation Language: Kotlin (with a Java-First Public API)

The user-facing API surface (`Task`, `Client`, `Context`, `Dag`, `BundleBuilder`) is
published as Java types and is the contract bundle authors program against. The SDK
*implementation* — `CoordinatorComm`, `Server`, `Task.kt`, `Frame.kt` — is written in
Kotlin.

Kotlin compiles to the same JVM bytecode as Java and is fully interoperable, so this choice
is invisible to bundle authors at runtime. The practical reasons for using Kotlin internally:

- **Null safety** is part of the type system, removing a large class of latent NPEs in the
  comm/serde paths.
- **Coroutines and structured I/O** simplify the synchronous-over-async pattern used by
  `Client.getVariable()` and friends.
- **Less boilerplate** in serialization and frame encoding code, which is the bulk of the SDK.

Because the user-facing API is Java, "Java SDK" remains the accurate name from a DAG-author
perspective. A future rename to "JVM SDK" has been floated but is not adopted here; it can be
revisited if/when Scala or other JVM-language bindings are proposed.

## Consequences

### New Interfaces

| Component | New Interface | Change Type |
|-----------|--------------|-------------|
| `BaseCoordinator` | Abstract base with single `execute_task` hook, defined in Task SDK | New class |
| `airflow.sdk.coordinators` | Namespace package for language coordinator modules | New namespace |
| `@task.stub` decorator | `queue: str \| None` parameter | Additive |
| `[sdk] coordinators` | Airflow configuration: JSON object of named coordinator entries | New option |
| `[sdk] queue_to_coordinator` | Airflow configuration mapping queue name → coordinator entry key | New option |
| `CoordinatorManager.for_queue` | Resolves queue → coordinator, falls back to `_PythonCoordinator` | New code path |

### What Becomes Easier

- Adding a new language runtime requires only a `BaseCoordinator` subclass and a corresponding
  entry in `[sdk] coordinators` — no changes to Airflow Core and no provider plumbing.
- DAG authors can mix Python and non-Python tasks in the same pipeline.
- The existing `ActivitySubprocess` infrastructure (heartbeating, state management, API
  request proxying) is reused for all language runtimes.

### What Becomes Harder

- Each non-Python task involves an additional subprocess and a TCP connection to it.
- Debugging non-Python tasks requires understanding the communication between the supervisor
  and the language runtime.
