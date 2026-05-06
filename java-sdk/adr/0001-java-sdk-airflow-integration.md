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

Airflow's current execution model is Python-only: DAGs are Python files, tasks are Python callables, and the task runner forks a Python process. To support DAGs and tasks authored in other languages (starting with Java), we need an architecture that:

- Allows entire DAGs to be written in a non-Python language (pure Java DAG).
- Allows non-Python tasks to coexist with Python tasks in the same DAG (`@task.stub`).
- Reuses the existing task-runner two-layer design (task-runner process + forked child process) so Airflow extensions (XCom backends, connections, variables) stay in Python.
- Is extensible to other languages (Go, Rust, etc.) without per-language changes to Airflow Core.

The existing task runner already uses a two-layer design. When an executor wants to run a task, it starts a task-runner process that talks to Airflow Core through the Execution API, and forks another process that talks to the task-runner through TCP to run the actual task code. All the Airflow extensions simply go into the task-runner process, keeping them in Python.

The only thing missing is a way for the task-runner process to run tasks in another language.

## Decision

### Writing a Non-Python Task

There is one way to write a non-Python task: implement the language SDK's task interface. For Java, this is the `Task` interface with a single `execute(Client client)` method. The `Client` provides access to Airflow services (connections, variables, XCom).

### Two Ways to Integrate Non-Python Tasks into a DAG

We provide two approaches for integrating non-Python tasks into a DAG:

**a) Pure Java DAG** — define the entire DAG in Java, with no Python file at all.
The Java SDK provides `BundleBuilder`, `Dag`, and `Task` interfaces:

```java
public class JavaExampleBuilder {

  public static class Extract implements Task {
    public void execute(Client client) throws Exception {
      var connection = client.getConnection("test_http");
      client.setXCom(new Date().getTime());
    }
  }

  public static class Transform implements Task {
    public void execute(Client client) {
      var extract_xcom = client.getXCom("extract");
      client.setXCom(new Date().getTime());
    }
  }

  @Override
  public Dag build() {
    var dag = new Dag("java_example", null, "@daily");
    dag.addTask("extract", Extract.class, List.of());
    dag.addTask("transform", Transform.class, List.of("extract"));
    return dag;
  }
}
```

**b) `@task.stub` in a Python DAG** — for mixed-language pipelines where Python and
Java tasks coexist in the same DAG. The `@task.stub` syntax is already supported for
the Go SDK; the same pattern applies to Java:

```python
@task()
def python_task_1(ti):
    ti.xcom_push(value="from-python", key="return_value")


@task.stub(queue="java")
def extract(): ...


@task.stub(queue="java")
def transform(): ...


@dag(dag_id="java_example")
def simple_dag():
    python_task_1() >> extract() >> transform()
```

Both approaches are supported in parallel. A pure Java DAG needs no Python at all for authoring. A `@task.stub` DAG requires a Python file but lets you mix Python operators and non-Python tasks in a single pipeline.

> **Note:** The current `BundleBuilder` interface used in pure Java DAGs is subject to review before the SDK reaches 1.0. Subclassing `Dag` directly may be a more natural fit and is being considered for post-OSS-integration.

### Public API Surface: `Client` and `Context`

The Java task interface is `void execute(Client client)`. Two design choices warrant explanation.

**Why `Client`, not `Context`?** The Java SDK exposes two objects, mirroring the Go SDK:

| Object | Holds | Lifecycle |
|---|---|---|
| `Context` | Static run-time data (`ds`, `ti`, logical date, run-id, etc.) | Populated once from `StartupDetails`, read-only during execution |
| `Client` | Active accessors that perform Execution API calls (connections, variables, XCom) | Each method call is a synchronous request/response over the comm channel |

In Python, magic objects on the context (e.g., `outlet_events`) can perform Execution API calls transparently because of the language's flexibility. Java is more rigid; making `Context` itself perform background API calls would require significantly more wiring without much user-visible benefit. Splitting the two surfaces makes the API call boundary explicit at the type level.

**Why is `execute` `void`?** Returning a value from `execute` would imply an automatic XCom push. Java's static type system does not have a clean equivalent of Python's "return any object, get a default-keyed XCom" pattern, and explicit `client.setXCom(...)` calls keep the wire-level behavior obvious. This is a deliberate departure from Python's `@task` semantics, not an oversight.

### Coordinator Interface: Subprocess-Based by Design

`BaseCoordinator` exposes both **low-level** hooks (`dag_parsing_cmd`, `task_execution_cmd`) and **high-level** lifecycle methods (`run_dag_parsing`, `run_task_execution`). Subclasses normally implement only the `*_cmd` callbacks; the base class owns the TCP servers, the subprocess spawn, and the I/O bridge.

This is deliberately tight coupling to a subprocess model. The reasoning:

- **DAG files written in a programming language have side effects.** Airflow already isolates Python parsing and task execution into child processes; the coordinator interface preserves that invariant for any non-Python language.
- **`*_cmd` is the smallest possible contract for a new language.** A new SDK only needs to translate "you're being asked to parse this file / run this task" into an OS-level launch command. Everything else (TCP plumbing, framing, byte forwarding) is shared.
- **High-level overrides are still available.** A coordinator that wants to bypass the subprocess model entirely (in-process JVM via JNI, REST call to a remote DAG repository, etc.) can override `run_dag_parsing` / `run_task_execution` directly and ignore the `*_cmd` hooks. The two-tier interface is intentional.

A complementary, **out-of-scope** future direction is parsing static (non-programming-language) DAG sources such as YAML (e.g., `dag-factory`). Those do not need a child process at all — but the decision to launch a child is currently made one layer above the coordinator (`DagFileProcessorManager` → `DagFileProcessorProcess`). Hooking in a YAML parser would need a separate extension point at the manager layer; it is not blocked by this design but is also not solved by it. A follow-up AIP is expected to formalize a general "any-source DAG parser" plugin model.

### The Coordinator Layer

We introduce a **Coordinator** layer. When a DAG bundle is loaded, it not only tells Airflow how to find the DAGs (and tasks in them), but also how to *run* each task. Current Python tasks use a Python code path that runs them by forking. A new **Java Coordinator** instructs the task runner how to run tasks in JAR files.

The base interface (`BaseCoordinator`) lives in `airflow.sdk.execution_time`. Concrete coordinators ship as standalone distributions — **not** as Airflow providers — under the shared `airflow.sdk.coordinators` namespace package. The Java coordinator ships as `apache-airflow-coordinators-java` and resolves to `airflow.sdk.coordinators.java.JavaCoordinator`. New language coordinators follow the same pattern: `apache-airflow-coordinators-<lang>` → `airflow.sdk.coordinators.<lang>.<Lang>Coordinator`.

Coordinators are instantiated from the `[sdk] coordinators` Airflow configuration (see [Coordinator Registration](#coordinator-registration) below). Both Airflow Core (DAG processor) and Task SDK (task runner) read that config and use `import_string()` to load the configured `classpath` — no provider plumbing is involved. Decoupling coordinators from the provider system is the direction agreed in [ADR-0005](0005-coordinator-packaging.md) and tracked in [apache/airflow#66451](https://github.com/apache/airflow/issues/66451), which also motivates the per-instance `kwargs` (multiple JDK versions, JVM flags, etc.) that a class-only registration could not express.

### Architecture Overview

```
            Airflow Backend                           Language Runtime Subprocess (Java in this example)
            ───────────────                           ──────────────────────────────────────────────────

    ┌──────────────────────────────┐
    │  DAG File (Python or JAR)    │
    │                              │
    │  @task.stub(queue="java")    │
    │  def my_java_task():         │
    │      ...                     │
    └──────────────┬───────────────┘
                   │
    ┌──────────────▼───────────────┐                    ┌──────────────────────────────┐
    │  DAG File Processor          │                    │  Runtime Subprocess (Java)   │
    │                              │  can_handle_dag    │                              │
    │  For each file in bundle:    │  _file() == True   │  dag_parsing_cmd()           │
    │  ┌ coordinator handles it? ──┼───────────────────►│                              │
    │  │  Yes ──► delegate parse   │                    │  Java SDK parses JAR, builds │
    │  │  No  ──► Python path      │  SDK Serialized    │  SDK-compatible Serialized   │
    │  │                           │◄─── DAG JSON ──────┤  DAG JSON (sdk, tasks, etc.) │
    │  └                           │                    │                              │
    └──────────────┬───────────────┘                    └──────────────────────────────┘
                   │
    ┌──────────────▼───────────────┐
    │  Metadata DB                 │
    │                              │
    │  serialized_dag: {           │  Stored as-is from the language runtime's
    │    "relative_fileloc":       │  SDK Serialized DAG JSON
    │       "path/to/example.jar"  │
    │  }                           │
    │  task_instance.queue         │
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
    │  TI.queue ──► Startup        │                    │  task_execution_cmd()        │
    │                   Details    │                    │  Executes task in JVM        │
    └──────────────┬───────────────┘                    │                              │
                   │                                    └──────────────▲───────────────┘
    ┌──────────────▼───────────────┐                                   │
    │  Task Runner                 │                                   │
    │                              │                                   │
    │  QueueToCoordinatorMapper    │                                   │
    │  resolves queue via `[sdk]   │                                   │
    │  queue_to_coordinator` ──────┼───────────────────────────────────┘
    │  to a coordinator instance   │
    │  from `[sdk] coordinators`   │
    └──────────────────────────────┘
```

### The `BaseCoordinator` Interface

This is the central abstraction that language SDKs implement. It lives in the Task SDK (`task-sdk/src/airflow/sdk/execution_time/coordinator.py`) and handles both DAG parsing and task execution for a specific language runtime.

```python
class BaseCoordinator:
    """
    Base coordinator for runtime-specific DAG file processing and task execution.

    Subclasses represent a specific language runtime (Java, Go, etc.) and are
    instantiated by Airflow Core (DAG processor) and Task SDK (task runner)
    from the ``[sdk] coordinators`` Airflow configuration. Each entry in that
    config carries an instance ``name``, an importable ``classpath``, and
    free-form ``kwargs`` that the subclass accepts in ``__init__`` — this is
    how operators express runtime variants (multiple JDK versions, custom JVM
    flags, etc.) without needing one subclass per variant.

    The base class owns the full bridge lifecycle: TCP servers, subprocess
    management, selector-based I/O loop, and cleanup.
    """

    name: str  # Instance name from [sdk] coordinators (e.g. "jdk-11", "jdk-17")

    def __init__(self, *, name: str, **kwargs) -> None:
        """Accept the per-instance ``kwargs`` declared in ``[sdk] coordinators``."""
        ...

    # Discovery (called by DAG File Processor)

    def can_handle_dag_file(self, bundle_name: str, path: str | os.PathLike) -> bool:
        """Return True if this coordinator should parse the file at *path*."""
        ...

    def get_code_from_file(self, fileloc: str) -> str:
        """Return the actual DAG code (the content of JavaExampleBuilder.java in this case"""
        ...

    # DAG Parsing (called in forked DagFileProcessor child process)

    def dag_parsing_cmd(
        self,
        *,
        dag_file_path: str,  # Absolute path to DAG file
        bundle_name: str,  # Name of the DAG bundle
        bundle_path: str,  # Root path of the bundle
        comm_addr: str,  # host:port for msgpack comm channel
        logs_addr: str,  # host:port for structured JSON log channel
    ) -> list[str]:
        """Return the subprocess command for DAG file parsing."""
        ...

    # Task Execution (called in forked worker child process)

    def task_execution_cmd(
        self,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike,  # Relative path to DAG file within bundle
        bundle_info: BundleInfo,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """Return the subprocess command for task execution."""
        ...

    # Lifecycle (owned by base class, not overridden)

    def run_dag_parsing(self, *, path, bundle_name, bundle_path) -> None: ...

    def run_task_execution(self, *, what, dag_rel_path, bundle_info, startup_details) -> None: ...
```

### Coordinator Registration

Coordinators are registered through Airflow configuration, not through `provider.yaml` or any provider-discovery mechanism. The Java coordinator ships as the standalone distribution `apache-airflow-coordinators-java`, which contributes the `airflow.sdk.coordinators.java` subpackage to the namespace package owned by the Task SDK. As long as the distribution is on `PYTHONPATH`, both Airflow Core and the Task SDK can resolve `airflow.sdk.coordinators.java.JavaCoordinator` via `import_string()`.

Operators wire concrete instances in `airflow.cfg`:

```ini
[sdk]
coordinators = [
    {
        "name": "jdk-11",
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-11-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx512m"],
            "jdk_home": "/usr/lib/jvm/java-11-openjdk-amd64"
        }
    },
    {
        "name": "jdk-17",
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-17-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx1024m", "-Xms256m"],
            "jdk_home": "/usr/lib/jvm/java-17-openjdk-amd64"
        }
    }
]

queue_to_coordinator = {"legacy-java-queue": "jdk-11", "modern-java-queue": "jdk-17"}
```

The same `JavaCoordinator` class can back several instances with different runtime configuration; the routing key is the instance `name`, not the class. This shape is the resolution to the packaging and registration questions originally raised in [ADR-0005](0005-coordinator-packaging.md), motivated by [apache/airflow#66451](https://github.com/apache/airflow/issues/66451) (multi-JDK and JVM-flag support).

### Implementation Language: Kotlin (with a Java-First Public API)

The user-facing API surface (`Task`, `Client`, `Context`, `Dag`, `DagBundle`) is published as Java types and is the contract bundle authors program against. The SDK *implementation* — `CoordinatorComm`, `Serde`, `TaskSdkFrames`, `Server`, `Supervisor`, `TaskRunner`, `DagParser` — is written in Kotlin.

Kotlin compiles to the same JVM bytecode as Java and is fully interoperable, so this choice is invisible to bundle authors at runtime. The practical reasons for using Kotlin internally:

- **Null safety** is part of the type system, removing a large class of latent NPEs in the comm/serde paths.
- **Coroutines and structured I/O** simplify the synchronous-over-async pattern used by `Client.getVariable()` and friends.
- **Less boilerplate** in serialization and frame encoding code, which is the bulk of the SDK.

Because the user-facing API is Java, "Java SDK" remains the accurate name from a DAG-author perspective. A future rename to "JVM SDK" has been floated but is not adopted here; it can be revisited if/when Scala or other JVM-language bindings are proposed.

### Example: `JavaCoordinator`

```python
# Shipped as ``apache-airflow-coordinators-java``;
# resolves to ``airflow.sdk.coordinators.java.JavaCoordinator``.
class JavaCoordinator(BaseCoordinator):
    def __init__(
        self,
        *,
        name: str,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
        jdk_home: str | None = None,
    ) -> None:
        self.name = name
        self.java_executable = java_executable
        self.jvm_args = list(jvm_args or [])
        self.jdk_home = jdk_home

    def can_handle_dag_file(self, bundle_name, path):
        """True when path is a JAR with a Main-Class manifest entry."""
        ...

    def dag_parsing_cmd(self, *, dag_file_path, bundle_name, bundle_path, comm_addr, logs_addr):
        main_class = find_main_class(Path(dag_file_path))
        return [
            self.java_executable,
            *self.jvm_args,
            "-classpath",
            f"{bundle_path}/*",
            main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]

    def task_execution_cmd(self, *, what, dag_rel_path, bundle_info, comm_addr, logs_addr):
        jar_path = Path(dag_rel_path)
        main_class = find_main_class(jar_path)
        return [
            self.java_executable,
            *self.jvm_args,
            "-classpath",
            f"{jar_path.parent}/*",
            main_class,
            f"--comm={comm_addr}",
            f"--logs={logs_addr}",
        ]
```

### Integration Points — Required Changes

**1. Decorator — DAG Author Interface**

DAG authors declare a non-Python task using `@task.stub` and specify a queue:

```python
@task.stub(queue="java")
def my_java_task(): ...
```

**2. Serialization — Each Language SDK Produces SDK-Compatible Serialized DAG JSON**

Serialization is the language runtime's responsibility, not Airflow Core's. Each language SDK implements its own serializer that understands the language-specific DAG and task structure and produces a Task SDK-compatible Serialized DAG JSON — the same schema that the Python SDK's `SerializedDAG` produces.

The language runtime subprocess returns this JSON to the DAG File Processor through the msgpack comm channel. The DAG File Processor and Airflow Core treat it identically to Python-serialized DAGs — it is stored as-is in the metadata DB.

We have already added compatibility validation between the Python SDK and Java SDK serialized DAG JSON formats to ensure both produce structurally equivalent output.

**3. Execution API — Task Queues Routed to the Worker**

A new pair of configurations registers coordinator instances and maps each task's `queue` to one of them:

```ini
[sdk]
coordinators = [
    {"name": "jdk-17", "classpath": "airflow.sdk.coordinators.java.JavaCoordinator", "kwargs": {"java_executable": "java"}}
]
queue_to_coordinator = {"java": "jdk-17"}
```

Tasks scheduled to the `java` queue are routed to the coordinator instance named `jdk-17`. Multiple instances of the same class may coexist (e.g., `jdk-11` and `jdk-17`) and bind to different queues — see [Coordinator Registration](#coordinator-registration).

## Consequences

### New Interfaces

| Component | New Interface | Change Type |
|-----------|--------------|-------------|
| `BaseCoordinator` | Abstract base defined in Task SDK | New class |
| `airflow.sdk.coordinators` | Namespace package contributed to by `apache-airflow-coordinators-<lang>` distributions | New namespace |
| `@task.stub` decorator | `queue: str \| None` parameter | Additive |
| `[sdk] coordinators` | Airflow configuration listing instances (`name`, `classpath`, `kwargs`) | New option |
| `[sdk] queue_to_coordinator` | Airflow configuration mapping queue → instance name | New option |
| `_resolve_runtime_entrypoint` | Route by `queue` → coordinator instance from `[sdk] coordinators` | Behavioral |

### What Becomes Easier

- Adding a new language runtime requires only a `BaseCoordinator` subclass shipped as `apache-airflow-coordinators-<lang>` and a corresponding entry in `[sdk] coordinators` — no changes to Airflow Core and no provider plumbing.
- DAG authors can mix Python and non-Python tasks in the same pipeline.
- The existing task-runner two-layer design is preserved, keeping all Airflow extensions in Python.

### What Becomes Harder

- Each language SDK must independently produce compatible serialized DAG JSON, which requires cross-language validation infrastructure.
- The coordinator subprocess bridge adds a TCP hop and process management overhead per non-Python task.
- Debugging non-Python tasks requires understanding the bridge layer between the task runner and the language runtime.
