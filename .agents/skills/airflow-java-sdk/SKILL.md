---
name: airflow-java-sdk
description: >
  Guide for contributing to the Airflow Java SDK (AIP-108). Use this skill
  whenever a contributor is working in the `java-sdk/` directory or on the Java
  coordinator in `task-sdk/src/airflow/sdk/coordinators/java/` — whether they
  want to add a feature, write tests, fix a bug, understand the architecture, or
  prepare a PR. Trigger on phrases like "Java SDK", "JavaCoordinator",
  "java-sdk", "annotation processor", "Builder.Task", "BundleBuilder", or
  anything about running JVM tasks in Airflow.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Java SDK contributor guide

The Java SDK lets Airflow tasks execute JVM code (Java, Kotlin, or any JVM language). You are helping
a contributor work in one or both of these locations:

- **`java-sdk/`** — the JVM-side library (Kotlin source, published to Maven)
- **`task-sdk/src/airflow/sdk/coordinators/java/`** — the Python coordinator that launches the JVM subprocess

Read these two documents early in every session — they contain the authoritative reference material:

- `airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst` — user-facing guide:
  annotation vs. interface API, XCom type mapping, Gradle/Maven steps, coordinator config.
- `java-sdk/README.md` — contributor guide: repository layout, detailed execution walkthrough,
  Gradle + Breeze test commands, coding conventions, common tasks, and PR checklist.

---

## SDK package architecture

The JVM-side library is split into two packages with distinct visibility rules:

- **`org.apache.airflow.sdk`** — public, user-facing API. Classes here (e.g. `Client`, `Bundle`,
  `BundleBuilder`, `Server`) are stable contracts that DAG authors and task implementers import
  directly. Changes to this package are breaking changes.
- **`org.apache.airflow.sdk.execution`** — internal implementation detail. Everything in this
  package (`CoordinatorComm`, `LogSender`, `Log`, `Client` in `execution/`, generated schema
  models, etc.) is not intended to be imported by users. It may change between releases without
  notice.

When reviewing or writing code, enforce this boundary: user task code and `BundleBuilder`
subclasses must only import from `org.apache.airflow.sdk`; any import of
`org.apache.airflow.sdk.execution.*` in user-facing API surface is a red flag.

---

## Bundle composition and coordinator discovery

A **bundle** is a directory of JAR files (typically `build/bundle/`) placed in the Dag bundle named
by the coordinator's `task_handler_bundle_name` (the task's own Dag bundle when unset). The Dag
processor lists the handler JARs in that Dag bundle and binds each stub task to the JAR that registers
its handler. At task-dispatch time the coordinator runs only the JAR the task was bound to (or, for a
Dag defined in Java, its own Dag file), and reads from it:

1. **`Main-Class`** (standard JAR manifest attribute) — the fully-qualified class name of the
   entry point that the coordinator invokes with `java -classpath … <Main-Class> --comm … --logs …`.
   This must be a class with a `public static void main(String[] args)` method; the Gradle plugin
   `org.apache.airflow.sdk` writes it automatically from `airflowBundle { mainClass = "…" }` and
   validates that the class exists and has the right signature at build time.

2. **`Airflow-Supervisor-Schema-Version`** (Airflow-specific manifest attribute) — the wire
   protocol version the JVM side expects when talking to the Python supervisor. In fat-JAR mode
   (the default), the Gradle plugin reads this value from the `airflow-sdk` JAR in
   `runtimeClasspath` and copies it into the shadow JAR manifest. In thin-JAR mode (`fatJar =
   false`), the value stays in the `airflow-sdk` JAR deployed alongside the bundle JAR.

The Python coordinator (`JavaCoordinator`) reads `META-INF/MANIFEST.MF` out of that JAR in
`_build_task_handler_command`, and takes `Main-Class` and `Airflow-Supervisor-Schema-Version` from
it. A thin JAR that carries no schema version takes the first one found in the other JARs of the
Dag bundle, in sorted path order. The resolved schema version is returned from
`_build_task_handler_command`, and the base `SubprocessCoordinator` uses it to negotiate the
supervisor wire protocol. The Dag processor probes a JAR with the same command.

If `main_class` is set explicitly on the `JavaCoordinator` instance (via `[sdk] coordinators`
kwargs), it runs instead of the manifest's `Main-Class`, for tasks and for the probe, so keep one
handler JAR per Dag bundle then. The Java candidate's cache digest then covers `main_class`, so
changing it makes the Dag processor probe again. Only a JAR whose manifest has `Airflow-Cache-Digest`
is a handler JAR: the Gradle plugin writes it, and a Maven build must set a value that changes on
every build (see java.rst). Either way, `Airflow-Supervisor-Schema-Version` must be present in
at least one JAR in the Dag bundle. Without it the Dag processor cannot probe the JAR, so the stub
Dag fails to import with the reason in its import error. A task fails before the JVM starts, with
the reason in its task log, only when its worker's JARs differ from the Dag processor's. Every JAR
in the Dag bundle goes on one classpath.

---

## Key files to know

| File | Purpose |
|---|---|
| `java-sdk/sdk/.../Client.kt` | Public API (Variables, Connections, XCom) |
| `java-sdk/sdk/.../execution/Client.kt` | Supervisor wire calls |
| `java-sdk/sdk/.../execution/Comm.kt` | 4-byte-prefix MessagePack framing |
| `java-sdk/sdk/.../Server.kt` | Entry-point; drives the execution loop |
| `java-sdk/processor/.../BuilderProcessor.kt` | Kapt annotation processor |
| `java-sdk/plugin/.../AirflowSdkPlugin.kt` | Gradle bundle plugin |
| `task-sdk/.../coordinators/java/coordinator.py` | Python side — spawns the JVM |
| `task-sdk/.../schema/schema.json` | Wire protocol definition (both sides) |

---

## Running tests

Always use `./gradlew` from inside `java-sdk/`; never run Gradle via apt's `gradle`.
See `java-sdk/README.md#testing` for the full list of Gradle commands.

For the Python coordinator, use Breeze (never `pytest` directly on the host):

```bash
breeze testing task-sdk-tests -- task_sdk/coordinators/java
```

End-to-end test suite:

```bash
E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \
    tests/airflow_e2e_tests/java_sdk_tests/ -xvs
```

---

## Updating the Python coordinator

`coordinator.py` extends `SubprocessCoordinator`. The methods subclasses implement are
`_read_task_handler_candidate`, which tells the coordinator's artifacts apart from the other files
of a Dag bundle, and `_build_task_handler_command`, which returns `(argv, schema_version)` for the
artifact at a path, for the probe and for a task alike. Look at the existing implementation for how
the Dag bundle's JARs, `java_executable`, `jvm_args`, and `main_class` are assembled into the
command. Do not reach into the JVM process from Python beyond what this method provides.

---

## Upgrading Supervisor Schema client

When upgrading to a newer Supervisor Schema version:

- Regenerate models with `./gradlew generateJsonSchema2Pojo`
- Modify `execution/Client.kt` to handle changes

The `java-sdk/README.md#contributing` section walks through the full "adding a new Client
method" sequence step by step.
