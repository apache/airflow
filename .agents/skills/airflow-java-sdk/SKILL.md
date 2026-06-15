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

`coordinator.py` extends `SubprocessCoordinator`. The only method subclasses must implement is
`_build_execute_task_command`, which returns `(argv, schema_version)`. Look at the existing
implementation for how `jars_root`, `java_executable`, `jvm_args`, and `main_class` are
assembled into the command. Do not reach into the JVM process from Python beyond what this
method provides.

---

## Upgrading Supervisor Schema client

When upgrading to a newer Supervisor Schema version:

- Regenerate models with `./gradlew generateJsonSchema2Pojo`
- Modify `execution/Client.kt` to handle changes

The `java-sdk/README.md#contributing` section walks through the full "adding a new Client
method" sequence step by step.
