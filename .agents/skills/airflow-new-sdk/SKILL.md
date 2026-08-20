---
name: airflow-new-sdk
description: >
  Guide for implementing a brand-new language SDK for Airflow (AIP-108). Use
  this skill when a contributor wants to add support for a new programming
  language — designing the Python coordinator, implementing the wire protocol in
  the target language, writing the bundle format, and structuring the PR. Trigger
  on phrases like "new language SDK", "new SDK", "add support for [language]",
  "implement coordinator for", "SubprocessCoordinator", "BaseCoordinator",
  "new runtime", "AFBNDL01", "supervisor schema", or anything about bringing a
  new language into the Airflow executor ecosystem.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Implementing a new language SDK for Airflow

## Start here

**Read `contributing-docs/30_new_language_sdk.rst` first.** It is the
authoritative contributor guide for this topic — coordinator base class choices,
wire protocol spec, bundle footer format, and testing requirements. Everything
in this skill builds on top of it, not alongside it.

---

## Repository layout

Every new SDK needs two things. The coordinator (Python) goes here:

```
task-sdk/src/airflow/sdk/coordinators/<language>/
    __init__.py        # re-export + module docstring
    coordinator.py     # subclass of SubprocessCoordinator or BaseCoordinator
task-sdk/tests/coordinators/<language>/
    test_coordinator.py
task-sdk/tests/integration/coordinators/<language>/
    test_integration.py   # requires Breeze
```

The language SDK itself lives in a top-level `<language>-sdk/` directory (like
`java-sdk/` and `go-sdk/`). For native-executable languages using
`ExecutableCoordinator`, no coordinator code is needed at all.

---

## Choosing the right base class — quick guide

```
Does the runtime compile to a self-contained native executable?
  YES → Use ExecutableCoordinator (zero Python to write).
        Append an AFBNDL01 footer with a packer tool (see go-sdk reference).
  NO  →
    Does it start via a single shell command (node, ruby, dotnet, …)?
      YES → Subclass SubprocessCoordinator.
            Implement _build_execute_task_command only (see 30_new_language_sdk.rst).
      NO  →
        Subclass BaseCoordinator and implement execute_task from scratch.
        (Rare: gRPC daemons, shared memory, persistent processes.)
```

The full rationale, method signature, and socket lifecycle for each path are in
`30_new_language_sdk.rst`. Read that section before writing any code.

---

## Reference implementations to study

| What to study | Where |
|---|---|
| SubprocessCoordinator base class | `task-sdk/src/airflow/sdk/coordinators/_subprocess.py` |
| Java coordinator (SubprocessCoordinator subclass) | `task-sdk/src/airflow/sdk/coordinators/java/coordinator.py` |
| ExecutableCoordinator (native bundles) | `task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py` |
| Wire protocol in Kotlin | `java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/execution/` |
| Wire protocol in Go | `go-sdk/pkg/execution/` |
| AFBNDL01 footer (Go reference) | `go-sdk/internal/bundlefooter/`, `task-sdk/docs/executable-bundle-spec.rst` |
| All message types and field specs | `task-sdk/src/airflow/sdk/execution_time/schema/schema.json` |

The Java and Go implementations are the two production reference points. When
implementing a new SDK, read whichever matches the target language's runtime
model (JVM/interpreted → Java; native/compiled → Go).

---

## Logging

The **Logging** section of `30_new_language_sdk.rst` is the spec: the `--logs`
JSON record format, the level names, and the `AIRFLOW__LOGGING__*` environment
variables. Read it first. A few language-neutral details it leaves out:

- **Level values.** Levels follow Python's `logging` scale, so thresholds line
  up with the rest of Airflow: `CRITICAL=50`, `ERROR=40`, `WARNING=30`,
  `INFO=20`, `DEBUG=10`, `NOTSET=0`.
- **Parsing `NAMESPACE_LEVELS`.** Split the value on `[\s,]+`, then split each
  item on `=` into `(logger_name, level_name)`. Emit a record only when its
  level is `>=` the matching `logger_name` threshold, or the global threshold
  when no per-logger entry matches.
- **Don't drop late logs.** Connect the `--logs` socket early and keep it open
  until the `--comm` channel has finished; otherwise records emitted during
  teardown can be lost.
- **Extra config keys.** The runtime can't read Airflow's config, so if your SDK
  needs `[logging]` settings beyond the two above, propagate them as environment
  variables from your coordinator's `start`, the same way.

For how a given language wires its native logging frameworks into this channel,
read that SDK's source (e.g. `java-sdk/`) rather than reproducing it here.

### E2E test suite

Create two files mirroring `java_sdk_tests/` or `go_sdk_tests/`:

```
airflow-e2e-tests/tests/airflow_e2e_tests/<language>_sdk_tests/
    __init__.py
    test_<language>_sdk_dag.py
```

The test file should:

- Trigger the SDK's example Dag (the one added to `<language>-sdk/dags/` or
  equivalent) via `AirflowClient.trigger_dag`.
- Wait for the run to finish with `AirflowClient.wait_for_dag_run`.
- Assert that each SDK task instance reached `"success"`.
- Assert at least one XCom value — confirms the full round-trip from task
  return value through the supervisor to the XCom store.
- Assert structured logs where the SDK emits them (see the Go suite for an
  example of log-content assertions).

Run locally with:

```bash
E2E_TEST_MODE=<language>_sdk uv run --project airflow-e2e-tests pytest \
    tests/airflow_e2e_tests/<language>_sdk_tests/ -xvs
```

The Java (`java_sdk_tests/test_java_sdk_dag.py`) and Go
(`go_sdk_tests/test_go_sdk_dag.py`) suites are the reference implementations.


---

## PR checklist (items not covered by 30_new_language_sdk.rst)

The `30_new_language_sdk.rst` guide covers coordinator placement, wire protocol
implementation, and testing. These additional items belong in the same PR:

1. `task-sdk/src/airflow/sdk/coordinators/<language>/__init__.py` — short
   module docstring and `__all__` re-export of the coordinator class.
2. `airflow-core/docs/authoring-and-scheduling/language-sdks/<language>.rst` —
   user-facing doc following the structure of `java.rst` or `go.rst`.
3. `airflow-core/docs/authoring-and-scheduling/language-sdks/index.rst` — add
   the new doc to the toctree.
4. `airflow-core/newsfragments/<PR>.feature.rst` — a new language is always
   user-visible; add a newsfragment.
5. CI wiring — check `dev/breeze/src/airflow_breeze/utils/selective_checks.py`
   to confirm `<language>-sdk/` changes trigger the right test group. Add if
   missing.
6. E2E tests — add a `<language>_sdk_tests/` suite under
   `airflow-e2e-tests/tests/airflow_e2e_tests/`. See below.
