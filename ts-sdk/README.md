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

# Airflow TypeScript SDK

Public TypeScript interfaces for writing Apache Airflow task handlers.

**Status:** 0.1.0-beta1 · API may change · Node 22+ · ESM-only

This package defines the user-facing task handler contract and the coordinator
runtime used to execute registered TypeScript handlers from Airflow.

## Installation

```bash
npm install apache-airflow-ts-sdk@0.1.0-beta1
```

## Task Handlers

```ts
import { Bundle, getClient, getContext, TaskHandler } from "apache-airflow-ts-sdk";

export async function sayHello() {
  const greeting = await getClient().getVariable("greeting");
  return { message: `Hello from ${getContext().taskId}: ${greeting}` };
}

const bundle = new Bundle();
bundle.register(new TaskHandler("example_dag", "say_hello", sayHello));
await bundle.serve();
```

A handler is a plain function. `getContext()` and `getClient()` reach the runtime from inside the call,
so a handler takes no SDK-supplied parameter.

`new TaskHandler(dagId, taskId, handler)` binds the function to the Python-owned task it implements.
The Dag is declared in Python with `@task.stub`, so the TypeScript side only supplies the task bodies.

Non-`undefined` return values are pushed to XCom under the `"return_value"`
key by the active runtime, matching Python `@task` behavior.

## Coordinator Usage

Airflow runs TypeScript task bundles through the Python-side
`airflow.sdk.coordinators.node.NodeCoordinator`. Declaring Airflow Dags in
TypeScript is not supported yet; the Dag is still declared in Python. The
intended authoring shape matches the other non-Python SDKs: a Python Dag
declares the scheduling shape with stub tasks, and the TypeScript module
registers handlers with matching task IDs.

Python Dag:

```python
from airflow.sdk import dag, task


@dag
def sales_pipeline():
    @task.stub(queue="typescript")
    def extract(): ...

    @task.stub(queue="typescript")
    def transform(extracted): ...

    transform(extract())


sales_pipeline()
```

Airflow coordinator config:

```ini
[sdk]
coordinators = {
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]}
  }
}
queue_to_coordinator = {"typescript": "ts"}
```

Each configured bundle directory is searched recursively for `*.min.mjs` bundles built with `airflow-ts-pack`
(see [Packing bundles](#packing-bundles)), which embeds the Airflow metadata in the bundle itself.

TypeScript entrypoint:

```ts
import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

export async function extract() {
  const client = getClient();
  const connection = await client.getConnection("sales_db");
  const rowCount = Number((await client.getVariable("daily_row_count")) ?? "0");

  return {
    connectionId: connection?.id ?? null,
    rowCount,
  };
}

export async function transform() {
  const extracted = await getClient().getXCom<{ rowCount: number }>({
    key: "return_value",
    taskId: "extract",
  });

  return {
    transformedRows: extracted?.rowCount ?? 0,
  };
}

const bundle = new Bundle();
bundle.register(
  new TaskHandler("sales_pipeline", "extract", extract),
  new TaskHandler("sales_pipeline", "transform", transform),
);
await bundle.serve();
```

The Python stub defines the Dag dependency graph. The TypeScript handler does
the work and uses `TaskClient` for task-time Airflow data access. The handler
function is the reusable task implementation; a `TaskHandler` binds it to a
Python stub task identity, a `Bundle` holds what this bundle process provides,
and `bundle.serve()` serves it to Airflow.

`bundle.serve()` is the entrypoint: a task left unregistered is not part of the bundle,
and one bundle can provide for several `TaskHandler`s.
Registering holds no sockets and starts nothing, so a unit test can build a bundle
and dispatch through `bundle.getTaskHandler(dagId, taskId)` without any runtime involved.

Dispatch keys on the `(dagId, taskId)` pair, so the same `taskId` under two Dags is two different handlers:

```ts
import { Bundle, TaskHandler } from "apache-airflow-ts-sdk";
import { chargeCustomer } from "./billing/tasks";
import { extract } from "./sales/tasks";

await new Bundle(
  new TaskHandler("sales_pipeline", "extract", extract),
  new TaskHandler("billing_pipeline", "extract", chargeCustomer),
).serve();
```

Register `TaskHandler` and `Dag` values with the `register` method, or pass them to the `Bundle` constructor.

## TaskFlow arguments

A Python Dag that calls a stub task TaskFlow-style passes those arguments straight to the handler,
which destructures them by name:

```python
# the Python Dag
@task.stub(queue="typescript")
def transform(region_code: str, threshold: float, dry_run: bool = False): ...


transform("uk", 0.75)
```

```ts
interface TransformArgs {
  regionCode: string;
  threshold: number;
  dryRun: boolean;
}

export async function transform({ regionCode, threshold, dryRun }: TransformArgs) {
  // ...
}
```

Names bind by **folding on both sides**, lowercased with underscores removed, so `region_code` reaches
`regionCode` and `s3_uri` reaches `s3Uri` with nothing declared on either side.
An argument the call leaves at its default arrives with the default's value.

A name nothing folds to is **logged, not thrown**, naming what the handler asked for and what the call
delivered. Two Python names that fold to the same token fail the task.

`Object.keys` and rest destructuring (`{ ...rest }`) yield Python's names, and `in` folds like a read.

### Upstream outputs

An argument the Python call fills from another task arrives as that task's value, not as a reference to it.
Feeding the `transform` above from an upstream task adds one argument on each side:

```python
# the Python Dag
@task
def extract() -> int: ...


@task.stub(queue="typescript")
def transform(rows: int, region_code: str, threshold: float, dry_run: bool = False): ...


transform(extract(), "uk", 0.75)
```

```ts
interface TransformArgs {
  rows: number;
  regionCode: string;
  threshold: number;
  dryRun: boolean;
}

export async function transform({ rows, regionCode, threshold, dryRun }: TransformArgs) {
  // `rows` is the number extract() returned.
}
```

An upstream that pushed no output fails the task, naming both the argument and the task it came from.
An upstream that pushed `null` binds `null`.

Being upstream is not the same as being passed. An XCom dependency declared with `>>` defines task order only,
so a value the call did not pass is read explicitly:

```ts
const rows = await getClient().getXCom<number>({ key: "return_value", taskId: "extract" });
```

A Python `int` beyond the ±9007199254740991 a JavaScript number holds exactly is refused rather than bound,
so carry such a value across the boundary as a string.

### Explicit renames

`withArgNames` states a binding folding cannot reach, for a name the Python side never used:
a clearer word than the Dag chose, or a TypeScript reserved word like `enum`. Mapping first, handler second:

```ts
interface ReportArgs {
  summary: Summary;
  label: string; // Python calls this `run_label`
}

const report = withArgNames({ label: "run_label" }, async ({ summary, label }: ReportArgs) => {
  // `label` is the call's `run_label`; `summary` folded as usual.
});

bundle.register(new TaskHandler("etl", "report", report));
```

An entry beats folding, and everything the map does not mention still folds,
so `withArgNames` should be rare in a real Dag.
The map's keys are checked against the handler's own parameter type,
so `{ labl: "run_label" }` is a compile error naming the right key.
Its values are Python names, which `tsc` cannot see and does not check.

`Dag` is another interface, for a Dag declared natively in TypeScript, and is still a work in progress.

Airflow launches the bundled entrypoint with `--comm=host:port` and
`--logs=host:port`. `bundle.serve()` connects to those sockets, receives the
task startup message, finds the registered handler for the Dag/task pair, and
reports the terminal task state back to Airflow.

See [`example/`](https://github.com/apache/airflow/tree/main/ts-sdk/example) for
a coordinator-runtime example that packs a bundle with `airflow-ts-pack` and
uses a Python stub Dag.

## Packing bundles

`airflow-ts-pack` produces a single self-contained bundle in one command.
Packing is build-time only, so `esbuild` is an optional peer dependency the
runtime install skips:

```bash
npm install --save-dev esbuild
airflow-ts-pack src/main.ts --outdir dist
```

It bundles the entrypoint into a minified `dist/bundle.min.mjs` with esbuild, then runs that bundle with
`--airflow-metadata` so it reports its own registered Dag/task pairs and supervisor schema version. The manifest is
embedded as a compact JSON `//# airflowMetadata=...` comment after a leading compact JSON `//# airflowBundle=...`
layout descriptor, and the entry module is embedded verbatim in a `/*# airflowSource ... #*/` block comment so
Airflow can show the source a bundle was authored from, which its minified code no longer is. The CLI records the
integrity metadata for all three regions in that descriptor, so a coordinator that is handed a bundle whose content
was replaced fails loudly instead of running it. The result is one deployable file with no hand-written metadata
sidecar.

Pass `--outfile <path>` instead of `--outdir` to name the artifact yourself, so one bundle directory can hold several
bundles. The name must still end in `.min.mjs`, which is how `NodeCoordinator` finds bundles.

Options:

- `--outdir <dir>`: output directory (default `dist`)
- `--outfile <path>`: exact output path, whose name must end in `.min.mjs`
- `--source <name>`: display name of the primary source file shown in the Airflow UI (default: entry basename)

## TaskClient

`getClient()` returns a `TaskClient` for task-time Airflow data access, for as long as a handler is running:

| Method                                           | Description         |
| ------------------------------------------------ | ------------------- |
| `getVariable(key)` / `getVariableOrThrow`        | Airflow Variables   |
| `getXCom(opts)` / `setXCom(opts)`                | XCom read/write     |
| `getConnection(connId)` / `getConnectionOrThrow` | Airflow Connections |

Locator fields such as `dagId`, `runId`, and `taskId` default to the
current task context when omitted.

## Cancellation

`getContext().signal` is an `AbortSignal` controlled by the active runtime.
Pass it to `fetch()`, timers, database clients, child processes, or any other API that accepts an abort signal,
so tasks can clean up cooperatively when Airflow terminates the task subprocess.

## Compatibility matrix

Which Airflow TaskInstance states and capabilities this SDK supports. This table is generated from
[`capabilities.yaml`](https://github.com/apache/airflow/blob/main/ts-sdk/capabilities.yaml);
the conformance dimensions are defined in the
[Language SDK conformance spec](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).
Do not edit the table by hand. Update the manifest and run the `update-ts-sdk-readme-matrix` prek hook.

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.4 · supervisor schema: 2026-10-30*

| Dimension | Tier | Supported | Since | Notes |
|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |
| state: `success` | MUST | ✓ | 3.4 |  |
| state: `failed` | MUST | ✓ | 3.4 |  |
| state: `up_for_retry` | MUST | ✓ | 3.4 | RetryTask |
| state: `skipped` | SHOULD | ✗ | – | runtime does not emit TaskState skipped yet |
| state: `deferred` | MAY | ✗ | – | runtime does not emit DeferTask yet |
| state: `up_for_reschedule` | MAY | ✗ | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✗ | – | runtime does not emit AwaitInputTask yet |
| state: `removed` | MAY | ✓ | 3.4 |  |
| **Runtime capabilities** |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.4 | @task.stub |
| capability: `taskflow-binding` | MUST | ✗ | – | bind @task.stub literal/XCom args to the native handler |
| capability: `task-logging` | MUST | ✓ | 3.4 | structured records over the log socket |
| capability: `xcom-read-write` | MUST | ✓ | 3.4 | getXCom / setXCom |
| capability: `connection-read` | MUST | ✓ | 3.4 | getConnection |
| capability: `variable-read-write` | MUST | ✗ | – | getVariable only; no write over the comm socket yet |
| capability: `self-contained-bundle` | MUST | ✓ | 3.4 | Airflow metadata embedded in the bundle |
| capability: `retry-policy` | MAY | ✗ | – | no task-facing retry-policy API yet |
| capability: `task-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | no task-facing asset-event API yet |
| **Native-Dag authoring** |  |  |  |  |
| capability: `native-dag-authoring` | SHOULD | ✗ | – | native Dag authoring not implemented yet |
| capability: `task-args` | MUST † | n/a | – |  |
| capability: `dag-params` | MUST † | n/a | – |  |
| capability: `taskflow-dependencies` | MUST † | n/a | – |  |
| capability: `branching` | SHOULD † | n/a | – |  |
| capability: `dag-test` | SHOULD † | n/a | – |  |
| capability: `task-group` | MAY † | n/a | – |  |
| capability: `dynamic-task-mapping` | MAY † | n/a | – |  |
| capability: `asset-inlets-outlets` | MAY † | n/a | – |  |
| capability: `asset-scheduling` | MAY † | n/a | – |  |
| capability: `object-store` | MAY † | n/a | – | no object-storage API yet |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `native-dag-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

## Links

- [TypeScript SDK guide (staged docs)](https://airflow.staged.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html)
  (how Airflow runs TypeScript task handlers)
- [API reference (staged)](https://airflow.staged.apache.org/docs/ts-sdk/stable/)
  (generated from the TypeScript sources)
- [Source](https://github.com/apache/airflow/tree/main/ts-sdk): the `ts-sdk/` directory of the Apache Airflow monorepo
- [Issues](https://github.com/apache/airflow/issues): bug reports and feature requests
- [Website](https://airflow.apache.org) · [Slack](https://s.apache.org/airflow-slack)
- [Developing this package](https://github.com/apache/airflow/blob/main/ts-sdk/DEVELOPMENT.md)
  (local build, docs, and the release workflow)
