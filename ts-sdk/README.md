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

Write Apache Airflow Dags and tasks in TypeScript.

**Status:** 0.1.0-beta1 · API may change · Node 22+ · ESM-only

There are two ways to use it:

- **Declare a whole Dag in TypeScript**: its schedule, tasks, options and dependencies, with no Python file.
- **Implement mixed-language tasks**: TypeScript bodies for the stub tasks of a Python Dag.

Both use the same task API, and one bundle can serve both.

## Installation

```bash
npm install apache-airflow-ts-sdk@0.1.0-beta1
npm install --save-dev esbuild
```

## Declaring a Dag

```ts
import { Bundle, Dag, getClient } from "apache-airflow-ts-sdk";

const dag = new Dag("sales_pipeline", { schedule: "@daily", queue: "typescript" });

const extract = dag.task("extract", async (): Promise<number> => {
  return Number((await getClient().getVariable("daily_row_count")) ?? "0");
});
const transform = dag.task(
  "transform",
  async ({ rows, region }: { rows: number; region: string }) => ({ rows, region }),
);

transform({ rows: extract(), region: "us" });

await new Bundle(dag).serve();
```

A handler takes one object of named arguments, and calling a task names its inputs. An input is either the
reference another task's call returned, which makes this task wait for it and receive its value, or a literal.

A Dag also has order-only dependencies (`before` and `after`), task groups, branching with `dag.if` and
`dag.switch`, and `dag.triggerDagRun`. See the
[guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html)
for all of them.

## Implementing mixed-language tasks

The Python Dag declares the tasks and their dependencies:

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

TypeScript implements them, with a `TaskHandler` per stub task:

```ts
import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

export async function extract() {
  const rowCount = Number((await getClient().getVariable("daily_row_count")) ?? "0");
  return { rowCount };
}

export async function transform({ extracted }: { extracted: { rowCount: number } }) {
  return { transformedRows: extracted.rowCount };
}

await new Bundle(
  new TaskHandler("sales_pipeline", "extract", extract),
  new TaskHandler("sales_pipeline", "transform", transform),
).serve();
```

A `TaskHandler` names the `dag_id` and `task_id` it implements, so the same `task_id` under two Dags is two
different handlers. Nothing connects to Airflow until `bundle.serve()`, so a unit test can build a bundle and
call a handler through `bundle.getTaskHandler(dagId, taskId)`.

### TaskFlow arguments

A Python Dag that calls a stub task TaskFlow-style passes those arguments to the handler, which destructures
them by name:

```python
@task.stub(queue="typescript")
def transform(region_code: str, threshold: float, dry_run: bool = False): ...


transform("uk", 0.75)
```

```ts
export async function transform({ regionCode, threshold, dryRun }: TransformArgs) {
  // ...
}
```

Names match ignoring case and underscores, so `region_code` reaches `regionCode` with nothing declared on
either side. An argument filled from another task, as in `transform(extract(), "uk")`, arrives as that task's
value. A dependency drawn with `>>` orders the tasks and passes nothing; read such a value with
`getClient().getXCom`.

`withArgNames` maps a handler's name to a different Python name, when the two cannot match on their own:

```ts
const report = withArgNames({ label: "run_label" }, async ({ summary, label }: ReportArgs) => {
  // `label` is the call's `run_label`.
});
```

## Writing tasks

A handler is a plain, usually `async`, function. `getContext()` and `getClient()` give it the task's context and
Airflow access while it runs, so it takes no SDK-supplied argument. A value it returns becomes the task's
`return_value` XCom, and an uncaught error fails the task.

## Building and deploying

`airflow-ts-pack` builds the entry module and everything it imports into one file, `dist/bundle.min.mjs`:

```bash
npx airflow-ts-pack src/main.ts --outdir dist
```

- `--outdir <dir>`: output directory (default `dist`)
- `--outfile <path>`: exact output path, whose name must end in `.min.mjs`

A bundle that declares Dags goes into a Dag bundle, such as the default `dags-folder` bundle. Route its queue to
the Node.js coordinator:

```ini
[sdk]
coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
queue_to_coordinator = {"typescript": "ts"}
```

A bundle that only implements stub tasks goes next to the Python Dag that declares them, or into the Dag
bundle the coordinator's `task_handler_bundle_name` names. See the guide for all coordinator options.

See [`example/`](https://github.com/apache/airflow/tree/main/ts-sdk/example) for a working project that serves
both a Dag declared in TypeScript and the tasks of two Python Dags from one bundle.

## TaskClient

`getClient()` returns a `TaskClient` for task-time Airflow data access, for as long as a handler is running:

| Method                                                          | Description             |
| --------------------------------------------------------------- | ----------------------- |
| `getVariable(key)` / `getVariableOrThrow`                       | Airflow Variables       |
| `setVariable(key, value, description?)` / `deleteVariable(key)` | Variable write / delete |
| `getXCom(opts)` / `setXCom(opts)`                               | XCom read/write         |
| `getConnection(connId)` / `getConnectionOrThrow`                | Airflow Connections     |

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
| capability: `variable-read-write` | MUST | ✓ | 3.4 | getVariable / setVariable / deleteVariable |
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
