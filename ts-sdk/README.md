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

Write Apache Airflow Dags in TypeScript, and run them on Node.js.

**Status:** 1.0.0-beta1 · API may change · Node 22+ · ESM-only

## Installation

```bash
npm install apache-airflow-ts-sdk@1.0.0-beta1
npm install --save-dev esbuild
```

## Quick start

Declare a Dag in `src/main.ts`:

```ts
import { Bundle, Dag } from "apache-airflow-ts-sdk";

const dag = new Dag("ts_hello", { schedule: "@daily", queue: "typescript" });

const extract = dag.task("extract", async (): Promise<number> => 42);
const report = dag.task("report", async ({ rows }: { rows: number }) => {
  console.log(`extracted ${rows} rows`);
});

report({ rows: extract() });

await new Bundle(dag).serve();
```

Build it into a single file:

```bash
npx airflow-ts-pack src/main.ts --outdir dist
```

Put `dist/bundle.min.mjs` in a Dag bundle, such as the default `dags-folder` bundle, and send the `typescript`
queue to the Node.js coordinator in `airflow.cfg`:

```ini
[sdk]
coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
queue_to_coordinator = {"typescript": "ts"}
```

Airflow reads the bundle like any other Dag file: `ts_hello` shows up in the UI, runs on its schedule, and
`report` receives the value `extract` returned.

## Learn more

The [TypeScript SDK guide][guide] covers:

| Topic                                                                                         | Guide section                                                  |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Task inputs, options, order-only dependencies, task groups, branching, triggering another Dag | [Declaring a Dag in TypeScript][declaring-a-dag-in-typescript] |
| `approval` and `hitl` tasks that wait for a person                                            | [Human-in-the-loop tasks][human-in-the-loop-tasks]             |
| `getContext()`, `getClient()` and the `TaskClient`                                            | [Writing tasks][writing-tasks]                                 |
| Building a bundle with `airflow-ts-pack`                                                      | [Building a bundle][building-a-bundle]                         |
| Where to deploy a bundle, and the `NodeCoordinator` options                                   | [Deploying][deploying]                                         |
| What a Dag declared in TypeScript cannot do yet                                               | [Limitations][limitations]                                     |

See [`example/`](https://github.com/apache/airflow/tree/main/ts-sdk/example) for a working project.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html
[declaring-a-dag-in-typescript]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#declaring-a-dag-in-typescript
[human-in-the-loop-tasks]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#human-in-the-loop-tasks
[writing-tasks]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#writing-tasks
[building-a-bundle]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#building-a-bundle
[deploying]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#deploying
[limitations]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html#limitations

## Compatibility matrix

Which Airflow TaskInstance states and capabilities this SDK supports. This table is generated from
[`capabilities.yaml`](https://github.com/apache/airflow/blob/main/ts-sdk/capabilities.yaml);
the conformance dimensions are defined in the
[Language SDK conformance spec](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).
Do not edit the table by hand. Update the manifest and run the `update-ts-sdk-readme-matrix` prek hook.

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.4.0 · supervisor schema: 2026-10-30*

| Dimension | Tier | Supported | Since (Airflow) | Since (SDK) | Notes |
|---|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |  |
| state: `success` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| state: `failed` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| state: `up_for_retry` | MUST | ✓ | 3.4.0 | 1.0.0 | RetryTask |
| state: `skipped` | SHOULD | ✓ | 3.4.0 | 1.0.0 | triggerDagRun with skipWhenAlreadyExists only |
| state: `deferred` | MAY | ✓ | 3.4.0 | 1.0.0 | triggerDagRun with deferrable only |
| state: `up_for_reschedule` | MAY | ✗ | – | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✓ | 3.4.0 | 1.0.0 | approval() / hitl() in a Dag definition |
| state: `removed` | MAY | ✓ | 3.4.0 | 1.0.0 |  |
| **Task runtime** |  |  |  |  |  |
| capability: `task-logging` | MUST | ✓ | 3.4.0 | 1.0.0 | structured records over the log socket |
| capability: `xcom-read-write` | MUST | ✓ | 3.4.0 | 1.0.0 | getXCom / setXCom |
| capability: `connection-read` | MUST | ✓ | 3.4.0 | 1.0.0 | getConnection |
| capability: `variable-read-write` | MUST | ✓ | 3.4.0 | 1.0.0 | getVariable / setVariable / deleteVariable |
| capability: `self-contained-bundle` | MUST | ✓ | 3.4.0 | 1.0.0 | Airflow metadata embedded in the bundle |
| capability: `retry-policy` | MAY | ✗ | – | – | no task-facing retry-policy API yet |
| capability: `task-state-store` | MAY | ✓ | 3.4.0 | 1.0.0 | taskStateStore.get / set / delete / clear |
| capability: `asset-state-store` | MAY | ✗ | – | – | no task-facing asset state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | – | no task-facing asset-event API yet |
| capability: `object-store` | MAY | ✗ | – | – | no object-storage API yet |
| capability: `listeners` | MAY | ✗ | – | – | no hook into the Python listener API, so a task emits no OpenLineage facets of its own |
| **Dag definition** |  |  |  |  |  |
| capability: `dag-definition-authoring` | SHOULD | ✓ | 3.4.0 | 1.0.0 | new Dag |
| capability: `dag-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `dag-params` | MUST † | ✗ | – | – |  |
| capability: `dag-callbacks` | MAY † | ✗ | – | – |  |
| capability: `explicit-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 | before / after |
| capability: `edge-labels` | MAY † | ✓ | 3.4.0 | 1.0.0 | label(task, text) |
| capability: `taskflow-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-group` | MAY † | ✓ | 3.4.0 | 1.0.0 | dag.taskGroup |
| capability: `task-loops` | MAY † | ✗ | – | – |  |
| capability: `if-else-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | dag.if |
| capability: `switch-case-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | dag.switch |
| capability: `trigger-dag-run` | MAY † | ✓ | 3.4.0 | 1.0.0 | triggerDagRun |
| capability: `dynamic-dag-generation` | MAY † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `human-in-the-loop` | MAY † | ✓ | 3.4.0 | 1.0.0 | approval() / hitl() |
| capability: `dag-test` | SHOULD † | ✗ | – | – |  |
| capability: `asset-definition` | MAY † | ✗ | – | – |  |
| capability: `asset-scheduling` | MAY † | ✗ | – | – |  |
| capability: `mapped-tasks` | MAY † | ✗ | – | – |  |
| capability: `asset-inlets-outlets` | MAY † | ✗ | – | – |  |
| capability: `task-callbacks` | MAY † | ✗ | – | – | no SDK-defined task callbacks |
| **Python Dag with stub TaskHandler** |  |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.4.0 | 1.0.0 | @task.stub |
| capability: `taskflow-binding` | MUST | ✓ | 3.4.0 | 1.0.0 | mapped stubs excluded |
| capability: `mapped-tasks` | MAY | ✗ | – | – | Airflow does not bind arguments to mapped stubs yet |
| capability: `asset-inlets-outlets` | MAY | ✗ | – | – | runtime does not report task outlets yet |
| capability: `task-callbacks` | MAY | ✗ | – | – | a Python-declared task callback does not run for a stub task |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `dag-definition-authoring` is supported.*

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
