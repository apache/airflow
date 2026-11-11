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

# TypeScript SDK examples

Example Dags for the Apache Airflow TypeScript SDK. See the [TypeScript SDK guide][guide] for the full documentation.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html

## Build

Build the SDK, then the example bundles into `dist/`:

```bash
cd ts-sdk
pnpm install
pnpm run build

cd example
pnpm install
pnpm run build               # dist/bundle.min.mjs
pnpm run build:hitl          # dist/hitl.min.mjs
pnpm run build:ai-approval   # dist/ai-approval.min.mjs
```

## Configure Airflow

Register `dist/` as a Dag bundle, and route the `typescript` queue to the Node.js coordinator:

```bash
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
  {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
  {
    "name": "ts-task-handlers",
    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
    "kwargs": {"path": "/absolute/path/to/airflow/ts-sdk/example/dist"}
  }
]'
export AIRFLOW__SDK__COORDINATORS='{
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"task_handler_bundle_name": "ts-task-handlers"}
  }
}'
export AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"typescript": "ts"}'
```

The workers and the Dag processor need Node.js 22 or later on `PATH`.

## Dag definition

These Dags are declared entirely in TypeScript:

| Dag                         | Source               | Shows                                                                                  |
| --------------------------- | -------------------- | -------------------------------------------------------------------------------------- |
| `typescript_native_example` | `src/native.ts`      | Task inputs, a task group, `dag.if`, `dag.switch`, and `triggerDagRun`                 |
| `ts_hitl_*`                 | `src/hitl.ts`        | One Dag per human-in-the-loop feature, such as approvals, options, timeouts, and forms |
| `ts_hitl_ai_approval`       | `src/ai-approval.ts` | An agent that asks a person to review its tool call before it acts                     |

`typescript_native_example` triggers `typescript_example`, so set up the [Python Dags](#python-dag-with-stub-taskhandler) too.
Set the `typescript_native_cadence` Variable to `weekly` to take the other branch of `dag.switch`.

```bash
airflow dags trigger typescript_native_example
```

Answer the human-in-the-loop requests on the **Required Actions** page.

## Python Dag with stub TaskHandler

These Dags are declared in Python, and their stub tasks are implemented in TypeScript:

| Dag                           | Source                                                   | Shows                                                        |
| ----------------------------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| `typescript_example`          | `dags/typescript_example.py`, `src/main.ts`              | Variables, Connections, XComs, and the task state store      |
| `typescript_taskflow_example` | `dags/typescript_taskflow_example.py`, `src/taskflow.ts` | TaskFlow arguments passed from Python to TypeScript handlers |

Copy both files in `dags/` into your Airflow Dags folder, and create the Variable and Connection
`typescript_example` reads:

```bash
airflow variables set typescript_example_greeting "hello from Airflow"
airflow connections add typescript_example_http \
  --conn-type http \
  --conn-host example.com \
  --conn-login user \
  --conn-password pass
```

```bash
airflow dags trigger typescript_example
airflow dags trigger typescript_taskflow_example
```
