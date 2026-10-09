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

# TypeScript Coordinator Runtime Example

This example shows both ways to use the TypeScript SDK, served from one bundle:

- `src/native.ts` declares the `typescript_native_example` Dag entirely in TypeScript: its schedule, task
  options and dependencies, including a task group, a condition, a multi-way branch and a triggered Dag run.
- `dags/typescript_example.py` and `dags/typescript_taskflow_example.py` declare two Python Dags with stub
  tasks, and `src/main.ts` and `src/taskflow.ts` implement them with a `TaskHandler` per stub task.
  Both Dags have a task called `build_message`; a handler names its `(dag_id, task_id)` pair, so the two are
  different tasks with different bodies.
- `dist/bundle.min.mjs` is the built bundle Airflow runs.

The build uses the SDK's `airflow-ts-pack` tool, which builds the entry module and everything it imports into
that single file.

## Build

Build the SDK first so the example can import the local package:

```bash
cd ts-sdk
pnpm install
pnpm run build
```

Build the example bundle and its metadata:

```bash
cd ts-sdk/example
pnpm install
pnpm run build
```

The coordinator expects this layout:

```text
ts-sdk/example/dist/
  bundle.min.mjs
```

## Airflow Configuration

Register the example bundle directory as a Dag bundle, and configure Airflow to
route the `typescript` queue to the Node coordinator that reads it:

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

Copy both files in `dags/` into your Airflow Dags folder.

The example also reads one Variable and one Connection:

```bash
airflow variables set typescript_example_greeting "hello from Airflow"
airflow connections add typescript_example_http \
  --conn-type http \
  --conn-host example.com \
  --conn-login user \
  --conn-password pass
```

`write_and_delete_variable` writes the Variables it needs: it records the run id in
`typescript_example_last_run` and deletes the `typescript_example_scratch` Variable it has just written.

Then start Airflow and trigger the Dag:

```bash
airflow dags trigger typescript_example
airflow dags trigger typescript_taskflow_example
```
