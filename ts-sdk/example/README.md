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

This example shows the coordinator-mode shape for TypeScript task handlers:

- `dags/typescript_example.py` and `dags/typescript_taskflow_example.py` declare two Airflow Dags and their stub tasks.
- `src/main.ts` and `src/taskflow.ts` register a `TaskHandler` per stub task and start the coordinator runtime.
  One bundle provides for both Dags, and both declare a task called `build_message`.
  A handler binds the `(dag_id, task_id)` pair, so the two are different tasks with different bodies.
- `dist/bundle.min.mjs` is the generated Node.js bundle that Airflow launches.

The build uses the SDK's `airflow-ts-pack` tool, which bundles the entrypoint
with esbuild and embeds the Airflow metadata generated from the bundle's
registered tasks, producing a single deployable file.

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

Configure Airflow to route the `typescript` queue to the Node coordinator and
point it at the example bundle directory:

```bash
export AIRFLOW__SDK__COORDINATORS='{
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"bundles_root": ["/absolute/path/to/airflow/ts-sdk/example/dist"]}
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
