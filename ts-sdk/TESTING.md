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

# Testing the TypeScript SDK

## Unit tests

```bash
pnpm install
pnpm test           # vitest, 69 tests, no network
pnpm run typecheck  # strict TS check
```

## End-to-end against a live Airflow

Drives a real Node worker against a real Airflow with the Edge Executor.
Use this when you change anything that touches the wire protocol.

You need an Airflow already running with `EdgeExecutor` configured. If
you don't have one, jump to **[Setting up a local Airflow with
breeze](#setting-up-a-local-airflow-with-breeze)** first, then come
back here.

### 1. Configure the worker

Set the required env vars (and optionally override the defaults):

```bash
# Required:
export AIRFLOW__API_AUTH__JWT_SECRET="<your-airflow-jwt-secret>"
export AIRFLOW__EDGE__API_URL="http://<airflow-host>:<port>/edge_worker/v1"

# Optional — defaults shown:
# export AIRFLOW__API_AUTH__JWT_ISSUER="airflow"
# export AIRFLOW__EDGE__AIRFLOW_VERSION="3.3.0"
# export AIRFLOW__EDGE__PROVIDER_VERSION="3.5.0"
```

The optional vars only matter if your Airflow customizes
`[api_auth] jwt_issuer` or runs a different version than the defaults.

### 2. Build and run the worker

```bash
cd ts-sdk
pnpm install && pnpm run build
npx tsx integration/workers/manual-test-worker.ts
```

You should see:

```
[worker] <hostname>-<pid>-<rand> registered on queues=core-integration-test,core-failure-test,core-sigterm-test,core-missing-test
```

### 3. Deploy the test DAGs

Copy them into your Airflow's `dags/` folder:

```bash
cp ts-sdk/integration/core_*.py <your-airflow-dags-folder>/
```

### 4. Trigger and verify

Easiest path: open the Airflow UI, find each DAG, unpause, hit ▶,
watch the task box turn green or red.

| DAG | Task | Expected | What it proves |
|---|---|---|---|
| `core_integration` | `core_smoke` | 🟢 success | Happy path: register, /run + token swap, handler, markSuccess |
| `core_failure` | `core_fail` | 🔴 failed | Handler throws → markFailed propagates |
| `core_missing_handler` | `core_unknown` | 🔴 failed | No handler registered → clean failure log |
| `core_sigterm_inflight` | `core_slow` | 🟢 success after 2 min | SIGTERM during handler → worker drains by waiting, never kills the task |

For `core_sigterm_inflight`: trigger the DAG, wait until the worker
prints `[core_slow] sleeping 2 min`, then `Ctrl+C` the worker. The
handler runs to completion before the worker exits.

### Pure-JS consumer canary

Proves the published-package experience for users who don't write
TypeScript. Imports the built `dist/` directly with stock `node`:

```bash
pnpm run build
node integration/workers/js-consumer.mjs
```

Triggers the same `core_integration` DAG.

## Setting up a local Airflow with breeze

If you don't already have an Airflow stack to test against, breeze
gives you one in ~5 min. Everything below is copy-paste from the host.

### Start breeze

```bash
breeze start-airflow --backend postgres --executor edgeexecutor
```

First run takes ~5 min to build images. The Airflow UI lands on
**http://localhost:28080** (login: `admin` / `admin`). Breeze maps
container port 8080 → host 28080.

### Set a reusable shell variable

```bash
export CONTAINER=$(docker ps --format '{{.Names}}' | grep breeze-airflow-run)
```

Verify the breeze config:

```bash
docker exec "$CONTAINER" airflow config get-value core executor
# expect: airflow.providers.edge3.executors.edge_executor.EdgeExecutor

docker exec "$CONTAINER" airflow config get-value api_auth jwt_issuer
# expect: airflow
```

### Get the env vars for the worker

Plug these into step 1 of the E2E test:

```bash
export AIRFLOW__API_AUTH__JWT_SECRET=$(docker exec "$CONTAINER" airflow config get-value api_auth jwt_secret 2>/dev/null | tail -1)
export AIRFLOW__EDGE__API_URL="http://localhost:28080/edge_worker/v1"
```

### Deploy the test DAGs

Breeze mounts `files/dags/` from the repo root into the container:

```bash
cp ts-sdk/integration/core_*.py files/dags/
```

Confirm the scheduler picked them up:

```bash
docker exec "$CONTAINER" airflow dags list | grep core_
```

### Trigger from the host (instead of the UI)

```bash
# Unpause all four DAGs once:
for d in core_integration core_failure core_missing_handler core_sigterm_inflight; do
    docker exec "$CONTAINER" airflow dags unpause "$d"
done

# Trigger one:
docker exec "$CONTAINER" airflow dags trigger core_integration

# Check the latest run state:
docker exec "$CONTAINER" airflow dags list-runs core_integration | head -3
```

### Cleanup

```bash
# Stop the SDK worker: Ctrl+C in the worker terminal
# Stop breeze:
breeze down
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ECONNREFUSED 127.0.0.1:8080` | breeze maps to host port **28080**, not 8080. |
| `403 Invalid issuer` | Set `AIRFLOW__API_AUTH__JWT_ISSUER` to match the server's `[api_auth] jwt_issuer`. SDK defaults to `"airflow"`. |
| `403 ... workload not allowed` | Stale build of this SDK. Pull latest, rebuild. |
| `400 Edge Worker runs on Airflow X.X.X and the core runs on Y.Y.Y` | Set `AIRFLOW__EDGE__AIRFLOW_VERSION` and `AIRFLOW__EDGE__PROVIDER_VERSION` to match the running Airflow. |
| Worker registers but never picks up jobs | DAG's `queue=` must match one of the worker's `queues: [...]` entries exactly. |
