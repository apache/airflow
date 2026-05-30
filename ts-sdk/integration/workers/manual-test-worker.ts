/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Reference worker for manual end-to-end testing against a running Airflow.
 * Registers handlers for several test task_ids and listens on all the
 * corresponding queues, so a single worker process can service every
 * integration DAG in `integration/core_*.py`.
 *
 * Run:
 *   npx tsx integration/workers/manual-test-worker.ts
 *
 * Trigger from the Airflow side:
 *   airflow dags trigger core_integration       → expect success
 *   airflow dags trigger core_failure           → expect failed
 *   airflow dags trigger core_missing_handler   → expect failed (no handler registered)
 *   airflow dags trigger core_sigterm_inflight  → start, then Ctrl+C the worker
 *                                                 mid-task; expect success
 *                                                 once the handler completes
 *
 * See ../../TESTING.md for the full setup recipe (breeze, env vars, etc.).
 */

import { registerTask, startWorker } from "../../src/index.js";

const BASE_URL = process.env.AIRFLOW__EDGE__API_URL
    ? process.env.AIRFLOW__EDGE__API_URL.replace(/\/edge_worker\/v1.*$/, "")
    : "http://localhost:8080";
const SECRET = process.env.AIRFLOW__API_AUTH__JWT_SECRET ?? "dev-jwt-secret-not-for-prod";

// Happy path.
registerTask("core_smoke", async ({ ctx }) => {
    console.log(`[core_smoke] invoked dag=${ctx.dagId} run=${ctx.runId}`);
    await new Promise<void>((r) => setTimeout(r, 500));
    console.log(`[core_smoke] done`);
    return { ok: true };
});

// Failure path — handler throws, worker reports state=failed.
registerTask("core_fail", async ({ ctx }) => {
    console.log(`[core_fail] invoked dag=${ctx.dagId} run=${ctx.runId}`);
    throw new Error("intentional failure for manual test");
});

// Graceful shutdown — handler runs for 2 minutes, ignoring abort signal.
// Ctrl+C the worker while this is running to confirm the worker drains by
// waiting for the in-flight task rather than killing it.
registerTask("core_slow", async ({ ctx }) => {
    console.log(`[core_slow] sleeping 2 min — Ctrl+C the worker mid-sleep to test drain`);
    const startedAt = Date.now();
    await new Promise<void>((resolve) => setTimeout(resolve, 120_000));
    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`[core_slow] done after ${elapsed}s (signal aborted=${ctx.signal?.aborted})`);
    return { ok: true };
});

// Note: NO handler registered for "core_unknown". The core_missing_handler
// DAG dispatches a task with that id; the worker should mark it failed with
// a "no handler registered" log line.

await startWorker({
    baseUrl: BASE_URL,
    secret: SECRET,
    queues: [
        "core-integration-test",  // core_smoke
        "core-failure-test",       // core_fail
        "core-sigterm-test",       // core_slow
        "core-missing-test",       // core_unknown (no handler)
    ],
    pollIntervalInMs: 2000,
    heartbeatIntervalInMs: 10000,
});
