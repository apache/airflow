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
 * Smoke-test worker — registers a single handler for task_id "core_smoke"
 * on queue "core-integration-test". Pair with the `core_integration` DAG.
 *
 * Run:
 *   npx tsx integration/workers/smoke-worker.ts
 *
 * For a multi-scenario worker that exercises success/failure/drain in one
 * process, see `manual-test-worker.ts`. See ../../TESTING.md for setup.
 */

import { registerTask, startWorker } from "../../src/index.js";

const BASE_URL = process.env.AIRFLOW__EDGE__API_URL
    ? process.env.AIRFLOW__EDGE__API_URL.replace(/\/edge_worker\/v1.*$/, "")
    : "http://localhost:8080";
const SECRET = process.env.AIRFLOW__API_AUTH__JWT_SECRET ?? "dev-jwt-secret-not-for-prod";

registerTask("core_smoke", async ({ ctx, job }) => {
    console.log(`[handler] core_smoke invoked`, {
        dagId: ctx.dagId,
        taskId: ctx.taskId,
        runId: ctx.runId,
        tryNumber: ctx.tryNumber,
        mapIndex: ctx.mapIndex,
        taskInstanceId: ctx.taskInstanceId,
    });
    // Simulate some work; nothing real, but prove AbortSignal is wired.
    await new Promise<void>((resolve) => setTimeout(resolve, 500));
    console.log(`[handler] core_smoke done (job.command.token length=${
        (job as { command: { token: string } }).command.token.length
    })`);
    return { ok: true };
});

await startWorker({
    baseUrl: BASE_URL,
    secret: SECRET,
    queues: ["core-integration-test"],
    pollIntervalInMs: 2000,
    heartbeatIntervalInMs: 10000,
});
