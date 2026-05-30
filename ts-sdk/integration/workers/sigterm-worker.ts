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
 * SIGTERM-during-handler worker. Handler sleeps ~20s while honoring
 * AbortSignal. SIGTERM during the handler propagates to ctx.signal, aborts
 * the sleep, and the handler throws. Pair with the `core_sigterm_inflight`
 * DAG; expected final TI state=failed (because handler threw).
 *
 * Run:
 *   npx tsx integration/workers/sigterm-worker.ts
 *
 * See ../../TESTING.md for setup.
 */
import { registerTask, startWorker } from "../../src/index.js";

const BASE_URL = process.env.AIRFLOW__EDGE__API_URL
    ? process.env.AIRFLOW__EDGE__API_URL.replace(/\/edge_worker\/v1.*$/, "")
    : "http://localhost:8080";
const SECRET = process.env.AIRFLOW__API_AUTH__JWT_SECRET ?? "dev-jwt-secret-not-for-prod";

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
        if (signal.aborted) return reject(signal.reason ?? new Error("aborted"));
        const t = setTimeout(() => {
            signal.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        const onAbort = () => {
            clearTimeout(t);
            reject(signal.reason ?? new Error("aborted"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
    });
}

registerTask("core_slow", async ({ ctx }) => {
    console.log(`[handler] core_slow starting (sleeping 20s) ti=${ctx.taskInstanceId}`);
    await abortableSleep(20_000, ctx.signal);
    console.log(`[handler] core_slow completed normally`);
});

await startWorker({
    baseUrl: BASE_URL,
    secret: SECRET,
    queues: ["core-sigterm-test"],
    pollIntervalInMs: 1000,
    heartbeatIntervalInMs: 10000,
});
