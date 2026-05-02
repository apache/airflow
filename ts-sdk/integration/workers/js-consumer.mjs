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

// Pure-JavaScript consumer of @apache-airflow/ts-sdk.
//
// No TypeScript, no tsx, no transpile step at runtime. Imports the built
// dist/ output directly — proves the "plain-JS users consume the compiled
// output and skip the types" claim.
//
// Run:
//   pnpm run build       # produce dist/
//   node integration/workers/js-consumer.mjs

import { registerTask, startWorker } from "../../dist/index.js";

registerTask("core_smoke", async ({ ctx, job }) => {
    console.log("[js-handler] core_smoke invoked", {
        dagId: ctx.dagId,
        taskId: ctx.taskId,
        runId: ctx.runId,
        tryNumber: ctx.tryNumber,
        mapIndex: ctx.mapIndex,
        taskInstanceId: ctx.taskInstanceId,
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    console.log(
        `[js-handler] core_smoke done (token length=${job.command.token.length})`,
    );
    return { ok: true };
});

const BASE_URL = process.env.AIRFLOW__EDGE__API_URL
    ? process.env.AIRFLOW__EDGE__API_URL.replace(/\/edge_worker\/v1.*$/, "")
    : "http://localhost:8080";
const SECRET = process.env.AIRFLOW__API_AUTH__JWT_SECRET ?? "dev-jwt-secret-not-for-prod";

await startWorker({
    baseUrl: BASE_URL,
    secret: SECRET,
    queues: ["core-integration-test"],
    pollIntervalInMs: 2000,
    heartbeatIntervalInMs: 10000,
});
