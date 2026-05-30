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
 * Failure-path worker — handler throws on every call. Pair with the
 * `core_failure` DAG; expect TI state=failed.
 *
 * Run:
 *   npx tsx integration/workers/failure-worker.ts
 *
 * See ../../TESTING.md for setup.
 */
import { registerTask, startWorker } from "../../src/index.js";

const BASE_URL = process.env.AIRFLOW__EDGE__API_URL
    ? process.env.AIRFLOW__EDGE__API_URL.replace(/\/edge_worker\/v1.*$/, "")
    : "http://localhost:8080";
const SECRET = process.env.AIRFLOW__API_AUTH__JWT_SECRET ?? "dev-jwt-secret-not-for-prod";

registerTask("core_fail", async ({ ctx }) => {
    console.log(`[handler] core_fail invoked ti=${ctx.taskInstanceId}`);
    throw new Error("intentional failure for integration test");
});

await startWorker({
    baseUrl: BASE_URL,
    secret: SECRET,
    queues: ["core-failure-test"],
    pollIntervalInMs: 2000,
    heartbeatIntervalInMs: 10000,
});
