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

// Public types exposed to user task code.
// TODO(pr2): extend TaskContext with TIRunContext fields (`dagRunConf`,
//            `maxTries`, `taskRescheduleCount`, etc.) — requires `enterRunning`
//            to return the response body instead of `Promise<void>`.
// TODO(pr3): add `client` to TaskHandlerArgs with sub-clients
//            (`client.xcom.{push,pull}`, `client.variables.get`,
//            `client.connections.get`).
// TODO(pr4): add `log` (forwarded to Edge API) to TaskHandlerArgs.

/** Per-task context delivered to every task handler invocation.
 *
 *  Minimal in PR #1 — see TODOs above for the APIs user tasks will need
 *  (XCom, Variables, Connections, dag_run.conf, etc.). */

import type { EdgeJobFetched } from "./edge-client.js";

export interface TaskContext {
    readonly dagId: string;
    readonly taskId: string;
    readonly runId: string;
    readonly tryNumber: number;
    /** -1 for non-mapped tasks, 0..N-1 for mapped instances. */
    readonly mapIndex: number;
    readonly taskInstanceId: string;
    /** AbortSignal that fires when the task should stop (SIGTERM drain or
     *  execution_timeout). User tasks should pass this to `fetch()`,
     *  `setTimeout` helpers, or any abortable API. */
    readonly signal: AbortSignal;
}

/** Arguments passed to every task handler. Adding fields is non-breaking
 *  for consumers that destructure by name. */
export interface TaskHandlerArgs {
    ctx: TaskContext;
    readonly job: EdgeJobFetched;
}

// TODO(pr3): capture handler return value and push to XCom (currently
//            discarded — generic <TReturn> exists so user types stay stable
//            when XCom lands).
/** User task handler function signature. */
export type TaskHandler<TReturn = unknown> = (args: TaskHandlerArgs) => Promise<TReturn>;

/** Configuration for `startWorker()`. */
export interface StartWorkerOptions {
    /** Airflow api-server base URL, e.g. `"http://localhost:8080"`.
     *  Falls back to `process.env.AIRFLOW__EDGE__API_URL`. */
    baseUrl?: string;
    /** Secret for Edge API JWT signing.
     *  Falls back to `process.env.AIRFLOW__API_AUTH__JWT_SECRET`. */
    secret?: string;
    /** Edge queue name(s) this worker listens on. At least one required. */
    queues: string[];
    /** Unique worker name. Defaults to `${hostname}-${pid}-${random}`. */
    workerName?: string;
    /** Milliseconds between poll attempts when idle. Default 5000. */
    pollIntervalInMs?: number;
    /** Milliseconds between worker-level heartbeats. Default 30000. */
    heartbeatIntervalInMs?: number;
    /** Consecutive heartbeat failures before the worker self-terminates.
     *  Default 10. */
    heartbeatFailureThreshold?: number;
    /** JWT `iss` claim sent on Edge API requests. Must match the server's
     *  `[api_auth] jwt_issuer` config. Falls back to
     *  `process.env.AIRFLOW__API_AUTH__JWT_ISSUER`, then `"airflow"`. */
    jwtIssuer?: string;
}
