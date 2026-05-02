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

// Execution API client. Auth uses "Bearer <token>" (token is server-issued
// in the job payload at command.token). Contrast with Edge API's raw JWT.
//
// Two-token lifecycle (Airflow PR #60108, merged 2026-04-23):
//   - command.token is "workload"-scoped and only accepted on /run.
//   - /run swaps it for an "execution"-scoped token returned in the
//     `Refreshed-API-Token` response header.
//   - Subsequent calls (markSuccess, markFailed, XCom, etc.) require the
//     execution token. The same header is also used by the server for
//     near-expiry refresh on any endpoint.
// The bearerMiddleware below captures the header on every response and updates
// `currentToken` so the next request uses it.
//
// TODO(pr2): per-TI heartbeat.
// TODO(pr3): getVariable, getConnection, XCom push/pull.

import createClient, { type Middleware } from "openapi-fetch";
import type { paths } from "./generated/execution.js";
import { ExecutionApiError } from "./errors.js";
import { withRetry, type RetryConfig } from "./utils/retry.js";

/** TI lifecycle states written on the wire. */
export const TI_STATE = {
    RUNNING: "running",
    SUCCESS: "success",
    FAILED: "failed",
} as const;

export interface EnterRunningArgs {
    hostname: string;
    unixname: string;
    pid: number;
    start_date: string;
}
export interface MarkSuccessArgs {
    end_date: string;
}
export interface MarkFailedArgs {
    end_date: string;
}

export interface ExecutionClientConfig {
    /** Base URL of the Airflow api-server. We append `/execution`. */
    baseUrl: string;
    /** Task instance UUID from the job payload. */
    taskInstanceId: string;
    /** Server-issued bearer token (from `job.command.token`). */
    token: string;
    fetchImpl?: typeof fetch;
    retry?: RetryConfig;
    /** API version header. Default "2025-08-10" (matches Airflow 3.2+). */
    apiVersion?: string;
    /** Per-request timeout. Default 120 000 (2 min). See decisions/01. */
    requestTimeoutMs?: number;
}

export interface ExecutionClient {
    readonly taskInstanceId: string;
    /** Transition queued → running. */
    enterRunning(body: EnterRunningArgs): Promise<void>;
    markSuccess(body: MarkSuccessArgs): Promise<void>;
    markFailed(body: MarkFailedArgs): Promise<void>;
}

export function makeExecutionClient(cfg: ExecutionClientConfig): ExecutionClient {
    const baseUrl = cfg.baseUrl.replace(/\/$/, "");
    const apiVersion = cfg.apiVersion ?? "2025-08-10";
    const requestTimeoutMs = cfg.requestTimeoutMs ?? 120_000;

    // Mutable: the server reissues via `Refreshed-API-Token` (workload→execution
    // swap on /run, near-expiry refresh on any endpoint).
    let currentToken = cfg.token;

    const bearerMiddleware: Middleware = {
        async onRequest({ request }) {
            request.headers.set("Authorization", `Bearer ${currentToken}`);
            request.headers.set("Airflow-API-Version", apiVersion);
            return request;
        },
        async onResponse({ response }) {
            const refreshed = response.headers.get("Refreshed-API-Token");
            if (refreshed) currentToken = refreshed;
            return response;
        },
    };

    const baseFetch = cfg.fetchImpl ?? fetch;
    const timedFetch: typeof fetch = (input, init) =>
        baseFetch(input, { ...init, signal: AbortSignal.timeout(requestTimeoutMs) });

    const client = createClient<paths>({
        baseUrl: `${baseUrl}/execution`,
        fetch: timedFetch,
    });
    client.use(bearerMiddleware);

    /** Run an openapi-fetch call under the configured retry policy. Converts
     *  the `{data, error}` shape into a thrown `ExecutionApiError` on failure.
     *  Returns `data` only (errors throw, callers don't have to discriminate). */
    function call<T>(
        method: string,
        path: string,
        fn: () => Promise<{ data?: T; error?: unknown; response: Response }>,
    ): Promise<NonNullable<T>> {
        return withRetry(async () => {
            const result = await fn();
            if (result.error !== undefined) {
                const body =
                    typeof result.error === "string"
                        ? result.error
                        : JSON.stringify(result.error);
                throw new ExecutionApiError(method, path, result.response.status, body);
            }
            return result.data as NonNullable<T>;
        }, cfg.retry);
    }

    return {
        taskInstanceId: cfg.taskInstanceId,

        async enterRunning(body) {
            await call("PATCH", "/task-instances/{id}/run", () =>
                client.PATCH("/task-instances/{task_instance_id}/run", {
                    params: { path: { task_instance_id: cfg.taskInstanceId } },
                    body: {
                        state: TI_STATE.RUNNING,
                        hostname: body.hostname,
                        unixname: body.unixname,
                        pid: body.pid,
                        start_date: body.start_date,
                    },
                }),
            );
        },

        async markSuccess(body) {
            await call("PATCH", "/task-instances/{id}/state", () =>
                client.PATCH("/task-instances/{task_instance_id}/state", {
                    params: { path: { task_instance_id: cfg.taskInstanceId } },
                    body: {
                        state: TI_STATE.SUCCESS,
                        end_date: body.end_date,
                        task_outlets: [],
                        outlet_events: [],
                    },
                }),
            );
        },

        async markFailed(body) {
            await call("PATCH", "/task-instances/{id}/state", () =>
                client.PATCH("/task-instances/{task_instance_id}/state", {
                    params: { path: { task_instance_id: cfg.taskInstanceId } },
                    body: {
                        state: TI_STATE.FAILED,
                        end_date: body.end_date,
                    },
                }),
            );
        },
    };
}
