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

// Edge Worker API client. Wraps openapi-fetch with HS512 JWT signing
// per request. Authorization header is RAW JWT (no Bearer prefix) —
// Edge API uses FastAPI's raw Header dependency.

import createClient, { type Middleware } from "openapi-fetch";
import type { paths, components } from "./generated/edge.js";
import { EdgeApiError } from "./errors.js";
import { signEdgeJwt, pathToEdgeMethod } from "./utils/jwt.js";
import { withRetry, type RetryConfig } from "./utils/retry.js";

const EDGE_PREFIX = "/edge_worker/v1";
type EDGE_PREFIX = typeof EDGE_PREFIX;

// Makes the `authorization` header optional on every operation; the JWT
// middleware sets it, so callers shouldn't have to.
type DropAuthHeaderFromOp<Op> = Op extends { parameters: infer P }
    ? Omit<Op, "parameters"> & {
          parameters: Omit<P, "header"> & { header?: { authorization?: string } };
      }
    : Op;

type DropAuthHeader<Path> = { [M in keyof Path]: DropAuthHeaderFromOp<Path[M]> };

// Narrows the paths map to worker endpoints only and strips the
// `/edge_worker/v1` prefix from each key; the runtime baseUrl supplies it.
type StrippedPaths = {
    [P in keyof paths as P extends `${EDGE_PREFIX}${infer Rest}` ? Rest : never]: DropAuthHeader<
        paths[P]
    >;
};

export type EdgeWorkerState = components["schemas"]["EdgeWorkerState"];
export type WorkerStateBody = components["schemas"]["WorkerStateBody"];
export type WorkerSetStateReturn = components["schemas"]["WorkerSetStateReturn"];
export type EdgeJobFetched = components["schemas"]["EdgeJobFetched"];
export type WorkerQueuesBody = components["schemas"]["WorkerQueuesBody"];

/** Worker lifecycle states matching `WorkerStateBody.state`. Use these
 *  constants instead of string literals at call sites — `satisfies` anchors
 *  every value to the spec union, so a typo fails to compile. */
export const WORKER_STATE = {
    STARTING: "starting",
    RUNNING: "running",
    IDLE: "idle",
    SHUTDOWN_REQUEST: "shutdown request",
    TERMINATING: "terminating",
    OFFLINE: "offline",
} as const satisfies Record<string, WorkerStateBody["state"]>;

/** Terminal states accepted by `reportJobState`. */
export const JOB_STATE = {
    SUCCESS: "success",
    FAILED: "failed",
} as const;
export type JobState = (typeof JOB_STATE)[keyof typeof JOB_STATE];

export interface ReportJobStateArgs {
    dagId: string;
    taskId: string;
    runId: string;
    tryNumber: number;
    mapIndex: number;
    state: JobState;
}


export interface EdgeClientConfig {
    baseUrl: string;
    secret: string;
    workerName: string;
    fetchImpl?: typeof fetch;
    retry?: RetryConfig;
    /** Default 30. */
    jwtTtlSeconds?: number;
    /** Per-request timeout. Default 120 000 (2 min). */
    requestTimeoutMs?: number;
    /** JWT `iss` claim. Must match the server's `[api_auth] jwt_issuer`
     *  config. Required: callers should resolve via
     *  `worker-options.resolveWorkerOptions`, which applies the project-wide
     *  default ("airflow"). */
    jwtIssuer: string;
}

export interface EdgeClient {
    readonly workerName: string;
    register(body: WorkerStateBody): Promise<void>;
    /** Response may carry `state="shutdown request"` signaling server-driven drain. */
    updateState(body: WorkerStateBody): Promise<WorkerSetStateReturn>;
    /** Returns null when no job is available. */
    fetchJob(body: WorkerQueuesBody): Promise<EdgeJobFetched | null>;
    reportJobState(args: ReportJobStateArgs): Promise<void>;
}

export function makeEdgeClient(cfg: EdgeClientConfig): EdgeClient {
    const baseUrl = cfg.baseUrl.replace(/\/$/, "");
    const jwtTtl = cfg.jwtTtlSeconds ?? 30;
    const requestTimeoutMs = cfg.requestTimeoutMs ?? 120_000;

    const jwtMiddleware: Middleware = {
        async onRequest({ request }) {
            const jwt = signEdgeJwt(cfg.secret, {
                method: pathToEdgeMethod(new URL(request.url).pathname),
                issuer: cfg.jwtIssuer,
                ttlSeconds: jwtTtl,
            });
            request.headers.set("Authorization", jwt);
            return request;
        },
    };

    const baseFetch = cfg.fetchImpl ?? fetch;
    const timedFetch: typeof fetch = (input, init) =>
        baseFetch(input, { ...init, signal: AbortSignal.timeout(requestTimeoutMs) });

    const client = createClient<StrippedPaths>({
        baseUrl: `${baseUrl}${EDGE_PREFIX}`,
        fetch: timedFetch,
    });
    client.use(jwtMiddleware);

    /** Run an openapi-fetch call under the configured retry policy. Converts
     *  the `{data, error}` shape into a thrown `EdgeApiError` on failure. */
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
                throw new EdgeApiError(method, path, result.response.status, body);
            }
            return result.data as NonNullable<T>;
        }, cfg.retry);
    }

    return {
        workerName: cfg.workerName,

        async register(body) {
            await call("POST", "/worker/{worker_name}", () =>
                client.POST("/worker/{worker_name}", {
                    params: { path: { worker_name: cfg.workerName } },
                    body,
                }),
            );
        },

        async updateState(body) {
            return call("PATCH", "/worker/{worker_name}", () =>
                client.PATCH("/worker/{worker_name}", {
                    params: { path: { worker_name: cfg.workerName } },
                    body,
                }),
            );
        },

        async fetchJob(body) {
            const data = await call("POST", "/jobs/fetch/{worker_name}", () =>
                client.POST("/jobs/fetch/{worker_name}", {
                    params: { path: { worker_name: cfg.workerName } },
                    body,
                }),
            );
            // Server returns {} / null / undefined when no job is available.
            if (!data || typeof data !== "object" || Object.keys(data).length === 0) {
                return null;
            }
            return data;
        },

        async reportJobState(args) {
            await call("PATCH", "/jobs/state/.../{state}", () =>
                client.PATCH(
                    "/jobs/state/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}/{state}",
                    {
                        params: {
                            path: {
                                dag_id: args.dagId,
                                task_id: args.taskId,
                                run_id: args.runId,
                                try_number: args.tryNumber,
                                map_index: args.mapIndex,
                                state: args.state,
                            },
                        },
                    },
                ),
            );
        },
    };
}
