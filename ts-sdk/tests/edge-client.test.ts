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

import { describe, it, expect } from "vitest";
import { makeEdgeClient, type WorkerStateBody } from "../src/edge-client.js";
import { EdgeApiError } from "../src/errors.js";

const SECRET = "test-secret-not-for-prod";
const BASE_URL = "http://airflow.test";
const WORKER = "alpha";
const ISSUER = "airflow";

function stateBody(state: WorkerStateBody["state"]): WorkerStateBody {
    return {
        state,
        jobs_active: 0,
        queues: ["ts-tasks"],
        sysinfo: {
            airflow_version: "3.2.0",
            edge_provider_version: "3.3.0",
            concurrency: 1,
            free_concurrency: 1,
        },
    };
}

/** Record-shape for an observed call. openapi-fetch passes a `Request` object,
 *  so we extract URL/method/headers/body into a flat shape the tests can inspect. */
interface CallRecord {
    url: string;
    method: string;
    headers: Record<string, string>;
    body: string | null;
    init: { method: string; headers: Record<string, string>; body: string | null };
}

/** Build a fake fetch that records calls and returns a canned response. */
function mockFetch(
    responder: (call: CallRecord) => Response | Promise<Response>,
): { fetch: typeof fetch; calls: CallRecord[] } {
    const calls: CallRecord[] = [];
    const impl = async (input: Request | URL | string, init?: RequestInit) => {
        let url: string;
        let method: string;
        let headers: Record<string, string>;
        let body: string | null;
        if (input instanceof Request) {
            url = input.url;
            method = input.method;
            headers = Object.fromEntries(input.headers.entries());
            body = input.body ? await input.clone().text() : null;
        } else {
            url = typeof input === "string" ? input : input.toString();
            method = init?.method ?? "GET";
            headers = toHeaderRecord(
                init?.headers as Headers | Record<string, string> | string[][] | undefined,
            );
            body = typeof init?.body === "string" ? init.body : null;
        }
        const call: CallRecord = {
            url,
            method,
            headers,
            body,
            init: { method, headers, body },
        };
        calls.push(call);
        return responder(call);
    };
    return { fetch: impl as typeof fetch, calls };
}

function toHeaderRecord(
    headers: Headers | Record<string, string> | string[][] | undefined,
): Record<string, string> {
    if (!headers) return {};
    if (headers instanceof Headers) return Object.fromEntries(headers.entries());
    if (Array.isArray(headers)) return Object.fromEntries(headers);
    return headers;
}

describe("makeEdgeClient — auth + path shape", () => {
    it("sends Authorization header as raw JWT (no Bearer prefix)", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const client = makeEdgeClient({
            baseUrl: BASE_URL,
            secret: SECRET,
            workerName: WORKER,
            jwtIssuer: ISSUER,
            fetchImpl,
        });
        await client.register(stateBody("starting"));
        expect(calls.length).toBe(1);
        const auth = calls[0]!.headers["authorization"] ?? calls[0]!.headers["Authorization"];
        expect(auth).toBeTruthy();
        expect(auth).not.toMatch(/^Bearer /);
        expect(auth!.split(".").length).toBe(3); // JWT
    });

    it("builds the register URL with the right path", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        await client.register(stateBody("starting"));
        expect(calls[0]!.url).toBe(`${BASE_URL}/edge_worker/v1/worker/${WORKER}`);
        expect(calls[0]!.method).toBe("POST");
    });

    it("plumbs jwtIssuer through to the JWT iss claim", async () => {
        function decodeIss(jwt: string): string {
            const payload = jwt.split(".")[1]!;
            const padded = payload + "=".repeat((4 - payload.length % 4) % 4);
            const std = padded.replace(/-/g, "+").replace(/_/g, "/");
            return JSON.parse(Buffer.from(std, "base64").toString("utf8")).iss;
        }

        const m = mockFetch(() => new Response("null", { status: 200 }));
        const client = makeEdgeClient({
            baseUrl: BASE_URL,
            secret: SECRET,
            workerName: WORKER,
            fetchImpl: m.fetch,
            jwtIssuer: "my-deployment",
        });
        await client.register(stateBody("starting"));
        const auth = m.calls[0]!.headers["authorization"] ?? m.calls[0]!.headers["Authorization"];
        expect(decodeIss(auth!)).toBe("my-deployment");
    });

    it("exposes workerName", () => {
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER });
        expect(client.workerName).toBe(WORKER);
    });

    it("trims trailing slash from baseUrl", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const client = makeEdgeClient({
            baseUrl: `${BASE_URL}/`,
            secret: SECRET,
            workerName: WORKER,
            jwtIssuer: ISSUER,
            fetchImpl,
        });
        await client.register(stateBody("starting"));
        expect(calls[0]!.url).toBe(`${BASE_URL}/edge_worker/v1/worker/${WORKER}`);
    });
});

describe("makeEdgeClient — updateState", () => {
    it("returns parsed WorkerSetStateReturn payload", async () => {
        const payload = { state: "running", queues: ["ts-tasks"] };
        const { fetch: fetchImpl } = mockFetch(
            () => new Response(JSON.stringify(payload), { status: 200 }),
        );
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        const resp = await client.updateState(stateBody("running"));
        expect(resp.state).toBe("running");
    });

    it("surfaces shutdown request from server", async () => {
        const { fetch: fetchImpl } = mockFetch(
            () =>
                new Response(JSON.stringify({ state: "shutdown request", queues: ["ts-tasks"] }), {
                    status: 200,
                }),
        );
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        const resp = await client.updateState(stateBody("running"));
        expect(resp.state).toBe("shutdown request");
    });
});

describe("makeEdgeClient — fetchJob", () => {
    it("returns null when response is an empty object (no job)", async () => {
        const { fetch: fetchImpl } = mockFetch(() => new Response("{}", { status: 200 }));
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        const job = await client.fetchJob({ queues: ["q"], free_concurrency: 1 });
        expect(job).toBeNull();
    });

    it("returns the full payload when a job is available", async () => {
        const jobPayload = {
            dag_id: "d",
            task_id: "t",
            run_id: "r",
            try_number: 1,
            map_index: -1,
            command: {
                token: "tok",
                ti: { id: "tiid" },
                dag_rel_path: "dag.py",
                bundle_info: { name: "b" },
                log_path: null,
                sentry_integration: "none",
                type: "ExecuteTask",
            },
            concurrency_slots: 1,
        };
        const { fetch: fetchImpl } = mockFetch(
            () => new Response(JSON.stringify(jobPayload), { status: 200 }),
        );
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        const job = await client.fetchJob({ queues: ["q"], free_concurrency: 1 });
        expect(job).not.toBeNull();
        expect(job?.dag_id).toBe("d");
        expect(job?.command.token).toBe("tok");
    });
});

describe("makeEdgeClient — reportJobState", () => {
    it("builds the correct path with all 6 segments", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(
            () => new Response("null", { status: 200 }),
        );
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        await client.reportJobState({
            dagId: "d",
            taskId: "t",
            runId: "r",
            tryNumber: 1,
            mapIndex: -1,
            state: "success",
        });
        expect(calls[0]!.url).toBe(
            `${BASE_URL}/edge_worker/v1/jobs/state/d/t/r/1/-1/success`,
        );
        expect(calls[0]!.method).toBe("PATCH");
    });
});

describe("makeEdgeClient — error handling", () => {
    it("throws EdgeApiError on 4xx with fail-fast (no retry)", async () => {
        let calls = 0;
        const { fetch: fetchImpl } = mockFetch(() => {
            calls += 1;
            return new Response("{\"detail\":\"bad request\"}", { status: 400 });
        });
        const client = makeEdgeClient({
            baseUrl: BASE_URL,
            secret: SECRET,
            workerName: WORKER,
            jwtIssuer: ISSUER,
            fetchImpl,
            retry: { maxAttempts: 3, baseBackoffMs: 1 },
        });
        await expect(client.register(stateBody("starting"))).rejects.toBeInstanceOf(EdgeApiError);
        expect(calls).toBe(1); // no retry on 4xx
    });

    it("retries on 5xx, succeeds on third attempt", async () => {
        let calls = 0;
        const { fetch: fetchImpl } = mockFetch(() => {
            calls += 1;
            if (calls < 3) return new Response("server error", { status: 503 });
            return new Response("null", { status: 200 });
        });
        const client = makeEdgeClient({
            baseUrl: BASE_URL,
            secret: SECRET,
            workerName: WORKER,
            jwtIssuer: ISSUER,
            fetchImpl,
            retry: { maxAttempts: 3, baseBackoffMs: 1 },
        });
        await client.register(stateBody("starting"));
        expect(calls).toBe(3);
    });

    it("throws EdgeApiError after max attempts on persistent 5xx", async () => {
        let calls = 0;
        const { fetch: fetchImpl } = mockFetch(() => {
            calls += 1;
            return new Response("server down", { status: 503 });
        });
        const client = makeEdgeClient({
            baseUrl: BASE_URL,
            secret: SECRET,
            workerName: WORKER,
            jwtIssuer: ISSUER,
            fetchImpl,
            retry: { maxAttempts: 2, baseBackoffMs: 1 },
        });
        await expect(client.register(stateBody("starting"))).rejects.toBeInstanceOf(EdgeApiError);
        expect(calls).toBe(2);
    });

    it("EdgeApiError includes status, method, and path", async () => {
        const { fetch: fetchImpl } = mockFetch(
            () => new Response("bad", { status: 400 }),
        );
        const client = makeEdgeClient({ baseUrl: BASE_URL, secret: SECRET, workerName: WORKER, jwtIssuer: ISSUER, fetchImpl });
        try {
            await client.register(stateBody("starting"));
            expect.fail("should have thrown");
        } catch (err) {
            expect(err).toBeInstanceOf(EdgeApiError);
            const e = err as EdgeApiError;
            expect(e.status).toBe(400);
            expect(e.method).toBe("POST");
            expect(e.path).toContain("worker");
        }
    });
});
