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
import { makeExecutionClient } from "../src/execution-client.js";
import { ExecutionApiError } from "../src/errors.js";

const BASE_URL = "http://airflow.test";
const TI_ID = "01920cbd-deed-7feb-a6be-abcdef012345";
const TOKEN = "server.issued.bearer";

interface CallRecord {
    url: string;
    method: string;
    headers: Record<string, string>;
    body: string | null;
}

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
            headers =
                init?.headers instanceof Headers
                    ? Object.fromEntries(init.headers.entries())
                    : ((init?.headers as Record<string, string>) ?? {});
            body = typeof init?.body === "string" ? init.body : null;
        }
        const call = { url, method, headers, body };
        calls.push(call);
        return responder(call);
    };
    return { fetch: impl as typeof fetch, calls };
}

describe("makeExecutionClient — auth", () => {
    it("sends Authorization: Bearer <token>", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        await exec.markSuccess({ end_date: new Date().toISOString() });
        const authHeader = calls[0]!.headers["authorization"] ?? calls[0]!.headers["Authorization"];
        expect(authHeader).toBe(`Bearer ${TOKEN}`);
    });

    it("sends Airflow-API-Version header", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        await exec.markSuccess({ end_date: new Date().toISOString() });
        const versionHeader =
            calls[0]!.headers["airflow-api-version"] ?? calls[0]!.headers["Airflow-API-Version"];
        expect(versionHeader).toBe("2025-08-10");
    });

    it("respects custom apiVersion", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
            apiVersion: "2026-01-01",
        });
        await exec.markSuccess({ end_date: new Date().toISOString() });
        const versionHeader =
            calls[0]!.headers["airflow-api-version"] ?? calls[0]!.headers["Airflow-API-Version"];
        expect(versionHeader).toBe("2026-01-01");
    });

    it("exposes taskInstanceId", () => {
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
        });
        expect(exec.taskInstanceId).toBe(TI_ID);
    });
});

describe("makeExecutionClient — Refreshed-API-Token swap", () => {
    it("uses the refreshed token from the previous response on the next request", async () => {
        // First call (e.g. /run): server hands back a fresh execution-scoped token.
        // Second call (e.g. /state): SDK should send the refreshed value, not the
        // original workload token.
        const REFRESHED = "execution.scoped.bearer";
        let callIndex = 0;
        const { fetch: fetchImpl, calls } = mockFetch(() => {
            callIndex += 1;
            const headers: Record<string, string> = callIndex === 1
                ? { "Refreshed-API-Token": REFRESHED }
                : {};
            return new Response("null", { status: 200, headers });
        });
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });

        await exec.enterRunning({ hostname: "h", unixname: "u", pid: 1, start_date: "2026-04-30T00:00:00Z" });
        await exec.markSuccess({ end_date: "2026-04-30T00:00:01Z" });

        const auth1 = calls[0]!.headers["authorization"] ?? calls[0]!.headers["Authorization"];
        const auth2 = calls[1]!.headers["authorization"] ?? calls[1]!.headers["Authorization"];
        expect(auth1).toBe(`Bearer ${TOKEN}`);
        expect(auth2).toBe(`Bearer ${REFRESHED}`);
    });

    it("keeps the existing token when the response has no Refreshed-API-Token header", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(
            () => new Response("null", { status: 200 }),
        );
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });

        await exec.enterRunning({ hostname: "h", unixname: "u", pid: 1, start_date: "2026-04-30T00:00:00Z" });
        await exec.markSuccess({ end_date: "2026-04-30T00:00:01Z" });

        const auth1 = calls[0]!.headers["authorization"] ?? calls[0]!.headers["Authorization"];
        const auth2 = calls[1]!.headers["authorization"] ?? calls[1]!.headers["Authorization"];
        expect(auth1).toBe(`Bearer ${TOKEN}`);
        expect(auth2).toBe(`Bearer ${TOKEN}`);
    });
});

describe("makeExecutionClient — state transitions", () => {
    it("enterRunning posts to /task-instances/{id}/run with state=running and user fields", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        await exec.enterRunning({
            hostname: "myhost",
            unixname: "me",
            pid: 12345,
            start_date: "2026-04-19T00:00:00Z",
        });
        expect(calls[0]!.url).toBe(`${BASE_URL}/execution/task-instances/${TI_ID}/run`);
        expect(calls[0]!.method).toBe("PATCH");
        const body = JSON.parse(calls[0]!.body as string);
        expect(body.state).toBe("running");
        expect(body.hostname).toBe("myhost");
        expect(body.pid).toBe(12345);
    });

    it("markSuccess posts state=success + empty outlet arrays", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        await exec.markSuccess({ end_date: "2026-04-19T00:00:05Z" });
        expect(calls[0]!.url).toBe(`${BASE_URL}/execution/task-instances/${TI_ID}/state`);
        const body = JSON.parse(calls[0]!.body as string);
        expect(body.state).toBe("success");
        expect(body.end_date).toBe("2026-04-19T00:00:05Z");
        expect(body.task_outlets).toEqual([]);
        expect(body.outlet_events).toEqual([]);
    });

    it("markFailed posts state=failed", async () => {
        const { fetch: fetchImpl, calls } = mockFetch(() => new Response("null", { status: 200 }));
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        await exec.markFailed({ end_date: "2026-04-19T00:00:05Z" });
        const body = JSON.parse(calls[0]!.body as string);
        expect(body.state).toBe("failed");
    });
});

describe("makeExecutionClient — error handling", () => {
    it("throws ExecutionApiError with isNotFound=true on 404", async () => {
        const { fetch: fetchImpl } = mockFetch(
            () => new Response("not found", { status: 404 }),
        );
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        try {
            await exec.markSuccess({ end_date: "2026-04-19T00:00:05Z" });
            expect.fail("should have thrown");
        } catch (err) {
            expect(err).toBeInstanceOf(ExecutionApiError);
            const e = err as ExecutionApiError;
            expect(e.isNotFound).toBe(true);
            expect(e.status).toBe(404);
        }
    });

    it("throws ExecutionApiError with isInvalidState=true on 409 'invalid_state'", async () => {
        const { fetch: fetchImpl } = mockFetch(
            () =>
                new Response(JSON.stringify({ detail: "invalid_state" }), { status: 409 }),
        );
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
        });
        try {
            await exec.markSuccess({ end_date: "2026-04-19T00:00:05Z" });
            expect.fail("should have thrown");
        } catch (err) {
            expect(err).toBeInstanceOf(ExecutionApiError);
            const e = err as ExecutionApiError;
            expect(e.isInvalidState).toBe(true);
        }
    });

    it("retries on 5xx (transient)", async () => {
        let calls = 0;
        const { fetch: fetchImpl } = mockFetch(() => {
            calls += 1;
            if (calls < 2) return new Response("server error", { status: 500 });
            return new Response("null", { status: 200 });
        });
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
            retry: { maxAttempts: 3, baseBackoffMs: 1 },
        });
        await exec.markSuccess({ end_date: "2026-04-19T00:00:05Z" });
        expect(calls).toBe(2);
    });

    it("does not retry on 404", async () => {
        let calls = 0;
        const { fetch: fetchImpl } = mockFetch(() => {
            calls += 1;
            return new Response("not found", { status: 404 });
        });
        const exec = makeExecutionClient({
            baseUrl: BASE_URL,
            taskInstanceId: TI_ID,
            token: TOKEN,
            fetchImpl,
            retry: { maxAttempts: 3, baseBackoffMs: 1 },
        });
        await expect(exec.markSuccess({ end_date: "2026-04-19T00:00:05Z" })).rejects.toBeInstanceOf(
            ExecutionApiError,
        );
        expect(calls).toBe(1);
    });
});
