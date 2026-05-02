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

// Typed error classes for the two Airflow APIs. Distinguish transient
// (5xx, network) from permanent (4xx) so retry logic can fail fast.

/** Coerce an `unknown` catch-binding into a readable log string. JSON.stringify
 *  returns undefined for undefined and throws on circular refs, so the explicit
 *  branches keep the return type honestly `string`. */
export function formatError(err: unknown): string {
    if (err instanceof Error) return err.stack ?? `${err.name}: ${err.message}`;
    if (typeof err === "string") return err;
    if (err === undefined) return "undefined";
    try {
        return JSON.stringify(err) ?? String(err);
    } catch {
        return String(err);
    }
}

/** Error from the Edge Worker API (/edge_worker/v1/*). */
export class EdgeApiError extends Error {
    readonly method: string;
    readonly path: string;
    readonly status: number;
    readonly body: string;

    constructor(method: string, path: string, status: number, body: string) {
        super(`Edge API ${method} ${path} → ${status}: ${body.slice(0, 200)}`);
        this.name = "EdgeApiError";
        this.method = method;
        this.path = path;
        this.status = status;
        this.body = body;
    }

    /** Transient errors (5xx, 0/unknown, network) should be retried. */
    get isTransient(): boolean {
        return this.status >= 500 || this.status === 0;
    }

    /** 404 — the requested resource does not exist. */
    get isNotFound(): boolean {
        return this.status === 404;
    }
}

/** Error from the Execution API (/execution/*). */
export class ExecutionApiError extends Error {
    readonly method: string;
    readonly path: string;
    readonly status: number;
    readonly body: string;

    constructor(method: string, path: string, status: number, body: string) {
        super(`Execution API ${method} ${path} → ${status}: ${body.slice(0, 200)}`);
        this.name = "ExecutionApiError";
        this.method = method;
        this.path = path;
        this.status = status;
        this.body = body;
    }

    get isTransient(): boolean {
        return this.status >= 500 || this.status === 0;
    }

    /** 409 "invalid_state" — TI is already terminal; our transition is a no-op. */
    get isInvalidState(): boolean {
        return this.status === 409 && this.body.includes("invalid_state");
    }

    // TODO(pr2): wire token refresh into client (server-driven via response header + self-sign fallback).
    /** 403 with "Signature has expired" — the per-task JWT (10-min TTL) aged out. */
    get isTokenExpired(): boolean {
        return this.status === 403 && this.body.includes("Signature has expired");
    }

    /** 404 — the requested resource does not exist. */
    get isNotFound(): boolean {
        return this.status === 404;
    }
}
