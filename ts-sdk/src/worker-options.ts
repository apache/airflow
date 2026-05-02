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

// Worker option resolution: env-var fallbacks, validation, default merging.

import { hostname } from "node:os";
import { randomBytes } from "node:crypto";
import type { StartWorkerOptions } from "./types.js";

export const DEFAULTS = {
    pollIntervalInMs: 5_000,
    heartbeatIntervalInMs: 30_000,
    heartbeatFailureThreshold: 10,
} as const;

export interface ResolvedWorkerOptions {
    baseUrl: string;
    secret: string;
    workerName: string;
    queues: string[];
    pollIntervalInMs: number;
    heartbeatIntervalInMs: number;
    heartbeatFailureThreshold: number;
    jwtIssuer: string;
}

export function resolveWorkerOptions(opts: StartWorkerOptions): ResolvedWorkerOptions {
    // Normalize to the host root — SDK builds its own paths.
    const rawBaseUrl = opts.baseUrl ?? process.env.AIRFLOW__EDGE__API_URL;
    if (!rawBaseUrl) {
        throw new Error("startWorker: baseUrl is required (pass in options or set AIRFLOW__EDGE__API_URL)");
    }
    // Strip an optional `/edge_worker/v1[/...]` suffix and any trailing slash,
    // leaving the host root that the SDK appends its own paths onto. Uses
    // indexOf/slice rather than regex to keep ReDoS analyzers happy.
    const edgeIdx = rawBaseUrl.indexOf("/edge_worker/v1");
    const root = edgeIdx >= 0 ? rawBaseUrl.slice(0, edgeIdx) : rawBaseUrl;
    const baseUrl = root.endsWith("/") ? root.slice(0, -1) : root;

    const secret = opts.secret ?? process.env.AIRFLOW__API_AUTH__JWT_SECRET;
    if (!secret) {
        throw new Error("startWorker: secret is required (pass in options or set AIRFLOW__API_AUTH__JWT_SECRET)");
    }

    if (!Array.isArray(opts.queues) || opts.queues.length === 0) {
        throw new Error("startWorker: queues must be a non-empty string[]");
    }

    const workerName = opts.workerName ?? generateWorkerName();

    return {
        baseUrl,
        secret,
        workerName,
        queues: opts.queues,
        pollIntervalInMs: opts.pollIntervalInMs ?? DEFAULTS.pollIntervalInMs,
        heartbeatIntervalInMs: opts.heartbeatIntervalInMs ?? DEFAULTS.heartbeatIntervalInMs,
        heartbeatFailureThreshold: opts.heartbeatFailureThreshold ?? DEFAULTS.heartbeatFailureThreshold,
        jwtIssuer: opts.jwtIssuer ?? process.env.AIRFLOW__API_AUTH__JWT_ISSUER ?? "airflow",
    };
}

function generateWorkerName(): string {
    const suffix = randomBytes(4).toString("hex");
    return `${hostname()}-${process.pid}-${suffix}`;
}
