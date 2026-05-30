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

// Shared retry helper for Edge + Execution clients. Exponential backoff
// (100ms × 3^attempt). Transient errors retry, permanent fail fast.
// TODO(pr2): jitter, per-attempt timeout, full state-transition retry budget.

export interface RetryConfig {
    maxAttempts?: number;
    baseBackoffMs?: number;
}

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BASE_BACKOFF_MS = 100;

/** Does this error warrant a retry? Falls through to false for non-typed errors. */
interface RetriableError {
    isTransient: boolean;
}

function isRetriable(err: unknown): err is RetriableError {
    return (
        typeof err === "object" &&
        err !== null &&
        "isTransient" in err &&
        typeof (err as RetriableError).isTransient === "boolean" &&
        (err as RetriableError).isTransient
    );
}

/** Invoke `fn` with retry on transient errors. Permanent errors fail fast.
 *  Honors the provided AbortSignal between attempts (aborts during sleep). */
export async function withRetry<T>(
    fn: () => Promise<T>,
    config: RetryConfig = {},
    signal?: AbortSignal,
): Promise<T> {
    const maxAttempts = config.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    const baseBackoffMs = config.baseBackoffMs ?? DEFAULT_BASE_BACKOFF_MS;
    let lastErr: unknown;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        if (signal?.aborted) throw signal.reason ?? new Error("aborted");
        try {
            return await fn();
        } catch (err) {
            if (!isRetriable(err)) throw err;
            lastErr = err;
        }
        const delayMs = baseBackoffMs * 3 ** attempt;
        await sleepAbortable(delayMs, signal);
    }
    throw lastErr;
}

export function sleepAbortable(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(signal.reason ?? new Error("aborted"));
            return;
        }
        const t = setTimeout(() => {
            if (signal) signal.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        const onAbort = () => {
            clearTimeout(t);
            reject(signal?.reason ?? new Error("aborted"));
        };
        signal?.addEventListener("abort", onAbort, { once: true });
    });
}
