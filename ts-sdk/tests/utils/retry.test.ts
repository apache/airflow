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
import { withRetry } from "../../src/utils/retry.js";

describe("withRetry", () => {
    it("returns on first success", async () => {
        let calls = 0;
        const result = await withRetry(async () => {
            calls += 1;
            return "ok";
        });
        expect(result).toBe("ok");
        expect(calls).toBe(1);
    });

    it("retries transient errors and eventually succeeds", async () => {
        let calls = 0;
        const result = await withRetry(
            async () => {
                calls += 1;
                if (calls < 3) {
                    throw { isTransient: true, message: "try again" };
                }
                return "ok";
            },
            { baseBackoffMs: 1 },
        );
        expect(result).toBe("ok");
        expect(calls).toBe(3);
    });

    it("fails fast on permanent errors (isTransient=false)", async () => {
        let calls = 0;
        await expect(
            withRetry(async () => {
                calls += 1;
                throw { isTransient: false, message: "permanent" };
            }),
        ).rejects.toEqual({ isTransient: false, message: "permanent" });
        expect(calls).toBe(1);
    });

    it("fails fast on errors without isTransient classification", async () => {
        let calls = 0;
        await expect(
            withRetry(async () => {
                calls += 1;
                throw new Error("naked error");
            }),
        ).rejects.toThrow("naked error");
        expect(calls).toBe(1);
    });

    it("throws the last transient error if all attempts fail", async () => {
        let calls = 0;
        await expect(
            withRetry(
                async () => {
                    calls += 1;
                    throw { isTransient: true, attempt: calls };
                },
                { maxAttempts: 3, baseBackoffMs: 1 },
            ),
        ).rejects.toMatchObject({ isTransient: true, attempt: 3 });
        expect(calls).toBe(3);
    });

    it("respects AbortSignal — fails before any attempt if already aborted", async () => {
        const ac = new AbortController();
        ac.abort(new Error("pre-abort"));
        await expect(
            withRetry(
                async () => {
                    throw new Error("should not run");
                },
                {},
                ac.signal,
            ),
        ).rejects.toThrow("pre-abort");
    });

    it("aborts during backoff sleep", async () => {
        const ac = new AbortController();
        let calls = 0;
        const start = Date.now();
        const promise = withRetry(
            async () => {
                calls += 1;
                throw { isTransient: true };
            },
            { maxAttempts: 10, baseBackoffMs: 1000 },
            ac.signal,
        );
        // Abort shortly after first attempt
        setTimeout(() => ac.abort(new Error("cancel")), 20);
        await expect(promise).rejects.toThrow("cancel");
        const elapsed = Date.now() - start;
        expect(elapsed).toBeLessThan(500); // didn't sleep full 1000ms
        expect(calls).toBe(1);
    });

    it("default maxAttempts = 3", async () => {
        let calls = 0;
        await expect(
            withRetry(
                async () => {
                    calls += 1;
                    throw { isTransient: true };
                },
                { baseBackoffMs: 1 },
            ),
        ).rejects.toBeDefined();
        expect(calls).toBe(3);
    });
});
