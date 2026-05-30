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
import { EdgeApiError, ExecutionApiError, formatError } from "../src/errors.js";

describe("formatError", () => {
    it("returns stack trace for Error instances", () => {
        const err = new Error("boom");
        const out = formatError(err);
        expect(out).toContain("Error: boom");
    });

    it("returns the string as-is for string errors", () => {
        expect(formatError("naked string")).toBe("naked string");
    });

    it("JSON-stringifies object-shaped errors", () => {
        expect(formatError({ code: 42 })).toBe('{"code":42}');
    });

    it("falls back to String() for circular objects", () => {
        const obj: Record<string, unknown> = {};
        obj.self = obj;
        expect(formatError(obj)).toBe("[object Object]");
    });

    it("handles null/undefined", () => {
        expect(formatError(null)).toBe("null");
        expect(formatError(undefined)).toBe("undefined");
    });
});

describe("EdgeApiError", () => {
    it("builds a readable message with method, path, status, and truncated body", () => {
        const err = new EdgeApiError("GET", "/jobs/fetch/w", 500, "a".repeat(500));
        expect(err.message).toMatch(/Edge API GET \/jobs\/fetch\/w → 500/);
        // Body is truncated to 200 chars
        expect(err.message.length).toBeLessThan(300);
    });

    it("isTransient is true for 5xx", () => {
        expect(new EdgeApiError("GET", "/p", 500, "").isTransient).toBe(true);
        expect(new EdgeApiError("GET", "/p", 503, "").isTransient).toBe(true);
    });

    it("isTransient is true for status 0 (network)", () => {
        expect(new EdgeApiError("GET", "/p", 0, "ECONNREFUSED").isTransient).toBe(true);
    });

    it("isTransient is false for 4xx", () => {
        expect(new EdgeApiError("GET", "/p", 400, "").isTransient).toBe(false);
        expect(new EdgeApiError("GET", "/p", 404, "").isTransient).toBe(false);
        expect(new EdgeApiError("GET", "/p", 409, "").isTransient).toBe(false);
    });

    it("isNotFound is true only for 404", () => {
        expect(new EdgeApiError("GET", "/p", 404, "").isNotFound).toBe(true);
        expect(new EdgeApiError("GET", "/p", 400, "").isNotFound).toBe(false);
        expect(new EdgeApiError("GET", "/p", 500, "").isNotFound).toBe(false);
    });

    it("is an instanceof Error", () => {
        expect(new EdgeApiError("GET", "/p", 500, "")).toBeInstanceOf(Error);
    });

    it("preserves fields readably", () => {
        const err = new EdgeApiError("PATCH", "/worker/alpha", 409, "conflict");
        expect(err.method).toBe("PATCH");
        expect(err.path).toBe("/worker/alpha");
        expect(err.status).toBe(409);
        expect(err.body).toBe("conflict");
        expect(err.name).toBe("EdgeApiError");
    });
});

describe("ExecutionApiError", () => {
    it("isTransient mirrors EdgeApiError semantics", () => {
        expect(new ExecutionApiError("GET", "/p", 500, "").isTransient).toBe(true);
        expect(new ExecutionApiError("GET", "/p", 404, "").isTransient).toBe(false);
    });

    it("isInvalidState: 409 + body contains 'invalid_state'", () => {
        const err = new ExecutionApiError(
            "PATCH",
            "/task-instances/123/state",
            409,
            '{"detail":"invalid_state"}',
        );
        expect(err.isInvalidState).toBe(true);
    });

    it("isInvalidState is false for 409 without the marker", () => {
        const err = new ExecutionApiError("PATCH", "/p", 409, "some other 409");
        expect(err.isInvalidState).toBe(false);
    });

    it("isTokenExpired: 403 + 'Signature has expired'", () => {
        const err = new ExecutionApiError(
            "GET",
            "/variables/foo",
            403,
            "Signature has expired.",
        );
        expect(err.isTokenExpired).toBe(true);
    });

    it("isTokenExpired is false for non-403s or different body", () => {
        expect(
            new ExecutionApiError("GET", "/p", 403, "other reason").isTokenExpired,
        ).toBe(false);
        expect(
            new ExecutionApiError("GET", "/p", 401, "Signature has expired").isTokenExpired,
        ).toBe(false);
    });

    it("isNotFound", () => {
        expect(new ExecutionApiError("GET", "/p", 404, "").isNotFound).toBe(true);
        expect(new ExecutionApiError("GET", "/p", 500, "").isNotFound).toBe(false);
    });

    it("name is set correctly", () => {
        expect(new ExecutionApiError("GET", "/p", 500, "").name).toBe("ExecutionApiError");
    });
});
