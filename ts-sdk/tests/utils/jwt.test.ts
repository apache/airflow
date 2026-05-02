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
import { createHmac } from "node:crypto";
import { signEdgeJwt, pathToEdgeMethod } from "../../src/utils/jwt.js";

function b64urlDecode(input: string): string {
    const pad = input.length % 4;
    const padded = pad ? input + "=".repeat(4 - pad) : input;
    const std = padded.replace(/-/g, "+").replace(/_/g, "/");
    return Buffer.from(std, "base64").toString("utf8");
}

describe("signEdgeJwt", () => {
    const SECRET = "test-secret-not-for-prod";
    const ISSUER = "test-issuer";

    it("produces a 3-part compact JWT", () => {
        const jwt = signEdgeJwt(SECRET, { method: "worker/alpha", issuer: ISSUER });
        expect(jwt.split(".").length).toBe(3);
    });

    it("sets the HS512 header alg", () => {
        const jwt = signEdgeJwt(SECRET, { method: "worker/alpha", issuer: ISSUER });
        const header = JSON.parse(b64urlDecode(jwt.split(".")[0]!)) as Record<string, unknown>;
        expect(header.alg).toBe("HS512");
        expect(header.typ).toBe("JWT");
    });

    it("embeds the method claim and standard fields", () => {
        const jwt = signEdgeJwt(SECRET, { method: "jobs/fetch/w", issuer: ISSUER, ttlSeconds: 60 });
        const payload = JSON.parse(b64urlDecode(jwt.split(".")[1]!)) as Record<string, unknown>;
        expect(payload.method).toBe("jobs/fetch/w");
        expect(payload.aud).toBe("api");
        expect(payload.iss).toBe(ISSUER);
        expect(typeof payload.iat).toBe("number");
        expect(typeof payload.exp).toBe("number");
        expect((payload.exp as number) - (payload.iat as number)).toBe(60);
    });

    it("signature verifies against the shared secret (HS512)", () => {
        const jwt = signEdgeJwt(SECRET, { method: "worker/alpha", issuer: ISSUER });
        const [h, p, sig] = jwt.split(".");
        const expected = createHmac("sha512", SECRET)
            .update(`${h}.${p}`)
            .digest("base64")
            .replace(/=+$/, "")
            .replace(/\+/g, "-")
            .replace(/\//g, "_");
        expect(sig).toBe(expected);
    });

    it("respects custom issuer and TTL", () => {
        const jwt = signEdgeJwt(SECRET, {
            method: "m",
            issuer: "my-worker",
            ttlSeconds: 5,
        });
        const payload = JSON.parse(b64urlDecode(jwt.split(".")[1]!)) as Record<string, unknown>;
        expect(payload.iss).toBe("my-worker");
        expect((payload.exp as number) - (payload.iat as number)).toBe(5);
    });
});

describe("pathToEdgeMethod", () => {
    it("strips the /edge_worker/v1/ prefix", () => {
        expect(pathToEdgeMethod("/edge_worker/v1/worker/alpha")).toBe("worker/alpha");
    });

    it("returns a decoded path (Spike 11 bug fix)", () => {
        // run_id with `:` and `+` is URL-encoded by fetch clients;
        // the server verifies the DECODED path, so we must decode before signing.
        const encoded = "/edge_worker/v1/jobs/state/dag_x/task_y/manual__2026-04-19T00%3A00%3A00%2B00%3A00/1/-1/success";
        const method = pathToEdgeMethod(encoded);
        expect(method).toContain("manual__2026-04-19T00:00:00+00:00");
        expect(method).not.toContain("%3A");
        expect(method).not.toContain("%2B");
    });

    it("handles paths already without the prefix", () => {
        expect(pathToEdgeMethod("/something-else")).toBe("/something-else");
    });

    it("handles paths that include but don't start with the prefix", () => {
        expect(pathToEdgeMethod("http://h/edge_worker/v1/a")).toBe("a");
    });
});
