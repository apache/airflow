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

// JWT primitives for the Edge API. Signed per request with the shared
// secret; short TTL; `method` claim is the path minus `/edge_worker/v1/`.
//
// TODO(pr2): add Execution-API token refresh (self-sign + decode + near-expiry
//            check). Server-issued tokens come in `command.token` and have a
//            10-min TTL — long tasks need to refresh before the token ages out.

import { createHmac } from "node:crypto";

function b64urlEncode(input: Buffer | string): string {
    // Node's built-in base64url encoding strips padding and uses URL-safe
    // characters — no manual char replacement needed.
    return Buffer.from(input).toString("base64url");
}

interface EdgeJwtOptions {
    /** Request path with `/edge_worker/v1/` prefix stripped. Must match the
     *  server-side `method_path` computed in
     *  `providers/edge3/worker_api/auth.py` from `request.url.path`. */
    method: string;
    /** JWT `iss` claim. Must match the server's `[api_auth] jwt_issuer`
     *  config. Required: the resolved value comes from
     *  `worker-options.resolveWorkerOptions`, which applies the project-wide
     *  default ("airflow"). */
    issuer: string;
    /** Time-to-live in seconds. Default 30 (Python SDK default). Go SDK uses 5. */
    ttlSeconds?: number;
}

/** Sign an HS512 JWT for an Edge API request. The returned token goes in the
 *  Authorization header AS-IS — no `Bearer ` prefix (FastAPI's raw Header
 *  dependency rejects it). Contrast: Execution API wants `Bearer <token>`. */
export function signEdgeJwt(secret: string, opts: EdgeJwtOptions): string {
    const now = Math.floor(Date.now() / 1000);
    const ttl = opts.ttlSeconds ?? 30;
    const header = { alg: "HS512", typ: "JWT" } as const;
    const payload = {
        method: opts.method,
        iss: opts.issuer,
        aud: "api",
        iat: now,
        nbf: now,
        exp: now + ttl,
    };
    return signHs512(secret, header, payload);
}

/** Derive the JWT `method` claim from a URL path. The server matches the
 *  claim against the DECODED path with `/edge_worker/v1/` stripped — encoded
 *  segments (e.g. `:` → `%3A`) must be decoded first or the JWT check 403s. */
export function pathToEdgeMethod(path: string): string {
    const PREFIX = "/edge_worker/v1/";
    const decoded = decodeURIComponent(path);
    const idx = decoded.indexOf(PREFIX);
    return idx >= 0 ? decoded.slice(idx + PREFIX.length) : decoded;
}

function signHs512(
    secret: string,
    header: Record<string, unknown>,
    payload: Record<string, unknown>,
): string {
    const h = b64urlEncode(JSON.stringify(header));
    const p = b64urlEncode(JSON.stringify(payload));
    const signingInput = `${h}.${p}`;
    const signature = createHmac("sha512", secret).update(signingInput).digest();
    return `${signingInput}.${b64urlEncode(signature)}`;
}
