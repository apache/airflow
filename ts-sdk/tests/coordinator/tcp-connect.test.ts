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

import { describe, expect, it } from "vitest";
import { connectTcp, splitHostPort } from "../../src/coordinator/tcp-connect.js";

describe("splitHostPort", () => {
  it.each([
    // [input, host, port]
    ["127.0.0.1:8080", "127.0.0.1", "8080"],
    ["localhost:5432", "localhost", "5432"],
    // Splits on the LAST colon, so a bare IPv6 host survives intact.
    // (Result feeds Node's connect() as-is; the bracketed "[::1]"
    // form is intentionally not handled — Node doesn't strip
    // brackets, so it wouldn't connect anyway.)
    ["::1:8080", "::1", "8080"],
  ])("splits %s into host=%s port=%s", (addr, host, port) => {
    expect(splitHostPort(addr)).toEqual([host, port]);
  });

  it("throws with the exact host:port message when there is no colon", () => {
    expect(() => splitHostPort("localhost")).toThrow("Address must be host:port, got localhost");
  });

  it("throws on the empty string (no colon)", () => {
    expect(() => splitHostPort("")).toThrow(/Address must be host:port/);
  });
});

describe("connectTcp", () => {
  it("rejects (does not throw synchronously) on a malformed address", async () => {
    await expect(connectTcp("nocolon")).rejects.toThrow("Address must be host:port, got nocolon");
  });
});
