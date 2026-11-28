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

import { spawn } from "node:child_process";
import { createServer, type AddressInfo, type Server } from "node:net";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const CRASH_FIXTURE = fileURLToPath(new URL("fixtures/stdio-crash.ts", import.meta.url));

describe("bundle.serve()", () => {
  const servers: Server[] = [];
  afterEach(() => {
    for (const server of servers.splice(0)) server.close();
  });

  async function listen(): Promise<string> {
    const server = createServer((socket) => socket.resume());
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    return `127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  it("keeps all output of a task process that fails with more queued than a socket holds", async () => {
    const args = [`--comm=${await listen()}`, `--logs=${await listen()}`];
    // spawn gives the child a socketpair for each stream, like the supervisor does.
    const child = spawn(process.execPath, ["--import", "tsx", CRASH_FIXTURE, ...args]);
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    await new Promise((resolve) => child.on("close", resolve));

    expect(Buffer.concat(stdout).toString()).toBe("o".repeat(1 << 20) + "\n");
    expect(Buffer.concat(stderr).toString()).toContain("e".repeat(1 << 20) + "\n");
    expect(Buffer.concat(stderr).toString()).toContain("Error: final failure");
  });
});
