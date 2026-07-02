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

import * as net from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LogChannel } from "../../src/coordinator/log-channel.js";

interface Fixture {
  server: net.Server;
  port: number;
  received: Buffer[];
  sockClosed: Promise<void>;
}

async function makeServer(): Promise<Fixture> {
  const received: Buffer[] = [];
  let resolveSockClosed: () => void;
  const sockClosed = new Promise<void>((r) => {
    resolveSockClosed = r;
  });
  const server = net.createServer((sock) => {
    sock.on("data", (chunk) => received.push(chunk));
    sock.on("close", () => resolveSockClosed());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as net.AddressInfo).port;
  return { server, port, received, sockClosed };
}

function readRecords(received: Buffer[]): Record<string, unknown>[] {
  return Buffer.concat(received)
    .toString("utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

describe("LogChannel", () => {
  let fx: Fixture;

  beforeEach(async () => {
    fx = await makeServer();
  });

  afterEach(async () => {
    fx.server.close();
  });

  it("defaults logger name to 'ts-sdk' and auto-stamps timestamp", async () => {
    const ch = await LogChannel.connect(`127.0.0.1:${fx.port}`);
    ch.info("hello");
    await ch.close();
    await fx.sockClosed;
    const records = readRecords(fx.received);
    expect(records).toHaveLength(1);
    const record = records[0]!;
    // Logger name kept as a JSON field AND prepended to the event so
    // it surfaces in the Airflow UI text renderer.
    expect(record).toMatchObject({
      event: "[ts-sdk] hello",
      level: "info",
      logger: "ts-sdk",
    });
    expect(typeof record["timestamp"]).toBe("string");
    expect(new Date(record["timestamp"] as string).toString()).not.toBe("Invalid Date");
  });

  it("accepts a custom root name", async () => {
    const ch = await LogChannel.connect(`127.0.0.1:${fx.port}`, "ts-sdk.runtime");
    ch.warning("started");
    await ch.close();
    await fx.sockClosed;
    const records = readRecords(fx.received);
    expect(records).toHaveLength(1);
    const record = records[0]!;
    expect(record).toMatchObject({
      event: "[ts-sdk.runtime] started",
      level: "warning",
      logger: "ts-sdk.runtime",
    });
  });

  it("child() creates a hierarchical sibling sharing the socket", async () => {
    const root = await LogChannel.connect(`127.0.0.1:${fx.port}`);
    const comm = root.child("comm");
    const client = root.child("client");
    expect(comm.loggerName).toBe("ts-sdk.comm");
    expect(client.loggerName).toBe("ts-sdk.client");

    root.info("a");
    comm.debug("b");
    client.error("c");
    await root.close();
    await fx.sockClosed;

    const records = readRecords(fx.received);
    expect(records.map((r) => r["logger"])).toEqual(["ts-sdk", "ts-sdk.comm", "ts-sdk.client"]);
    expect(records.map((r) => r["level"])).toEqual(["info", "debug", "error"]);
  });

  it("children must not close the shared socket", async () => {
    const root = await LogChannel.connect(`127.0.0.1:${fx.port}`);
    const child = root.child("comm");
    // Child.close() is a no-op. Root.close() ends the socket.
    await child.close();
    // The socket should still be open — write through root to prove it.
    root.info("still alive");
    await root.close();
    await fx.sockClosed;
    const records = readRecords(fx.received);
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      event: "[ts-sdk] still alive",
      logger: "ts-sdk",
    });
  });

  it("handles post-connect socket errors on the root channel", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await LogChannel.connect(`127.0.0.1:${fx.port}`);
    root.child("child");
    const sock = (root as unknown as { sock: net.Socket }).sock;

    try {
      sock.emit("error", new Error("boom"));
      expect(write).toHaveBeenCalledTimes(1);
      expect(write.mock.calls[0]?.[0]).toBe("[ts-sdk] log socket error: boom\n");
    } finally {
      write.mockRestore();
      await root.close();
      await fx.sockClosed;
    }
  });
});
