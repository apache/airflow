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
    sock.on("data", (chunk: Buffer) => received.push(chunk));
    sock.on("close", () => resolveSockClosed());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as net.AddressInfo).port;
  return { server, port, received, sockClosed };
}

async function connectChannel(addr: string, name?: string): Promise<LogChannel> {
  const channel =
    name === undefined ? LogChannel.createBuffered() : LogChannel.createBuffered(name);
  await channel.connect(addr);
  return channel;
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
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", undefined);
    vi.stubEnv("AIRFLOW__LOGGING__NAMESPACE_LEVELS", "");
    fx = await makeServer();
  });

  afterEach(async () => {
    fx.server.close();
    vi.unstubAllEnvs();
  });

  it.each([
    [undefined, ["info", "warning", "error"]],
    ["", ["info", "warning", "error"]],
    // Invalid values fall back to INFO and report themselves as the first record.
    ["VERBOSE", ["error", "info", "warning", "error"]],
    ["FATAL", ["error", "info", "warning", "error"]],
    ["INFO", ["info", "warning", "error"]],
    [" debug ", ["debug", "info", "warning", "error"]],
    ["WARNING", ["warning", "error"]],
    ["WARN", ["warning", "error"]],
    ["ERROR", ["error"]],
    ["EXCEPTION", ["error"]],
    ["CRITICAL", []],
    ["NOTSET", ["debug", "info", "warning", "error"]],
  ] as const)("filters socket records with logging_level=%s", async (level, expected) => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", level);
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.debug("debug");
    root.info("info");
    root.warning("warning");
    root.error("error");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received).map((record) => record["level"])).toEqual(expected);
  });

  it("uses the nearest dotted namespace override and the last duplicate entry", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR");
    vi.stubEnv(
      "AIRFLOW__LOGGING__NAMESPACE_LEVELS",
      " ts-sdk=WARNING, ts-sdk.comm=ERROR ts-sdk.comm=INFO,ts-sdk.comm.deep=DEBUG ",
    );
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.info("filtered at root");
    root.warning("root warning");
    root.child("comm").debug("filtered at comm");
    root.child("comm").info("comm info");
    root.child("comm").child("deep").child("child").debug("deep debug");
    root.child("commander").info("partial namespace must not match");
    root.child("commander").warning("inherited root warning");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received).map((record) => record["event"])).toEqual([
      "[ts-sdk] root warning",
      "[ts-sdk.comm] comm info",
      "[ts-sdk.comm.deep.child] deep debug",
      "[ts-sdk.commander] inherited root warning",
    ]);
  });

  it("keeps valid namespace overrides and reports invalid entries once", async () => {
    vi.stubEnv(
      "AIRFLOW__LOGGING__NAMESPACE_LEVELS",
      "broken,=DEBUG,ts-sdk.comm=VERBOSE,ts-sdk.client=debug",
    );
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.child("comm").debug("invalid override falls back to INFO");
    root.child("client").debug("valid override");
    root.child("client").debug("second record");
    await root.close();
    await fx.sockClosed;
    const records = readRecords(fx.received);
    expect(records.filter((record) => record["level"] === "error").map((r) => r["entry"])).toEqual([
      "broken",
      "=DEBUG",
      "ts-sdk.comm=VERBOSE",
    ]);
    expect(records.filter((record) => record["level"] === "debug").map((r) => r["event"])).toEqual([
      "[ts-sdk.client] valid override",
      "[ts-sdk.client] second record",
    ]);
  });

  it("filters before buffering and shares the initial configuration with later children", async () => {
    const root = LogChannel.createBuffered();
    root.debug("filtered before connect");
    root.info("buffered info");
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG");
    vi.stubEnv("AIRFLOW__LOGGING__NAMESPACE_LEVELS", "ts-sdk.comm=DEBUG");
    root.child("comm").debug("filtered despite later environment change");
    await root.connect(`127.0.0.1:${fx.port}`);
    root.child("comm").info("connected info");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received).map((record) => record["event"])).toEqual([
      "[ts-sdk] buffered info",
      "[ts-sdk.comm] connected info",
    ]);
  });

  it("filters records on stderr fallback as well as the socket", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = LogChannel.createBuffered();
    try {
      root.debug("filtered before connect");
      root.info("buffered info");
      await root.close();
      expect(write.mock.calls).toHaveLength(1);
      expect(JSON.parse(String(write.mock.calls[0]?.[0]))).toMatchObject({
        event: "[ts-sdk] buffered info",
        level: "info",
      });
    } finally {
      write.mockRestore();
    }
  });

  it("reports an invalid logging_level once", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "TRACE");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.debug("filtered at INFO");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received)).toMatchObject([
      {
        level: "error",
        event: "[ts-sdk] Ignoring invalid logging_level; using INFO",
        logging_level: "TRACE",
      },
    ]);
  });

  it("keeps the method's level when args carry a level field", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.info("numeric level", { level: 3 });
    root.debug("level escalation", { level: "error" });
    root.error("level demotion", { level: "debug" });
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received)).toMatchObject([
      { event: "[ts-sdk] level demotion", level: "error" },
    ]);
  });

  it("treats a level outside LogLevel as INFO instead of bypassing the filter", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.send({ event: "unknown", level: "trace" as never });
    root.send({ event: "non-string", level: 7 as never });
    root.warning("kept");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received).map((record) => record["event"])).toEqual(["[ts-sdk] kept"]);
  });

  it("filters by the channel name even when the record overrides logger", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__NAMESPACE_LEVELS", "ts-sdk.client=DEBUG");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.child("comm").debug("override does not unlock DEBUG", { logger: "ts-sdk.client" });
    root.child("comm").info("non-string logger", { logger: 123 });
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received)).toMatchObject([
      { event: "[ts-sdk.comm] non-string logger", logger: 123 },
    ]);
  });

  it("applies namespace overrides to the invalid-entry report itself", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__NAMESPACE_LEVELS", "broken ts-sdk=CRITICAL");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.error("filtered at CRITICAL");
    await root.close();
    await fx.sockClosed;
    expect(readRecords(fx.received)).toEqual([]);
  });

  it("defaults logger name to 'ts-sdk' and auto-stamps timestamp", async () => {
    const ch = await connectChannel(`127.0.0.1:${fx.port}`);
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
    const ch = await connectChannel(`127.0.0.1:${fx.port}`, "ts-sdk.runtime");
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
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG");
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
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
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
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
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    root.child("child");
    const sock = (root as unknown as { shared: { sock: net.Socket } }).shared.sock;

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

  it("buffers records emitted before connect and flushes them in order on connect", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG");
    const root = LogChannel.createBuffered();
    const child = root.child("runtime");
    child.debug("connecting", { attempt: 1 });
    root.info("still starting");

    await root.connect(`127.0.0.1:${fx.port}`);
    root.info("connected");
    await root.close();
    await fx.sockClosed;

    const records = readRecords(fx.received);
    expect(records.map((r) => r["event"])).toEqual([
      "[ts-sdk.runtime] connecting",
      "[ts-sdk] still starting",
      "[ts-sdk] connected",
    ]);
    expect(records[0]).toMatchObject({ attempt: 1, level: "debug" });
  });

  it("rejects connect() from a child and a second connect() on the root", async () => {
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    try {
      await expect(root.child("runtime").connect(`127.0.0.1:${fx.port}`)).rejects.toThrow(
        "root-only",
      );
      await expect(root.connect(`127.0.0.1:${fx.port}`)).rejects.toThrow("more than once");
    } finally {
      await root.close();
      await fx.sockClosed;
    }
  });

  it("close() before connect writes buffered records to stderr", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = LogChannel.createBuffered();
    try {
      root.info("never made it");
      await root.close();
      const line = write.mock.calls.find((c) => String(c[0]).includes("never made it"))?.[0];
      expect(String(line)).toContain('"event":"[ts-sdk] never made it"');
    } finally {
      write.mockRestore();
    }
  });

  it("drops records sent after close instead of writing to the ended socket", async () => {
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    const child = root.child("comm");
    root.info("before close");
    await root.close();

    root.info("after close");
    child.debug("after close via child");

    await fx.sockClosed;
    const records = readRecords(fx.received);
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({ event: "[ts-sdk] before close" });
  });

  it("writes only the error message when error and close fire together", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    const sock = (root as unknown as { shared: { sock: net.Socket } }).shared.sock;

    try {
      sock.emit("error", Object.assign(new Error("write EPIPE"), { code: "EPIPE" }));
      sock.destroy();
      await fx.sockClosed;
      expect(write).toHaveBeenCalledTimes(1);
      expect(write.mock.calls[0]?.[0]).toBe("[ts-sdk] log socket error: write EPIPE\n");
    } finally {
      write.mockRestore();
      await root.close();
    }
  });

  it("falls back to stderr when the socket is no longer writable", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    const sock = (root as unknown as { shared: { sock: net.Socket } }).shared.sock;

    try {
      sock.end();
      root.info("peer died");
      const line = write.mock.calls.find((c) => String(c[0]).includes("peer died"))?.[0];
      expect(String(line)).toContain('"event":"[ts-sdk] peer died"');
    } finally {
      write.mockRestore();
      await root.close();
      await fx.sockClosed;
    }
  });

  it("close() resolves via timeout when the flush never completes", async () => {
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    const sock = (root as unknown as { shared: { sock: net.Socket } }).shared.sock;
    vi.spyOn(sock, "end").mockImplementation(() => sock);
    const destroy = vi.spyOn(sock, "destroy");

    vi.useFakeTimers();
    try {
      const closed = root.close();
      await vi.advanceTimersByTimeAsync(3_000);
      await closed;
      expect(destroy).toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports close-time socket errors", async () => {
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await connectChannel(`127.0.0.1:${fx.port}`);
    const sock = (root as unknown as { shared: { sock: net.Socket } }).shared.sock;

    try {
      sock.emit("error", Object.assign(new Error("write EPIPE"), { code: "EPIPE" }));
      expect(write).toHaveBeenCalledTimes(1);
      expect(write.mock.calls[0]?.[0]).toBe("[ts-sdk] log socket error: write EPIPE\n");
    } finally {
      write.mockRestore();
      await root.close();
      await fx.sockClosed;
    }
  });

  it("warns once and falls back to stderr after an unexpected disconnect", async () => {
    vi.stubEnv("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG");
    const serverSocks: net.Socket[] = [];
    const received: Buffer[] = [];
    const server = net.createServer((sock) => {
      serverSocks.push(sock);
      sock.on("data", (chunk: Buffer) => received.push(chunk));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as net.AddressInfo).port;

    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await connectChannel(`127.0.0.1:${port}`);
    const child = root.child("comm");
    const shared = (root as unknown as { shared: { connected: boolean } }).shared;
    try {
      root.info("before disconnect");
      await vi.waitFor(() => expect(readRecords(received)).toHaveLength(1));

      serverSocks[0]!.destroy();
      await vi.waitFor(() => expect(shared.connected).toBe(false));
      expect(write).toHaveBeenCalledWith(
        "[ts-sdk] log socket closed unexpectedly; further logs go to stderr\n",
      );

      root.info("while disconnected");
      child.debug("child while disconnected");
      const stderrRecords = write.mock.calls
        .map((call) => String(call[0]))
        .filter((line) => line.startsWith("{"))
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(stderrRecords.map((r) => r["event"])).toEqual([
        "[ts-sdk] while disconnected",
        "[ts-sdk.comm] child while disconnected",
      ]);
      expect(readRecords(received)).toHaveLength(1);
    } finally {
      write.mockRestore();
      await root.close();
      server.close();
    }
  });

  it("close() while disconnected resolves and drops later records", async () => {
    const serverSocks: net.Socket[] = [];
    const server = net.createServer((sock) => serverSocks.push(sock));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as net.AddressInfo).port;

    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const root = await connectChannel(`127.0.0.1:${port}`);
    try {
      await vi.waitFor(() => expect(serverSocks).toHaveLength(1));
      serverSocks[0]!.destroy();
      const shared = (root as unknown as { shared: { connected: boolean } }).shared;
      await vi.waitFor(() => expect(shared.connected).toBe(false));

      await root.close();
      write.mockClear();
      root.info("dropped after close");
      expect(write).not.toHaveBeenCalled();
    } finally {
      write.mockRestore();
      server.close();
    }
  });
});
