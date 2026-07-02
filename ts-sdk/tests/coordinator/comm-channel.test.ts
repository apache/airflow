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

import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CommChannel, COORDINATOR_REQUEST_TIMEOUT_MS } from "../../src/coordinator/comm-channel.js";
import { encodeResponse } from "../../src/coordinator/frames.js";

class FakeSocket extends EventEmitter {
  writeCallback: ((err?: Error) => void) | undefined;
  writeError: Error | undefined;
  write = vi.fn((_buf: Buffer, cb?: (err?: Error) => void) => {
    if (this.writeError) throw this.writeError;
    this.writeCallback = cb;
    return true;
  });
  end = vi.fn((cb?: () => void) => cb?.());
  destroy = vi.fn();
}

function createChannel(sock: FakeSocket): CommChannel {
  const ctor = CommChannel as unknown as new (sock: FakeSocket, logs: null) => CommChannel;
  return new ctor(sock, null);
}

describe("CommChannel", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("destroys the socket when a response write times out", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const send = channel.sendResponse(7, { type: "TaskState", state: "failed" }, undefined, {
      timeoutMs: 10,
    });
    const assertion = expect(send).rejects.toThrow("Timed out sending response after 10 ms");
    await vi.advanceTimersByTimeAsync(10);

    await assertion;
    expect(sock.destroy).toHaveBeenCalledWith(expect.any(Error));
  });

  it("clears the response timeout when the write completes", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const send = channel.sendResponse(7, { type: "TaskState", state: "failed" }, undefined, {
      timeoutMs: 10,
    });
    sock.writeCallback?.();

    await expect(send).resolves.toBeUndefined();
    await vi.advanceTimersByTimeAsync(10);
    expect(sock.destroy).not.toHaveBeenCalled();
  });

  it("clears the response timeout when the write throws", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);
    sock.writeError = new Error("write failed");

    const send = channel.sendResponse(7, { type: "TaskState", state: "failed" }, undefined, {
      timeoutMs: 10,
    });

    await expect(send).rejects.toThrow("write failed");
    await vi.advanceTimersByTimeAsync(10);
    expect(sock.destroy).not.toHaveBeenCalled();
  });

  it("rejects a request when its response times out", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const request = channel.request({ type: "GetVariable", key: "greeting" }, { timeoutMs: 10 });
    const assertion = expect(request).rejects.toThrow(
      "Timed out waiting for GetVariable response after 10 ms",
    );
    await vi.advanceTimersByTimeAsync(10);

    await assertion;
  });

  it("uses the default request timeout when no override is provided", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const request = channel.request({ type: "GetVariable", key: "greeting" });
    const rejection = vi.fn();
    request.catch(rejection);
    const assertion = expect(request).rejects.toThrow(
      `Timed out waiting for GetVariable response after ${COORDINATOR_REQUEST_TIMEOUT_MS} ms`,
    );

    await vi.advanceTimersByTimeAsync(COORDINATOR_REQUEST_TIMEOUT_MS - 1);
    expect(rejection).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);

    await assertion;
    expect(rejection).toHaveBeenCalledOnce();
  });

  it("clears the request timeout when the response arrives", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const request = channel.request({ type: "GetVariable", key: "greeting" }, { timeoutMs: 10 });
    sock.emit("data", encodeResponse(0, { type: "VariableResult", value: "hello" }));

    await expect(request).resolves.toMatchObject({
      body: { type: "VariableResult", value: "hello" },
    });
    await vi.advanceTimersByTimeAsync(10);
  });

  it("clears the request timeout when the write fails", async () => {
    vi.useFakeTimers();
    const sock = new FakeSocket();
    const channel = createChannel(sock);

    const request = channel.request({ type: "GetVariable", key: "greeting" }, { timeoutMs: 10 });
    sock.writeCallback?.(new Error("write failed"));

    await expect(request).rejects.toThrow("write failed");
    await vi.advanceTimersByTimeAsync(10);
  });

  it("does not log clean close events", async () => {
    const logs = { debug: vi.fn(), warning: vi.fn() };
    const sock = new FakeSocket();
    const ctor = CommChannel as unknown as new (sock: FakeSocket, logs: unknown) => CommChannel;
    const channel = new ctor(sock, logs);

    sock.emit("data", encodeResponse(0, { type: "StartupDetails" }));
    await channel.close();
    sock.emit("close");

    expect(logs.debug).not.toHaveBeenCalledWith("Comm channel closed", expect.anything());
    expect(logs.warning).not.toHaveBeenCalled();
  });
});
