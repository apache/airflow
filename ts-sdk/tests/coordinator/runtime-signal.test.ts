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
import { ABORT_GRACE_PERIOD_MS, createRuntimeAbort } from "../../src/coordinator/runtime.js";

describe("coordinator runtime signal handling", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("aborts on SIGTERM and force-exits when the grace period expires", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const exitProcess = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const runtimeAbort = createRuntimeAbort(null, {
      signalSource: events,
      exitProcess,
    });

    events.emit("SIGTERM", "SIGTERM");

    expect(runtimeAbort.signal.aborted).toBe(true);
    expect(exitProcess).not.toHaveBeenCalled();
    expect(() => vi.advanceTimersByTime(ABORT_GRACE_PERIOD_MS)).toThrow("exit 1");
    expect(exitProcess).toHaveBeenCalledWith(1);
    runtimeAbort.dispose();
  });

  it("uses the registered signal name when the event does not pass a payload", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const exitProcess = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const runtimeAbort = createRuntimeAbort(null, {
      signalSource: events,
      exitProcess,
    });

    events.emit("SIGTERM");

    expect(runtimeAbort.signal.aborted).toBe(true);
    expect((runtimeAbort.signal.reason as Error).message).toBe("Task aborted by SIGTERM");
    runtimeAbort.dispose();
  });

  it("removes signal listeners and clears the force-exit timer on dispose", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const exitProcess = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const runtimeAbort = createRuntimeAbort(null, {
      signalSource: events,
      exitProcess,
    });

    events.emit("SIGINT", "SIGINT");
    runtimeAbort.dispose();
    vi.advanceTimersByTime(ABORT_GRACE_PERIOD_MS);

    expect(exitProcess).not.toHaveBeenCalled();
    expect(events.listenerCount("SIGINT")).toBe(0);
    expect(events.listenerCount("SIGTERM")).toBe(0);
  });
});
