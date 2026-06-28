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
import {
  COORDINATOR_SIGNAL_GRACE_PERIOD_MS,
  createCoordinatorCancellation,
} from "../../src/coordinator/runtime.js";

describe("coordinator runtime signal handling", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("aborts on SIGTERM and force-exits when the grace period expires", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const forceExit = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const cancellation = createCoordinatorCancellation(null, {
      processEvents: events,
      forceExit,
    });

    events.emit("SIGTERM", "SIGTERM");

    expect(cancellation.signal.aborted).toBe(true);
    expect(forceExit).not.toHaveBeenCalled();
    expect(() => vi.advanceTimersByTime(COORDINATOR_SIGNAL_GRACE_PERIOD_MS)).toThrow("exit 1");
    expect(forceExit).toHaveBeenCalledWith(1);
    cancellation.dispose();
  });

  it("uses the registered signal name when the event does not pass a payload", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const forceExit = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const cancellation = createCoordinatorCancellation(null, {
      processEvents: events,
      forceExit,
    });

    events.emit("SIGTERM");

    expect(cancellation.signal.aborted).toBe(true);
    expect((cancellation.signal.reason as Error).message).toBe("Task cancelled by SIGTERM");
    cancellation.dispose();
  });

  it("removes signal listeners and clears the force-exit timer on dispose", () => {
    vi.useFakeTimers();
    const events = new EventEmitter();
    const forceExit = vi.fn((code: number): never => {
      throw new Error(`exit ${code}`);
    });
    const cancellation = createCoordinatorCancellation(null, {
      processEvents: events,
      forceExit,
    });

    events.emit("SIGINT", "SIGINT");
    cancellation.dispose();
    vi.advanceTimersByTime(COORDINATOR_SIGNAL_GRACE_PERIOD_MS);

    expect(forceExit).not.toHaveBeenCalled();
    expect(events.listenerCount("SIGINT")).toBe(0);
    expect(events.listenerCount("SIGTERM")).toBe(0);
  });
});
