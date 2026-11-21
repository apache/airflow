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

import { afterEach, describe, expect, it, vi } from "vitest";
import { setTimeout as sleep } from "node:timers/promises";
import {
  createExecutionTimeout,
  executionTimeoutOf,
} from "../../src/coordinator/execution-timeout.js";
import { TaskTimeoutError } from "../../src/sdk/task.js";

/** A task that waits on `signal` and settles the way `onAbort` says. */
function untilAborted<T>(signal: AbortSignal, onAbort: () => T): Promise<T> {
  return new Promise((resolve, reject) => {
    signal.addEventListener("abort", () => {
      try {
        resolve(onAbort());
      } catch (err) {
        reject(err as Error);
      }
    });
  });
}

function timeoutOf(seconds: number, stopWaiting: AbortSignal = new AbortController().signal) {
  return createExecutionTimeout(seconds, stopWaiting);
}

describe("executionTimeoutOf", () => {
  it("reads no timeout from an absent, null or zero value", () => {
    expect(executionTimeoutOf({})).toBeNull();
    expect(executionTimeoutOf({ execution_timeout: null })).toBeNull();
    expect(executionTimeoutOf({ execution_timeout: 0 })).toBeNull();
  });

  it("keeps a positive or negative timeout", () => {
    expect(executionTimeoutOf({ execution_timeout: 2.5 })).toBe(2.5);
    expect(executionTimeoutOf({ execution_timeout: -1 })).toBe(-1);
  });
});

describe("createExecutionTimeout", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns what the task returns when it finishes in time", async () => {
    const timeout = timeoutOf(5);

    await expect(timeout.wait(Promise.resolve(42))).resolves.toBe(42);
    expect(timeout.signal.aborted).toBe(false);
  });

  it("passes the task's own failure through", async () => {
    const timeout = timeoutOf(5);

    await expect(timeout.wait(Promise.reject(new Error("boom")))).rejects.toThrow("boom");
    expect(timeout.signal.aborted).toBe(false);
  });

  it("aborts with TaskTimeoutError at the timeout and fails once the task has cleaned up", async () => {
    const timeout = timeoutOf(0.05);
    let cleanedUp = false;

    const outcome = timeout.wait(
      untilAborted(timeout.signal, () => {
        cleanedUp = true;
        throw timeout.signal.reason as Error;
      }),
    );

    await expect(outcome).rejects.toBeInstanceOf(TaskTimeoutError);
    await expect(outcome).rejects.toThrow("Task timed out after 0.05 seconds");
    expect(cleanedUp).toBe(true);
    expect(timeout.signal.reason).toBeInstanceOf(TaskTimeoutError);
    expect((timeout.signal.reason as TaskTimeoutError).timeoutSeconds).toBe(0.05);
  });

  it("fails with the timeout even when the task returns after being aborted", async () => {
    const timeout = timeoutOf(0.05);

    const outcome = timeout.wait(untilAborted(timeout.signal, () => "done anyway"));

    await expect(outcome).rejects.toBeInstanceOf(TaskTimeoutError);
  });

  it("waits for a task that ignores the signal until Airflow's SIGTERM", async () => {
    const airflowStop = new AbortController();
    const timeout = timeoutOf(0.05, airflowStop.signal);
    let settled = false;

    const outcome = timeout.wait(new Promise<never>(() => {}));
    void outcome.catch(() => (settled = true));
    await sleep(300);
    expect(timeout.signal.aborted).toBe(true);
    expect(settled).toBe(false);

    airflowStop.abort(new Error("Task aborted by SIGTERM"));
    await expect(outcome).rejects.toBeInstanceOf(TaskTimeoutError);
  });

  it("stops waiting for cleanup when Airflow stops the task", async () => {
    const airflowStop = new AbortController();
    const timeout = timeoutOf(0.05, airflowStop.signal);
    const started = Date.now();

    const outcome = timeout.wait(sleep(10_000));
    setTimeout(() => airflowStop.abort(new Error("Task aborted by SIGTERM")), 100);

    await expect(outcome).rejects.toBeInstanceOf(TaskTimeoutError);
    expect(Date.now() - started).toBeLessThan(2000);
  });

  it("enforces a timeout too long for one timer", async () => {
    vi.useFakeTimers();
    const timeoutMs = 30 * 24 * 3600 * 1000;
    const airflowStop = new AbortController();
    const timeout = timeoutOf(timeoutMs / 1000, airflowStop.signal);

    const outcome = timeout.wait(new Promise<never>(() => {}));
    const failed = expect(outcome).rejects.toBeInstanceOf(TaskTimeoutError);
    await vi.advanceTimersByTimeAsync(timeoutMs - 1);
    expect(timeout.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(timeout.signal.aborted).toBe(true);
    airflowStop.abort(new Error("Task aborted by SIGTERM"));
    await failed;
  });
});
