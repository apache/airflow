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
import { Deferred } from "../../src/coordinator/deferred.js";

describe("Deferred", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("settles only once", async () => {
    const deferred = new Deferred<number>();

    deferred.resolve(1);
    deferred.resolve(2);
    deferred.reject(new Error("late"));

    await expect(deferred.promise).resolves.toBe(1);
    expect(deferred.settled).toBe(true);
  });

  it("runs onSettle once after resolve", async () => {
    const onSettle = vi.fn();
    const deferred = new Deferred<number>().onSettle(onSettle);

    deferred.resolve(1);
    deferred.resolve(2);

    await expect(deferred.promise).resolves.toBe(1);
    expect(onSettle).toHaveBeenCalledTimes(1);
  });

  it("runs onSettle once after reject", async () => {
    const onSettle = vi.fn();
    const deferred = new Deferred<number>().onSettle(onSettle);
    const assertion = expect(deferred.promise).rejects.toThrow("boom");

    deferred.reject(new Error("boom"));
    deferred.resolve(1);

    await assertion;
    expect(onSettle).toHaveBeenCalledTimes(1);
  });

  it("runs onSettle immediately when already settled", async () => {
    const onSettle = vi.fn();
    const deferred = new Deferred<number>();

    deferred.resolve(1);
    deferred.onSettle(onSettle);

    await expect(deferred.promise).resolves.toBe(1);
    expect(onSettle).toHaveBeenCalledTimes(1);
  });

  it("rejects after timeout", async () => {
    vi.useFakeTimers();
    const deferred = new Deferred<number>().rejectAfter(10, () => new Error("timeout"));

    const assertion = expect(deferred.promise).rejects.toThrow("timeout");
    await vi.advanceTimersByTimeAsync(10);

    await assertion;
    expect(deferred.settled).toBe(true);
  });

  it("clears rejectAfter timer when resolved first", async () => {
    vi.useFakeTimers();
    const makeError = vi.fn(() => new Error("timeout"));
    const deferred = new Deferred<number>().rejectAfter(10, makeError);

    deferred.resolve(1);
    await vi.advanceTimersByTimeAsync(10);

    await expect(deferred.promise).resolves.toBe(1);
    expect(makeError).not.toHaveBeenCalled();
  });

  it("clears rejectAfter timer when rejected first", async () => {
    vi.useFakeTimers();
    const makeError = vi.fn(() => new Error("timeout"));
    const deferred = new Deferred<number>().rejectAfter(10, makeError);
    const assertion = expect(deferred.promise).rejects.toThrow("manual");

    deferred.reject(new Error("manual"));
    await vi.advanceTimersByTimeAsync(10);

    await assertion;
    expect(makeError).not.toHaveBeenCalled();
  });
});
