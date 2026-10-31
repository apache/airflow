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

// A task's `execution_timeout`, enforced inside the runtime so the task can stop
// at the timeout and fail with it. The supervisor keeps its own deadline for a
// process that cannot stop itself, such as one stuck in a synchronous loop.

import { TaskTimeoutError } from "../sdk/task.js";

// setTimeout fires at once for a longer delay, so the supervisor alone enforces
// a timeout past this (about 24.8 days).
const MAX_TIMER_MS = 2 ** 31 - 1;

// What the timer resolves with: no value a task returns can equal it.
const TIMED_OUT = Symbol("timed out");

/**
 * The task's timeout in seconds, from its run context, or `null` when it has
 * none: unset and `0` both mean no timeout.
 */
export function executionTimeoutOf(tiContext: {
  execution_timeout?: number | null;
}): number | null {
  const seconds = tiContext.execution_timeout;
  return seconds === undefined || seconds === null || seconds === 0 ? null : seconds;
}

/** One task's execution timeout: the signal it aborts, and the wait that enforces it. */
export interface ExecutionTimeout {
  /** The timeout in seconds. A negative one has already passed. */
  readonly seconds: number;
  /** Aborted with a {@link TaskTimeoutError} when the timeout passes. */
  readonly signal: AbortSignal;
  /**
   * Wait for the task, which is already running; the timeout counts from here.
   *
   * At the timeout {@link signal} is aborted, so a task passing it to its work
   * stops there. The task then has the cleanup time to finish; what it returns
   * or throws by then is ignored, so a task that times out never succeeds. A
   * task that ignores the signal is not waited for longer: the timeout is
   * reported, and the supervisor ends the process if it lingers.
   */
  wait<T>(running: Promise<T>): Promise<T>;
}

/** How long {@link ExecutionTimeout.wait} waits for a task after its timeout. */
export interface ExecutionTimeoutOptions {
  /** How long the task has to finish after the timeout. */
  readonly cleanupMs: number;
  /**
   * Ends that wait early: the runtime's signal, which the supervisor's SIGTERM
   * aborts when its own deadline comes first.
   */
  readonly stopWaiting: AbortSignal;
}

export function createExecutionTimeout(
  seconds: number,
  { cleanupMs, stopWaiting }: ExecutionTimeoutOptions,
): ExecutionTimeout {
  const controller = new AbortController();
  return {
    seconds,
    signal: controller.signal,
    async wait<T>(running: Promise<T>): Promise<T> {
      const timeoutMs = seconds * 1000;
      if (timeoutMs > MAX_TIMER_MS) return running;

      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedOut = new Promise<typeof TIMED_OUT>((resolve) => {
        timer = setTimeout(() => resolve(TIMED_OUT), timeoutMs);
      });
      let first: T | typeof TIMED_OUT;
      try {
        first = await Promise.race([running, timedOut]);
      } finally {
        clearTimeout(timer);
      }
      if (first !== TIMED_OUT) return first;

      const error = new TaskTimeoutError(seconds);
      controller.abort(error);
      // Until the task finishes, its cleanup time runs out, or the supervisor's
      // SIGTERM arrives, whichever comes first.
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(cleanupTimer);
          stopWaiting.removeEventListener("abort", done);
          resolve();
        };
        const cleanupTimer = setTimeout(done, cleanupMs);
        stopWaiting.addEventListener("abort", done);
        if (stopWaiting.aborted) done();
        running.then(done, done);
      });
      throw error;
    },
  };
}
