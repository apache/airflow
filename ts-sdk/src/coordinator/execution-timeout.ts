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

// setTimeout fires at once for a delay past this (about 24.8 days), so a longer
// timeout re-arms the timer in steps of at most this.
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
   * stops there. The task then has until the supervisor's SIGTERM, which comes
   * `killed_task_cleanup_time` after the timeout, to finish; what it returns or
   * throws by then is ignored, so a task that times out never succeeds. At the
   * SIGTERM the timeout is reported, and the runtime then ends the process.
   */
  wait<T>(running: Promise<T>): Promise<T>;
}

/**
 * `stopWaiting` ends the wait after the timeout: the runtime's signal, which the
 * supervisor's SIGTERM aborts. `SetExecutionTimeout` starts the supervisor's
 * deadline when this timeout starts, so the SIGTERM comes exactly
 * `killed_task_cleanup_time` after it, the cleanup time a Python task gets.
 */
export function createExecutionTimeout(
  seconds: number,
  stopWaiting: AbortSignal,
): ExecutionTimeout {
  const controller = new AbortController();
  return {
    seconds,
    signal: controller.signal,
    async wait<T>(running: Promise<T>): Promise<T> {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedOut = new Promise<typeof TIMED_OUT>((resolve) => {
        const arm = (leftMs: number) => {
          timer =
            leftMs > MAX_TIMER_MS
              ? setTimeout(() => arm(leftMs - MAX_TIMER_MS), MAX_TIMER_MS)
              : setTimeout(() => resolve(TIMED_OUT), leftMs);
        };
        arm(seconds * 1000);
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
      // Until the task finishes or the supervisor's SIGTERM arrives.
      await new Promise<void>((resolve) => {
        const done = () => {
          stopWaiting.removeEventListener("abort", done);
          resolve();
        };
        stopWaiting.addEventListener("abort", done);
        if (stopWaiting.aborted) done();
        running.then(done, done);
      });
      throw error;
    },
  };
}
