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

// The task loop surface a handler sees: what a gate returns, and `getLoop()`.
// See ts-sdk/adr/0003-task-loop-authoring-interface.md.

import { getTaskScope } from "./task.js";

/**
 * What a loop's `until` handler returns. A bare `boolean` is `{ stop }`.
 *
 * `reason` is shown in the Airflow UI next to the iteration, so a secret the handler masked does not
 * belong in it unredacted; the supervisor redacts it on the way out. It is at most 512 bytes of
 * UTF-8, and an empty one is the same as none.
 */
export interface LoopDecision {
  readonly stop: boolean;
  readonly reason?: string;
}

/**
 * The gate's handler. `A` is its wired inputs, one key per argument, exactly as a task handler takes
 * its own. Nothing of the SDK's is in `A`; the loop context comes from {@link getLoop}.
 */
export type UntilCallable<A> = (
  args: A,
) => LoopDecision | boolean | Promise<LoopDecision | boolean>;

/** The loop a running task is in, as {@link getLoop} returns it. */
export interface LoopContext {
  /** The iteration this task runs in, counting from 1. */
  readonly index: number;
  /** The loop's bound: no iteration runs past it. */
  readonly maxIterations: number;
  /**
   * The previous iteration's terminal result as it arrived, or `null` at iteration 1 and when the
   * terminal task returned nothing. An `XComSequence` when the terminal task is mapped.
   */
  readonly previous: unknown;
  /** The same value as {@link previous}, as the type the caller names, as `client.getXCom<T>` does. */
  getPrevious<T>(): T | null;
}

/**
 * The loop the running task is in: which iteration, the bound, and the previous iteration's result.
 *
 * ```ts
 * const improve = refine.task(async function improve(): Promise<number> {
 *   const loop = await getLoop();
 *   const estimate = loop.getPrevious<number>() ?? 1.0;
 *   return (estimate + 2.0 / estimate) / 2.0;
 * });
 * ```
 *
 * Awaited because the previous result is read from Airflow, once per task run however often this is
 * called. A handler that never calls it pays nothing.
 *
 * @throws when called outside a task handler, or from a task that is not in a loop body or gate.
 */
export async function getLoop(): Promise<LoopContext> {
  const { ctx, loop } = getTaskScope("getLoop");
  if (loop === undefined) {
    throw new Error(
      `getLoop() was called from task "${ctx.taskId}" of Dag "${ctx.dagId}", which is not in a ` +
        "loop; it is only available to the tasks of a loop body and to the loop's until handler",
    );
  }
  return loop();
}
