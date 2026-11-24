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

// What a task in a loop reads about it.
// Core counts the iterations and scopes every XCom read; the runtime only
// relays them. See airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md.

import { RETURN_VALUE_KEY, type CoordinatorClient } from "./client.js";
import { LazyXComSequence } from "./xcom-sequence.js";
import type { LoopContext as WireLoopContext } from "../generated/supervisor.js";
import type { LoopContext } from "../sdk/loop.js";

/**
 * Internal: what `getLoop()` resolves to for a task in `loop`, built on the first call.
 *
 * The previous iteration's result is read then, once, so a handler that never asks pays nothing.
 */
export function createLoopSource(
  loop: WireLoopContext,
  client: CoordinatorClient,
): () => Promise<LoopContext> {
  let pending: Promise<LoopContext> | undefined;
  return () => {
    pending ??= buildLoopContext(loop, client).catch((err: unknown) => {
      // A failed read is not remembered, so a later call asks again.
      pending = undefined;
      throw err;
    });
    return pending;
  };
}

async function buildLoopContext(
  loop: WireLoopContext,
  client: CoordinatorClient,
): Promise<LoopContext> {
  const previous = loop.index <= 1 ? null : await readPrevious(loop, client);
  return Object.freeze({
    index: loop.index,
    maxIterations: loop.max_iterations,
    getPrevious: <T = unknown>() => previous as T | null,
  });
}

async function readPrevious(loop: WireLoopContext, client: CoordinatorClient): Promise<unknown> {
  const taskId = loop.terminal_task_id;
  if (loop.terminal_is_mapped) {
    return new LazyXComSequence(client, {
      key: RETURN_VALUE_KEY,
      taskId,
      previousIteration: true,
    });
  }
  const entry = await client.getXComEntry({
    key: RETURN_VALUE_KEY,
    taskId,
    mapIndex: null,
    previousIteration: true,
  });
  return entry.value;
}
