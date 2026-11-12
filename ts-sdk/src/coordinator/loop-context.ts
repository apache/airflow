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

// What a task in a loop reads about it, and what a loop's gate sends back.
// Core counts the iterations and scopes every XCom read; the runtime only
// relays them. See airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md.

import { RETURN_VALUE_KEY, type CoordinatorClient } from "./client.js";
import { LazyXComSequence } from "./xcom-sequence.js";
import type {
  LoopContext as WireLoopContext,
  LoopDecisionPayload,
} from "../generated/supervisor.js";
import type { LoopContext, LoopDecision } from "../sdk/loop.js";

/**
 * A lone surrogate has no UTF-8 encoding, so a reason holding one would leave the supervisor unable
 * to decode the frame. A NUL, which core refuses to store, is checked apart from this, since the
 * linter rejects a control character in a regular expression.
 */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

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

/**
 * Internal: what a loop's gate sends core, from what its `until` handler returned.
 *
 * Throws, naming the problem, for a value core would refuse: one the supervisor could not even
 * decode would leave the gate without an answer. A `stop` of false at the last iteration is
 * passed on, since core is what rejects it (ADR-0017 decision 5).
 */
export function toLoopDecisionPayload(returned: unknown): LoopDecisionPayload {
  if (typeof returned === "boolean") return { stop: returned };
  if (typeof returned !== "object" || returned === null || Array.isArray(returned)) {
    throw new Error(
      `A loop's until handler returns a boolean or { stop, reason }, not ${describe(returned)}`,
    );
  }
  const { stop, reason } = returned as Partial<LoopDecision>;
  if (typeof stop !== "boolean") {
    throw new Error(`The stop of a loop decision is a boolean, not ${describe(stop)}`);
  }
  if (reason === undefined || reason === null || reason === "") return { stop };
  if (typeof reason !== "string") {
    throw new Error(`The reason of a loop decision is a string, not ${describe(reason)}`);
  }
  // No length check: the supervisor cuts a reason to core's limit and warns in the task log.
  if (reason.includes("\u0000") || LONE_SURROGATE.test(reason)) {
    throw new Error(
      "The reason of a loop decision cannot hold a NUL character or a lone surrogate",
    );
  }
  return { stop, reason };
}

function describe(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  return typeof value === "object" ? "an object" : `the ${typeof value} ${String(value)}`;
}
