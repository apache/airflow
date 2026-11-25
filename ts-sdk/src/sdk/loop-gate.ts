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

// A loop's gate: the operator that runs after each iteration and decides whether another follows.
// See airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md.

import type { LoopDecisionPayload } from "../generated/supervisor.js";
import { getArgNames } from "./arg-names.js";
import type { LoopDecision, UntilCallable } from "./loop.js";
import {
  brandOperator,
  type Operator,
  type OperatorContext,
  type OperatorOutcome,
} from "./operator.js";

/** Core finds a loop's gate by this task type, whatever language the gate runs in (ADR-0017 decision 2). */
const LOOP_GATE_TASK_TYPE = "LoopGateOperator";

/**
 * A lone surrogate has no UTF-8 encoding, so a reason holding one would leave the supervisor unable
 * to decode the frame. A NUL, which core refuses to store, is checked apart from this, since the
 * linter rejects a control character in a regular expression.
 */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/** Internal: the gate of a loop, deciding with `until`, or by counting when there is none. */
export function createLoopGate(until: UntilCallable<never> | undefined): Operator<never, void> {
  return brandOperator<Operator<never, void>>({
    operatorName: LOOP_GATE_TASK_TYPE,
    taskType: LOOP_GATE_TASK_TYPE,
    execute: (op) => executeLoopGate(until, op),
  });
}

/**
 * Decide whether another iteration follows, and send core the decision.
 *
 * The reason goes into the task log as it is: the supervisor masks the log and the reason alike.
 *
 * The gate pushes no return value. Core creates the next iteration in the same transaction that
 * accepts the decision (ADR-0017 decision 5). A `stop` of false at the last iteration fails the gate
 * here without a retry, as Python does, and the decision goes with the failure so its reason is kept.
 */
async function executeLoopGate(
  until: UntilCallable<never> | undefined,
  op: OperatorContext,
): Promise<OperatorOutcome> {
  const { ctx, logs } = op;
  const loop = op.details.ti_context.loop;
  if (loop == null) {
    return op.fail(
      `Task "${ctx.taskId}" is the gate of a loop, but Airflow ran it with no loop context; ` +
        "the Dag version it ran against does not declare the loop",
      { retry: false },
    );
  }
  let decision: LoopDecisionPayload;
  if (until === undefined) {
    decision = { stop: loop.index >= loop.max_iterations };
  } else {
    let args: object;
    try {
      args = await op.resolveArgs(getArgNames(until));
    } catch (err) {
      return op.fail((err as Error).message ?? String(err), {
        logEvent: "Cannot bind this task's call arguments",
      });
    }
    let returned: unknown;
    try {
      returned = await op.runInScope(() => until(args as never));
    } catch (err) {
      // A handler that destructured an argument under a name nothing folds to gets no error of its
      // own, so say what it was called with.
      return op.fail((err as Error).message ?? String(err), {
        logFields: { stack: (err as Error).stack ?? null, bound_args: Object.keys(args) },
      });
    }
    try {
      decision = toLoopDecisionPayload(returned);
    } catch (err) {
      // The handler would return the same value again, so a retry cannot help, as Python's
      // AirflowFailException for a malformed LoopDecision has it.
      return op.fail((err as Error).message, { retry: false });
    }
    if (!decision.stop && loop.index >= loop.max_iterations) {
      // Python raises LoopMaxIterationsExceeded, an AirflowFailException, which is never retried.
      const reason = decision.reason === undefined ? "" : `: ${decision.reason}`;
      return op.fail(
        `Loop condition remained false at max_iterations=${loop.max_iterations}${reason}`,
        { retry: false, loopDecision: decision },
      );
    }
  }
  logs.info(`Loop iteration ${loop.index}: ${decision.stop ? "stop" : "continue"}`, {
    task_id: ctx.taskId,
    index: loop.index,
    max_iterations: loop.max_iterations,
    ...(decision.reason !== undefined && { reason: decision.reason }),
  });
  return op.succeed(undefined, { loopDecision: decision });
}

/**
 * Internal: what a loop's gate sends core, from what its `until` handler returned.
 *
 * Throws, naming the problem, for a value core would refuse: one the supervisor could not even
 * decode would leave the gate without an answer. A `stop` of false at the last iteration is valid
 * here; the gate fails for it.
 */
export function toLoopDecisionPayload(returned: unknown): LoopDecisionPayload {
  if (typeof returned === "boolean") return { stop: returned };
  if (typeof returned !== "object" || returned === null || Array.isArray(returned)) {
    throw new Error(
      `A loop's until handler must return a boolean or { stop, reason }, not ${describe(returned)}`,
    );
  }
  const { stop, reason } = returned as Partial<LoopDecision>;
  if (typeof stop !== "boolean") {
    throw new Error(`The stop of a loop decision must be a boolean, not ${describe(stop)}`);
  }
  if (reason === undefined || reason === null || reason === "") return { stop };
  if (typeof reason !== "string") {
    throw new Error(`The reason of a loop decision must be a string, not ${describe(reason)}`);
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
  if (value === null || value === undefined) return String(value);
  if (Array.isArray(value)) return "an array";
  return typeof value === "object" ? "an object" : `the ${typeof value} ${String(value)}`;
}
