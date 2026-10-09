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

// Runs an operator the SDK executes itself: starts it, or resumes it after it parked or deferred.

import { resolveArgs } from "./arg-binding.js";
import type { CoordinatorClient } from "./client.js";
import type { LogChannel } from "./log-channel.js";
import type {
  RuntimeAwaitInputTask,
  RuntimeDeferTask,
  RuntimeSucceedTask,
  StartupDetails,
} from "./protocol.js";
import type { JsonValue } from "../sdk/client-types.js";
import { isPlainRecord, skipTasks, type Dag } from "../sdk/dag.js";
import { getBooleanEnv } from "../sdk/env.js";
import type { Operator, OperatorContext, OperatorOutcome } from "../sdk/operator.js";
import type { TaskContext } from "../sdk/task.js";

const EXECUTE_COMPLETE = "execute_complete";
/** `TRIGGER_FAIL_REPR`: the `next_method` of a task Airflow could not resume. */
const TRIGGER_FAIL = "__fail__";

const NO_ARG_NAMES: ReadonlyMap<string, string> = new Map();

/** What {@link buildOperatorContext} builds the context from. */
export interface OperatorContextDeps {
  readonly details: StartupDetails;
  readonly dag: Dag;
  readonly ctx: TaskContext;
  readonly client: CoordinatorClient;
  readonly logs: LogChannel;
  readonly fail: OperatorContext["fail"];
}

/** Run `operator`: its `execute` on the first run, `executeComplete` on the run that resumes it. */
export async function runOperator(
  operator: Operator<never, unknown>,
  op: OperatorContext,
): Promise<OperatorOutcome> {
  const nextMethod = op.details.ti_context.next_method;
  if (!nextMethod) return operator.execute(op);
  const nextKwargs = op.details.ti_context.next_kwargs;
  const kwargs = isPlainRecord(nextKwargs) ? nextKwargs : {};
  if (nextMethod === TRIGGER_FAIL) return failedToResume(op, kwargs);
  if (nextMethod !== EXECUTE_COMPLETE || operator.executeComplete === undefined) {
    return op.fail(`Task cannot resume with next_method "${nextMethod}"`);
  }
  return operator.executeComplete(op, kwargs["event"]);
}

/** Airflow could not resume the task (`__fail__`), so it fails with Airflow's reason. */
function failedToResume(op: OperatorContext, kwargs: Record<string, unknown>) {
  const traceback = kwargs["traceback"];
  if (Array.isArray(traceback)) op.logs.error(`Trigger failed:\n${traceback.join("\n")}`);
  return op.fail(String(kwargs["error"] ?? "Unknown"));
}

export function buildOperatorContext(deps: OperatorContextDeps): OperatorContext {
  const { details, dag, ctx, client, logs, fail } = deps;
  return {
    details,
    dag,
    ctx,
    client,
    logs,
    fail,
    async resolveArgs(argNames = NO_ARG_NAMES) {
      const bound = await resolveArgs(details.ti_context?.arg_bindings, {
        client,
        signal: ctx.signal,
        logs,
        argNames,
      });
      return bound.args;
    },
    async succeed(value?: JsonValue): Promise<RuntimeSucceedTask> {
      if (value !== undefined) await client.setXCom({ key: "return_value", value });
      // The Execution API's TISuccessStatePayload validator rejects null for
      // `task_outlets` and `outlet_events`, so both are empty lists.
      return {
        type: "SucceedTask",
        end_date: new Date().toISOString(),
        task_outlets: [],
        outlet_events: [],
      };
    },
    skip: (taskIds) => skipTasks(client, taskIds),
    awaitInput(opts = {}): RuntimeAwaitInputTask {
      return {
        type: "AwaitInputTask",
        state: "awaiting_input",
        timeout: toIsoDuration(opts.timeoutSeconds),
        next_method: EXECUTE_COMPLETE,
        next_kwargs: opts.kwargs ?? {},
      };
    },
    defer(opts): RuntimeDeferTask {
      return {
        type: "DeferTask",
        state: "deferred",
        classpath: opts.classpath,
        trigger_kwargs: opts.kwargs,
        trigger_timeout: toIsoDuration(opts.timeoutSeconds),
        // `_defer_task` hands the trigger the task's queue only when triggerer queues are enabled.
        queue:
          opts.queue !== undefined
            ? opts.queue
            : getBooleanEnv("AIRFLOW__TRIGGERER__QUEUES_ENABLED", false)
              ? (details.ti.queue ?? null)
              : null,
        next_method: EXECUTE_COMPLETE,
        next_kwargs: {},
      };
    },
  };
}

/** An ISO-8601 duration, as `AwaitInputTask.timeout` and `DeferTask.trigger_timeout` are sent. */
function toIsoDuration(seconds: number | undefined): string | null {
  return seconds === undefined ? null : `PT${seconds}S`;
}
