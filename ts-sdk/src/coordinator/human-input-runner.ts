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

// Mirrors `HITLOperator.execute` / `execute_complete` and `ApprovalOperator.execute_complete`
// in the standard provider, on the Airflow 3.3+ path that parks the task in `awaiting_input`.
//
// The task runs twice, in two processes. The first run writes the request and parks; the
// second starts with `next_method` set and the response in `next_kwargs.event`.
//
// As `HITLOperator` does, the second run reads the options and the reject policy from the task
// it is started for, and works out what a rejection skips from that task's Dag; nothing is
// carried over from the first run. A versioned Dag bundle starts both runs from the same
// version. With an unversioned bundle, a bundle deployed while the task waits is the one the
// response is read against, as it is for any task that runs after a deployment.

import { resolveArgs } from "./arg-binding.js";
import type { CoordinatorClient } from "./client.js";
import type { LogChannel } from "./log-channel.js";
import type {
  RuntimeAwaitInputTask,
  RuntimeRetryTask,
  RuntimeSucceedTask,
  RuntimeTaskState,
  StartupDetails,
} from "./protocol.js";
import { getDagDownstreamTaskIds } from "./serde.js";
import { getArgNames } from "../sdk/arg-names.js";
import type { JsonValue } from "../sdk/client-types.js";
import { isPlainRecord, skipTasks, type Dag } from "../sdk/dag.js";
import {
  REJECT,
  type HITLUser,
  type HumanInputResult,
  type HumanInputTask,
  type HumanInputText,
} from "../sdk/human-input.js";
import { runInTaskScope, type TaskContext } from "../sdk/task.js";

/** How the task fails: for retry, or for good once its retries are spent. */
type Failure = RuntimeRetryTask | RuntimeTaskState;
export type HumanInputOutcome = RuntimeSucceedTask | Failure | RuntimeAwaitInputTask;

/** The `next_method` the task parks with and resumes with, as `HITLOperator` names it. */
const EXECUTE_COMPLETE = "execute_complete";
/** `TRIGGER_FAIL_REPR`: the `next_method` of a task Airflow could not resume. */
const TRIGGER_FAIL = "__fail__";

export interface HumanInputRun {
  readonly details: StartupDetails;
  readonly dag: Dag;
  readonly task: HumanInputTask<never>;
  readonly client: CoordinatorClient;
  readonly ctx: TaskContext;
  readonly logs: LogChannel;
  readonly fail: (message: string) => Failure;
}

export async function runHumanInput(run: HumanInputRun): Promise<HumanInputOutcome> {
  const nextMethod = run.details.ti_context.next_method;
  if (nextMethod) return resume(run, nextMethod, run.details.ti_context.next_kwargs);
  return park(run);
}

/** The first run: write the request, then park the task until a response arrives. */
async function park({ details, task, client, ctx, logs, fail }: HumanInputRun) {
  // The inputs are read here only, for the text: the resumed run must not depend on upstream
  // XComs still being there.
  const bound = await resolveArgs(details.ti_context?.arg_bindings, {
    client,
    signal: ctx.signal,
    logs,
    argNames: textArgNames(task),
  });
  const render = (text: HumanInputText<never, string | null>) =>
    runInTaskScope({ ctx, client }, () => renderText(text, bound.args));
  const subject = await render(task.subject);
  const body = task.body === undefined ? null : await render(task.body);
  if (typeof subject !== "string" || subject.length === 0) {
    return fail(`The subject of task "${ctx.taskId}" has to be a non-empty string`);
  }
  if (body !== null && body !== undefined && typeof body !== "string") {
    return fail(`The body of task "${ctx.taskId}" has to be a string or null`);
  }

  const options = task.options as [string, ...string[]];
  const defaults = task.defaults === undefined ? null : [...task.defaults];
  const assignedUsers = task.assignees.map((user) => ({ id: user.id, name: user.name }));
  await client.createHITLDetail({
    ti_id: details.ti.id,
    options,
    subject,
    body: body ?? null,
    defaults,
    multiple: task.multiple,
    params: {},
    assigned_users: assignedUsers,
  });

  // An ISO-8601 duration, which is how `AwaitInputTask.timeout` is sent.
  const timeout = task.responseTimeout === undefined ? null : `PT${task.responseTimeout}S`;
  logs.info("Waiting for response", { task_id: ctx.taskId });
  return {
    type: "AwaitInputTask",
    state: "awaiting_input",
    timeout,
    next_method: EXECUTE_COMPLETE,
    next_kwargs: {},
  } satisfies RuntimeAwaitInputTask;
}

/** The second run: read the response against the task's options, then finish. */
async function resume(
  run: HumanInputRun,
  nextMethod: string,
  nextKwargs: unknown,
): Promise<HumanInputOutcome> {
  const { task, dag, ctx, client, logs, fail } = run;
  const kwargs = isPlainRecord(nextKwargs) ? nextKwargs : {};
  if (nextMethod === TRIGGER_FAIL) return failedToResume(run, kwargs);
  if (nextMethod !== EXECUTE_COMPLETE) {
    return fail(`Task cannot resume with next_method "${nextMethod}"`);
  }

  // Throws, failing the task, when the answer cannot be read or is not allowed.
  const result = readAnswer(kwargs["event"], task);
  const responder = result.responded_by_user?.name ?? "the response timeout default";
  logs.info("Received response", { chosen_options: result.chosen_options, responder });

  // `ApprovalOperator.execute_complete`: on "Reject", fail, or skip what follows. Only an approval
  // has a reject policy.
  if (task.onReject !== undefined && result.chosen_options[0] === REJECT) {
    if (task.onReject === "fail") return fail(`Rejected by ${responder}`);
    const skipped = skipTargets(dag, ctx.taskId, task.onReject);
    if (skipped.length > 0) {
      // `SkipMixin.skip` ends the task as soon as it skips, so Python pushes no response then.
      logs.info("Skipping downstream tasks", { task_ids: skipped });
      await skipTasks(client, skipped);
      return succeeded();
    }
    logs.info("No downstream tasks; nothing to do.");
  } else if (task.onReject !== undefined) {
    logs.info("Approved. Proceeding with downstream tasks...");
  }
  await client.setXCom({ key: "return_value", value: result as unknown as JsonValue });
  return succeeded();
}

/** Airflow could not resume the task (`__fail__`), so it fails with Airflow's reason. */
function failedToResume({ logs, fail }: HumanInputRun, kwargs: Record<string, unknown>) {
  const traceback = kwargs["traceback"];
  if (Array.isArray(traceback)) logs.error(`Task could not be resumed:\n${traceback.join("\n")}`);
  return fail(String(kwargs["error"] ?? "Unknown"));
}

/**
 * Turn the answer Airflow resumed the task with into the result downstream tasks get, or throw
 * the reason the task fails. It only reads and checks; `resume` acts on what it returns.
 *
 * For an approval, for example:
 * - admin picked "Approve": the result `["Approve"]`, answered by admin.
 * - the timeout passed, with `defaults: "Approve"`: the result `["Approve"]`, answered by no
 *   one, `timedout: true`.
 * - the timeout passed, with no defaults: throws "Response timed out: ...".
 * - `["Maybe"]`, an option it does not offer: throws, as `HITLOperator.validate_chosen_options` does.
 */
function readAnswer(event: unknown, task: HumanInputTask<never>): HumanInputResult {
  if (!isPlainRecord(event)) {
    throw new Error(`Task resumed with an event it cannot read: ${JSON.stringify(event)}`);
  }

  // 1. Airflow resumed the task with a failure: a timeout with no defaults, or another error.
  if ("error" in event) throw new Error(failureReason(event));

  // 2. Read the answer. A field Airflow sent in a shape it cannot be read in is undefined.
  const chosen = isStringArray(event["chosen_options"]) ? event["chosen_options"] : undefined;
  const respondedAt = decodeDatetime(event["responded_at"]);
  const respondedBy = readUser(event["responded_by_user"]);
  if (chosen === undefined || respondedAt === undefined || respondedBy === undefined) {
    throw new Error(`Task resumed with a response it cannot read: ${JSON.stringify(event)}`);
  }

  // 3. Check the choice is one the task allows.
  const notAllowed = checkChoice(chosen, task);
  if (notAllowed !== undefined) throw new Error(notAllowed);

  // 4. The result downstream tasks receive.
  const paramsInput = isPlainRecord(event["params_input"]) ? event["params_input"] : {};
  return {
    chosen_options: chosen,
    params_input: paramsInput as Record<string, JsonValue>,
    responded_at: respondedAt.toISOString(),
    responded_by_user: respondedBy,
    timedout: event["timedout"] === true,
  };
}

/** `HITLOperator.process_trigger_event_error`: why a failed resume fails the task. */
function failureReason(event: Record<string, unknown>): string {
  const reason = String(event["error"]);
  return event["error_type"] === "timeout" ? `Response timed out: ${reason}` : reason;
}

/** Who answered: `null` when the timeout defaults did, undefined when it cannot be read. */
function readUser(value: unknown): HITLUser | null | undefined {
  if (value === null || value === undefined) return null;
  return isUser(value) ? { id: value.id, name: value.name } : undefined;
}

/**
 * `HITLOperator.validate_chosen_options`: why the choice is not allowed, or undefined. How many
 * options were chosen is not checked again: Airflow checked that against the request when the
 * answer was given.
 */
function checkChoice(chosen: string[], task: HumanInputTask<never>): string | undefined {
  const unknown = chosen.filter((option) => !task.options.includes(option));
  if (unknown.length > 0) {
    return `Responses ${JSON.stringify(unknown)} not in ${JSON.stringify(task.options)}`;
  }
  return undefined;
}

/** The tasks a rejection skips: the direct downstream, or every task downstream. */
function skipTargets(dag: Dag, taskId: string, onReject: "skip" | "skipAll"): string[] {
  const downstream = getDagDownstreamTaskIds(dag);
  const found = new Set(downstream.get(taskId) ?? []);
  if (onReject === "skipAll") {
    const pending = [...found];
    while (pending.length > 0) {
      for (const next of downstream.get(pending.pop()!) ?? []) {
        if (!found.has(next)) {
          found.add(next);
          pending.push(next);
        }
      }
    }
  }
  return [...found].sort();
}

/**
 * The response time Airflow resumed the task with, or undefined when it cannot be read.
 *
 * serde writes a datetime as `{__classname__, __version__, __data__: {timestamp, tz}}`, under
 * `datetime.datetime` or `pendulum.datetime.DateTime` depending on where the value came from;
 * the timestamp, in seconds, is the instant either way.
 */
export function decodeDatetime(value: unknown): Date | undefined {
  const data = isPlainRecord(value) ? value["__data__"] : undefined;
  const seconds = isPlainRecord(data) ? data["timestamp"] : undefined;
  if (typeof seconds !== "number" || !Number.isFinite(seconds)) return undefined;
  // Truncated, as the stored microseconds are, so the result reads as the same instant.
  return new Date(Math.floor(seconds * 1000));
}

/** The `withArgNames` renames the task's text function declares; a fixed text declares none. */
function textArgNames(task: HumanInputTask<never>) {
  const textFunction = [task.subject, task.body].find((text) => typeof text === "function");
  return getArgNames((textFunction ?? (() => undefined)) as never);
}

/** The text, calling it first when it is a function; the caller checks what it returns. */
async function renderText(
  text: HumanInputText<never, string | null>,
  args: object,
): Promise<unknown> {
  return typeof text === "function" ? await text(args as never) : text;
}

function isUser(value: unknown): value is HITLUser {
  return (
    isPlainRecord(value) && typeof value["id"] === "string" && typeof value["name"] === "string"
  );
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function succeeded(): RuntimeSucceedTask {
  return {
    type: "SucceedTask",
    end_date: new Date().toISOString(),
    task_outlets: [],
    outlet_events: [],
  };
}
