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
// As `HITLOperator` does, the second run reads the options, the params and the reject policy from
// the task it is started for, and works out what a rejection skips from that task's Dag; nothing
// is carried over from the first run. A versioned Dag bundle starts both runs from the same
// version. With an unversioned bundle, a bundle deployed while the task waits is the one the
// response is read against, as it is for any task that runs after a deployment.

import type { JsonValue } from "../sdk/client-types.js";
import { isPlainRecord, type Dag } from "../sdk/dag.js";
import { getDagDownstreamTaskIds } from "../sdk/dag-graph.js";
import { brandOperator, type OperatorContext, type OperatorOutcome } from "../sdk/operator.js";
import { runInTaskScope } from "../sdk/task.js";
import type { HITLResult, HITLTask, HITLTaskFields, HITLText, HITLUser } from "./spec.js";

export const APPROVE = "Approve";
export const REJECT = "Reject";

/** Build the operator for a task whose options `spec.ts` has checked. */
export function createHITLTask<TArgs extends object | void>(
  fields: HITLTaskFields,
): HITLTask<TArgs> {
  const isApproval = fields.kind === "approval";
  const task: HITLTask<never> = {
    ...fields,
    operatorName: isApproval ? "ApprovalOperator" : "HITLOperator",
    // An approval skips what follows it on "Reject".
    canSkipDownstream: isApproval,
    requiresTaskId: true,
    label: "human-in-the-loop",
    taskIdExample: isApproval
      ? 'dag.task("sign_off", approval({ subject: "..." }))'
      : 'dag.task("choose", hitl({ subject: "...", options: ["..."] }))',
    executionTimeoutAlternative: "responseTimeout",
    execute: (op) => executeHITL(task, op),
    executeComplete: (op, event) => resumeHITL(task, op, event),
  };
  return brandOperator(task) as HITLTask<TArgs>;
}

/** The first run: write the request, then park the task until a response arrives. */
async function executeHITL(task: HITLTask<never>, op: OperatorContext): Promise<OperatorOutcome> {
  const { details, ctx, client, logs } = op;
  // The inputs are read here only, for the text: the resumed run must not depend on upstream
  // XComs still being there.
  const args = await op.resolveArgs(task.argNames);
  const render = (text: HITLText<never, string | null>) =>
    runInTaskScope({ ctx, client }, () => renderText(text, args));
  const subject = await render(task.subject);
  const body = task.body === undefined ? null : await render(task.body);
  if (typeof subject !== "string" || subject.length === 0) {
    return op.fail(`The subject of task "${ctx.taskId}" has to be a non-empty string`);
  }
  if (body !== null && body !== undefined && typeof body !== "string") {
    return op.fail(`The body of task "${ctx.taskId}" has to be a string or null`);
  }

  const options = task.options as [string, ...string[]];
  const defaults = task.defaults === undefined ? null : [...task.defaults];
  const assignedUsers = task.assignedUsers.map((user) => ({ id: user.id, name: user.name }));
  await client.createHITLDetail({
    ti_id: details.ti.id,
    options,
    subject,
    body: body ?? null,
    defaults,
    multiple: task.multiple,
    params: serializeParams(task.params),
    assigned_users: assignedUsers,
  });

  logs.info("Waiting for response", { task_id: ctx.taskId });
  return op.awaitInput({ timeoutSeconds: task.responseTimeout });
}

/**
 * The params as `Param.serialize()` writes them. The source is always "task":
 * `HITLOperator` drops the params that come from the Dag before it sends them.
 */
function serializeParams(params: HITLTask<never>["params"]): Record<string, JsonValue> {
  return Object.fromEntries(
    Object.entries(params).map(([name, { value, description, schema }]) => [
      name,
      { value, description: description ?? null, schema: schema ?? {}, source: "task" },
    ]),
  );
}

/** The second run: read the response against the task's options, then finish. */
async function resumeHITL(
  task: HITLTask<never>,
  op: OperatorContext,
  event: unknown,
): Promise<OperatorOutcome> {
  const { dag, ctx, logs } = op;
  // Throws, failing the task, when the answer cannot be read or is not allowed.
  const result = readAnswer(event, task);
  const responder = result.responded_by_user?.name ?? "the response timeout default";
  logs.info("Received response", { chosen_options: result.chosen_options, responder });

  // `ApprovalOperator.execute_complete`: on "Reject", fail, or skip what follows.
  if (task.kind === "approval" && result.chosen_options[0] === REJECT) {
    if (task.failOnReject) return op.fail(`Rejected by ${responder}`);
    const skipped = getSkipTargets(dag, ctx.taskId, task.ignoreDownstreamTriggerRules);
    if (skipped.length > 0) {
      // `SkipMixin.skip` ends the task as soon as it skips, so Python pushes no response then.
      logs.info("Skipping downstream tasks", { task_ids: skipped });
      await op.skip(skipped);
      return op.succeed();
    }
    logs.info("No downstream tasks; nothing to do.");
  } else if (task.kind === "approval") {
    logs.info("Approved. Proceeding with downstream tasks...");
  }
  return op.succeed(result as unknown as JsonValue);
}

/**
 * Turn the answer Airflow resumed the task with into the result downstream tasks get, or throw
 * the reason the task fails. It only reads and checks; `resumeHITL` acts on what it returns.
 *
 * For an approval, for example:
 * - admin picked "Approve": the result `["Approve"]`, answered by admin.
 * - the timeout passed, with `defaults: "Approve"`: the result `["Approve"]`, answered by no
 *   one, `timedout: true`.
 * - the timeout passed, with no defaults: throws "Response timed out: ...".
 * - `["Maybe"]`, an option it does not offer: throws, as `HITLOperator.validate_chosen_options` does.
 * - a params input that does not match the task's params: throws, as
 *   `HITLOperator.validate_params_input` does.
 */
function readAnswer(event: unknown, task: HITLTask<never>): HITLResult {
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

  // 4. Check the params input is the form the task asked for.
  const paramsInput = (isPlainRecord(event["params_input"]) ? event["params_input"] : {}) as Record<
    string,
    JsonValue
  >;
  const mismatch = checkParamsInput(paramsInput, Object.keys(task.params));
  if (mismatch !== undefined) throw new Error(mismatch);

  // 5. The result downstream tasks receive.
  return {
    chosen_options: chosen,
    params_input: paramsInput,
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
function checkChoice(chosen: string[], task: HITLTask<never>): string | undefined {
  const unknown = chosen.filter((option) => !task.options.includes(option));
  if (unknown.length > 0) {
    return `Responses ${JSON.stringify(unknown)} not in ${JSON.stringify(task.options)}`;
  }
  return undefined;
}

/**
 * `HITLOperator.validate_params_input`: why the params input is not the form the task asked for,
 * or undefined. An input for no param, or a task with no params, is not checked.
 */
function checkParamsInput(
  given: Record<string, JsonValue>,
  declared: readonly string[],
): string | undefined {
  const received = Object.keys(given);
  if (declared.length === 0 || received.length === 0) return undefined;
  if (received.length === declared.length && declared.every((key) => Object.hasOwn(given, key))) {
    return undefined;
  }
  return `params_input ${JSON.stringify(received)} does not match params ${JSON.stringify(declared)}`;
}

/** The tasks a rejection skips: the direct downstream, or every task downstream. */
function getSkipTargets(dag: Dag, taskId: string, ignoreDownstreamTriggerRules: boolean): string[] {
  const downstream = getDagDownstreamTaskIds(dag);
  const found = new Set(downstream.get(taskId) ?? []);
  if (ignoreDownstreamTriggerRules) {
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

/** The text, calling it first when it is a function; the caller checks what it returns. */
async function renderText(text: HITLText<never, string | null>, args: object): Promise<unknown> {
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
