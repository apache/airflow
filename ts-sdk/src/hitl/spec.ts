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

// Human-in-the-loop tasks: `hitl(...)` and `approval(...)`, which mirror `HITLOperator` and
// `ApprovalOperator` in the standard provider. A factory is the Python operator's name without
// "Operator", and an option is the Python keyword argument in camelCase.

import { getArgNames } from "../sdk/arg-names.js";
import type { JsonValue } from "../sdk/client-types.js";
import { isPlainRecord } from "../sdk/dag.js";
import type { Operator } from "../sdk/operator.js";
import { toPlainJson } from "../sdk/plain-json.js";
import { APPROVE, createHITLTask, REJECT } from "./operator.js";

/** A user allowed to respond, as Airflow's auth manager identifies them. */
export type HITLUser = {
  readonly id: string;
  readonly name: string;
};

/** Text shown to the responder: fixed, or computed from the task's inputs on its first run. */
export type HITLText<TArgs, TText extends string | null = string> =
  TText | ((args: TArgs) => TText | Promise<TText>);

/** One form field on the Required Actions page, as Python's `Param` serializes. */
export interface HITLParam {
  /**
   * Pre-filled value. When `responseTimeout` passes and `defaults` is set, the scheduler records
   * each param's `value` as the response, so it is also the value used on timeout.
   */
  readonly value: JsonValue;
  readonly description?: string;
  /**
   * JSON Schema. The form renders `type`, `enum`, `format`, `items`, `minimum`, `maximum`,
   * `minLength`, `maxLength` and `values_display`.
   */
  readonly schema?: Readonly<Record<string, JsonValue>>;
}

/**
 * The options both HITL tasks take.
 *
 * `TSubjectArgs` and `TBodyArgs` are the inputs `subject` and `body` read when they are
 * functions; the task takes both.
 */
interface HITLBaseSpec<TSubjectArgs extends object | void, TBodyArgs extends object | void> {
  /** Headline of the request. */
  readonly subject: HITLText<TSubjectArgs>;
  /** Markdown shown below the headline. */
  readonly body?: HITLText<TBodyArgs, string | null>;
  /** Users allowed to respond. Anyone who can act on the task may respond when unset. */
  readonly assignedUsers?: readonly HITLUser[];
  /** Form fields the responder fills in, by name. Their answers are the result's `paramsInput`. */
  readonly params?: Readonly<Record<string, HITLParam>>;
  /**
   * Whole seconds to wait for a response, since TypeScript has no timedelta. Waits indefinitely
   * when unset. Use this rather than `executionTimeout`, which a HITL task does not enforce.
   */
  readonly responseTimeout?: number;
}

/** The `HITLOperator` options, named as TypeScript spells them. */
export interface HITLSpec<
  TSubjectArgs extends object | void = void,
  TBodyArgs extends object | void = TSubjectArgs,
> extends HITLBaseSpec<TSubjectArgs, TBodyArgs> {
  /** What the responder chooses from. */
  readonly options: readonly string[];
  /** Options chosen for the responder when `responseTimeout` passes without a response. */
  readonly defaults?: readonly string[];
  /** Whether more than one option may be chosen. */
  readonly multiple?: boolean;
}

/** The `ApprovalOperator` options, named as TypeScript spells them. */
export interface ApprovalSpec<
  TSubjectArgs extends object | void = void,
  TBodyArgs extends object | void = TSubjectArgs,
> extends HITLBaseSpec<TSubjectArgs, TBodyArgs> {
  /** The answer given for the responder when `responseTimeout` passes without a response. */
  readonly defaults?: "Approve" | "Reject";
  /**
   * On "Reject", skip every task downstream rather than only the tasks directly downstream.
   * Defaults to false.
   */
  readonly ignoreDownstreamTriggerRules?: boolean;
  /**
   * On "Reject", fail this task rather than skip what follows it. Defaults to false, so a
   * rejection succeeds. A retry asks the reviewer again, so `retries` with this option repeats
   * the request on each attempt.
   */
  readonly failOnReject?: boolean;
}

/**
 * The response a HITL task pushes to XCom and passes downstream.
 *
 * The keys are those of Python's `HITLTriggerEventSuccessPayload` in camelCase, and `respondedAt`
 * is an ISO-8601 string where Python's is a datetime. Airflow's own records of the request and
 * the response keep their snake_case keys.
 */
export type HITLResult = {
  readonly chosenOptions: string[];
  readonly paramsInput: Record<string, JsonValue>;
  /** ISO-8601 UTC instant of the response, to the millisecond. */
  readonly respondedAt: string;
  /** `null` when the defaults were applied on timeout. */
  readonly respondedByUser: HITLUser | null;
  /** Whether the defaults were applied because the response timeout passed. */
  readonly timedout: boolean;
};

/**
 * What `hitl(...)` and `approval(...)` return, to pass to `dag.task(...)`: the options, checked
 * and with their defaults applied.
 */
export interface HITLTask<TArgs extends object | void = void> extends Operator<TArgs, HITLResult> {
  readonly kind: "choice" | "approval";
  readonly subject: HITLText<TArgs>;
  readonly body: HITLText<TArgs, string | null> | undefined;
  /** @internal The `withArgNames` renames of `subject` and `body`, merged. */
  readonly argNames: ReadonlyMap<string, string>;
  readonly options: readonly string[];
  readonly defaults: readonly string[] | undefined;
  readonly multiple: boolean;
  readonly assignedUsers: readonly HITLUser[];
  readonly params: Readonly<Record<string, HITLParam>>;
  readonly responseTimeout: number | undefined;
  /** Only an approval acts on this. */
  readonly ignoreDownstreamTriggerRules: boolean;
  /** Only an approval acts on this. */
  readonly failOnReject: boolean;
}

/** What {@link createHITLTask} builds a task from: a task without its operator members. */
export type HITLTaskFields = Omit<HITLTask<never>, keyof Operator<never, HITLResult>>;

/**
 * The inputs a task takes when its subject reads `TSubject` and its body reads `TBody`: both, or
 * whichever is read. Bracketed, so that `void` is not distributed over.
 */
export type HITLTextArgs<TSubject, TBody> = [TSubject, TBody] extends [void, void]
  ? void
  : [TSubject] extends [void]
    ? TBody
    : [TBody] extends [void]
      ? TSubject
      : TSubject & TBody;

const SHARED_OPTION_NAMES = ["subject", "body", "assignedUsers", "params", "responseTimeout"];
const HITL_OPTION_NAMES: ReadonlySet<string> = new Set([
  ...SHARED_OPTION_NAMES,
  "options",
  "defaults",
  "multiple",
]);
const APPROVAL_OPTION_NAMES: ReadonlySet<string> = new Set([
  ...SHARED_OPTION_NAMES,
  "defaults",
  "ignoreDownstreamTriggerRules",
  "failOnReject",
]);
const PARAM_KEYS: ReadonlySet<string> = new Set(["value", "description", "schema"]);

/**
 * A task that asks a human to choose, passed to `dag.task` in place of a handler.
 *
 * Its first run creates the request and parks the task in `awaiting_input`, freeing the worker;
 * a later run resumes with the response, pushes it to XCom as a {@link HITLResult}, and
 * succeeds. The choice never skips or fails anything by itself.
 *
 * ```ts
 * dag.task("choose_region", hitl({
 *   subject: ({ report }: { report: Report }) => `Pick a region for ${report.version}`,
 *   options: ["us", "eu"],
 * }))({ report });
 * ```
 */
export function hitl<
  TSubjectArgs extends object | void = void,
  TBodyArgs extends object | void = TSubjectArgs,
>(spec: HITLSpec<TSubjectArgs, TBodyArgs>): HITLTask<HITLTextArgs<TSubjectArgs, TBodyArgs>> {
  const given = checkSpec("hitl", spec, HITL_OPTION_NAMES);
  const subject = checkSubject("hitl", given["subject"]);
  const body = checkBody("hitl", given["body"]);
  const options = checkOptions(given["options"]);
  const multiple = checkMultiple(given["multiple"]);
  const defaults = checkDefaults(given["defaults"], options, multiple);

  return createHITLTask({
    kind: "choice",
    subject,
    body,
    argNames: mergeArgNames("hitl", subject, body),
    options,
    defaults,
    multiple,
    assignedUsers: checkAssignedUsers("hitl", given["assignedUsers"]),
    params: checkParams("hitl", given["params"]),
    responseTimeout: checkResponseTimeout("hitl", given["responseTimeout"]),
    ignoreDownstreamTriggerRules: false,
    failOnReject: false,
  });
}

/**
 * A task that asks a human to approve or reject, passed to `dag.task` in place of a handler.
 *
 * Offers exactly "Approve" and "Reject", which is what the Airflow UI shows as an approval. On
 * "Reject" it skips what follows, or fails when {@link ApprovalSpec.failOnReject} is set.
 *
 * ```ts
 * dag.task("sign_off", approval({ subject: "Ship it?" }))().before(publish());
 * ```
 */
export function approval<
  TSubjectArgs extends object | void = void,
  TBodyArgs extends object | void = TSubjectArgs,
>(spec: ApprovalSpec<TSubjectArgs, TBodyArgs>): HITLTask<HITLTextArgs<TSubjectArgs, TBodyArgs>> {
  const given = checkSpec("approval", spec, APPROVAL_OPTION_NAMES);
  const subject = checkSubject("approval", given["subject"]);
  const body = checkBody("approval", given["body"]);

  return createHITLTask({
    kind: "approval",
    subject,
    body,
    argNames: mergeArgNames("approval", subject, body),
    options: [APPROVE, REJECT],
    defaults: checkApprovalDefault(given["defaults"]),
    multiple: false,
    assignedUsers: checkAssignedUsers("approval", given["assignedUsers"]),
    params: checkParams("approval", given["params"]),
    responseTimeout: checkResponseTimeout("approval", given["responseTimeout"]),
    ignoreDownstreamTriggerRules: checkFlag("approval", "ignoreDownstreamTriggerRules", given),
    failOnReject: checkFlag("approval", "failOnReject", given),
  });
}

// One checker per option. Each returns the option's value, or throws naming what is wrong.

function checkSpec(
  factory: string,
  spec: unknown,
  names: ReadonlySet<string>,
): Record<string, unknown> {
  if (!isPlainRecord(spec)) throw new Error(`${factory}(...) takes an options object`);
  for (const name of Object.keys(spec)) {
    if (!names.has(name)) throw new Error(`Unknown option "${name}" for ${factory}(...)`);
  }
  return spec;
}

function checkSubject(factory: string, value: unknown): HITLText<never> {
  if (typeof value === "function") return value as HITLText<never>;
  if (typeof value !== "string") {
    throw new Error(`${factory}(...) needs a "subject": a string, or a function returning one`);
  }
  if (value.length === 0) throw new Error(`${factory}(...) option "subject" cannot be empty`);
  return value;
}

function checkBody(factory: string, value: unknown): HITLText<never, string | null> | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string" || typeof value === "function") {
    return value as HITLText<never, string | null>;
  }
  throw new Error(`${factory}(...) option "body" must be a string, or a function returning one`);
}

/**
 * The `withArgNames` renames of the subject and the body, merged. Both texts read the one set of
 * inputs, so a name mapped to two different wire names has no single answer.
 */
function mergeArgNames(
  factory: string,
  subject: HITLText<never>,
  body: HITLText<never, string | null> | undefined,
): ReadonlyMap<string, string> {
  const subjectNames = typeof subject === "function" ? getArgNames(subject as never) : undefined;
  const bodyNames = typeof body === "function" ? getArgNames(body as never) : undefined;
  const merged = new Map(subjectNames);
  for (const [name, wireName] of bodyNames ?? []) {
    const existing = merged.get(name);
    if (existing !== undefined && existing !== wireName) {
      throw new Error(
        `${factory}(...) maps the input "${name}" to "${existing}" in its subject and to ` +
          `"${wireName}" in its body; use one name for it`,
      );
    }
    merged.set(name, wireName);
  }
  return merged;
}

function checkOptions(value: unknown): readonly string[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error('hitl(...) needs "options": a non-empty array of strings');
  }
  const seen = new Set<string>();
  for (const option of value) {
    if (typeof option !== "string" || option.length === 0) {
      throw new Error('hitl(...) option "options" holds a value that is not a non-empty string');
    }
    if (seen.has(option)) {
      throw new Error(`hitl(...) option "options" lists ${JSON.stringify(option)} twice`);
    }
    seen.add(option);
  }
  return Object.freeze([...seen]);
}

function checkMultiple(value: unknown): boolean {
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new Error('hitl(...) option "multiple" must be a boolean');
}

function checkDefaults(
  value: unknown,
  options: readonly string[],
  multiple: boolean,
): readonly string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error('hitl(...) option "defaults" must be a non-empty array of options');
  }
  const unknown = value.find((option) => !options.includes(option as string));
  if (unknown !== undefined) {
    throw new Error(
      `hitl(...) option "defaults" holds ${JSON.stringify(unknown)}, which is not one ` +
        `of the options ${JSON.stringify(options)}`,
    );
  }
  if (!multiple && value.length > 1) {
    throw new Error(`hitl(...) gives ${value.length} defaults, but "multiple" is not set`);
  }
  return Object.freeze([...(value as string[])]);
}

function checkApprovalDefault(value: unknown): readonly string[] | undefined {
  if (value === undefined) return undefined;
  if (value === APPROVE || value === REJECT) return Object.freeze([value]);
  throw new Error('approval(...) option "defaults" must be "Approve" or "Reject"');
}

function checkFlag(
  factory: string,
  name: "ignoreDownstreamTriggerRules" | "failOnReject",
  given: Record<string, unknown>,
): boolean {
  const value = given[name];
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new Error(`${factory}(...) option "${name}" must be a boolean`);
}

function checkResponseTimeout(factory: string, value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (Number.isInteger(value) && (value as number) > 0) return value as number;
  throw new Error(
    `${factory}(...) option "responseTimeout" must be a positive whole number of seconds`,
  );
}

function checkAssignedUsers(factory: string, value: unknown): readonly HITLUser[] {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value)) {
    throw new Error(`${factory}(...) option "assignedUsers" must be an array of { id, name }`);
  }
  return Object.freeze(value.map((user: unknown) => checkAssignedUser(factory, user)));
}

function checkAssignedUser(factory: string, user: unknown): HITLUser {
  if (isAssignedUser(user)) return Object.freeze({ id: user.id, name: user.name });
  throw new Error(
    `${factory}(...) option "assignedUsers" holds ${JSON.stringify(user)}; each user is ` +
      "{ id, name } with a non-empty string id",
  );
}

/** `{ id, name }` with a non-empty string id, and nothing else. */
function isAssignedUser(user: unknown): user is HITLUser {
  if (!isPlainRecord(user)) return false;
  const { id, name, ...rest } = user;
  return (
    typeof id === "string" &&
    id.length > 0 &&
    typeof name === "string" &&
    Object.keys(rest).length === 0
  );
}

/** `HITLOperator.validate_params`, and a check that each param is one the form can carry. */
function checkParams(factory: string, value: unknown): Readonly<Record<string, HITLParam>> {
  if (value === undefined) return Object.freeze({});
  if (!isPlainRecord(value)) {
    throw new Error(`${factory}(...) option "params" must be an object of { value, ... } by name`);
  }
  if (Object.hasOwn(value, "_options")) {
    throw new Error(`${factory}(...) option "params": "_options" is not allowed in params`);
  }
  const params: Record<string, HITLParam> = {};
  for (const [name, param] of Object.entries(value)) {
    params[name] = checkParam(factory, name, param);
  }
  return Object.freeze(params);
}

function checkParam(factory: string, name: string, param: unknown): HITLParam {
  const label = `${factory}(...) param "${name}"`;
  if (!isPlainRecord(param)) throw new Error(`${label} must be an object of { value, ... }`);
  for (const key of Object.keys(param)) {
    if (!PARAM_KEYS.has(key)) throw new Error(`${label} has an unknown key "${key}"`);
  }
  if (param["value"] === undefined) throw new Error(`${label} needs a "value"`);
  const { description, schema } = param;
  if (description !== undefined && typeof description !== "string") {
    throw new Error(`${label} key "description" must be a string`);
  }
  if (schema !== undefined && !isPlainRecord(schema)) {
    throw new Error(`${label} key "schema" must be a JSON Schema object`);
  }
  return Object.freeze({
    value: toPlainJson(param["value"], `${label} key "value"`),
    ...(description !== undefined && { description }),
    ...(schema !== undefined && {
      schema: toPlainJson(schema, `${label} key "schema"`) as Record<string, JsonValue>,
    }),
  });
}
