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

// Human-in-the-loop tasks: `humanInput(...)` and `approval(...)`, which mirror
// `HITLOperator` and `ApprovalOperator` in the standard provider.

import { brand, hasBrand } from "./brand.js";
import { isPlainRecord } from "./dag.js";
import type { JsonValue } from "./client-types.js";

/** A user allowed to respond, as Airflow's auth manager identifies them. */
export interface HITLUser {
  readonly id: string;
  readonly name: string;
}

/** Text shown to the responder: fixed, or computed from the task's inputs on its first run. */
export type HumanInputText<TArgs, TText extends string | null = string> =
  TText | ((args: TArgs) => TText | Promise<TText>);

/** The options both human-input tasks take. */
interface HumanInputBaseSpec<TArgs extends object | void> {
  /** Headline of the request. */
  readonly subject: HumanInputText<TArgs>;
  /** Markdown shown below the headline. */
  readonly body?: HumanInputText<TArgs, string | null>;
  /** Users allowed to respond. Anyone who can act on the task may respond when unset. */
  readonly assignees?: readonly HITLUser[];
  /** Whole seconds to wait for a response. Waits indefinitely when unset. */
  readonly responseTimeout?: number;
}

/** The `HITLOperator` options, named as TypeScript spells them. */
export interface HumanInputSpec<
  TArgs extends object | void = void,
> extends HumanInputBaseSpec<TArgs> {
  /** What the responder chooses from. */
  readonly options: readonly string[];
  /** Options chosen for the responder when `responseTimeout` passes without a response. */
  readonly defaults?: readonly string[];
  /** Whether more than one option may be chosen. */
  readonly multiple?: boolean;
}

/** What an approval does when the responder rejects. */
export type OnReject = "skip" | "skipAll" | "fail";

/** The `ApprovalOperator` options, named as TypeScript spells them. */
export interface ApprovalSpec<
  TArgs extends object | void = void,
> extends HumanInputBaseSpec<TArgs> {
  /** The answer given for the responder when `responseTimeout` passes without a response. */
  readonly defaults?: "Approve" | "Reject";
  /**
   * On "Reject": skip the tasks directly downstream (`"skip"`, the default), skip every task
   * downstream (`"skipAll"`, Python's `ignore_downstream_trigger_rules`), or fail this task
   * (`"fail"`, Python's `fail_on_reject`). The task itself succeeds unless this is `"fail"`.
   */
  readonly onReject?: OnReject;
}

/**
 * The response a human-input task pushes to XCom and passes downstream.
 *
 * The keys are Python's (`HITLTriggerEventSuccessPayload`), so the value reads the same from any
 * language that pulls it.
 */
export interface HumanInputResult {
  readonly chosen_options: string[];
  readonly params_input: Record<string, JsonValue>;
  /** ISO-8601 UTC instant of the response, to the millisecond. */
  readonly responded_at: string;
  /** `null` when the defaults were applied on timeout. */
  readonly responded_by_user: HITLUser | null;
  /** Whether the defaults were applied because the response timeout passed. */
  readonly timedout: boolean;
}

/** What `humanInput(...)` and `approval(...)` return, to pass to `dag.task(...)`: the options,
 *  checked and with their defaults applied. */
export interface HumanInputTask<TArgs extends object | void = void> {
  readonly kind: "choice" | "approval";
  readonly subject: HumanInputText<TArgs>;
  readonly body: HumanInputText<TArgs, string | null> | undefined;
  readonly options: readonly string[];
  readonly defaults: readonly string[] | undefined;
  readonly multiple: boolean;
  readonly assignees: readonly HITLUser[];
  readonly responseTimeout: number | undefined;
  /** Set only for an approval. */
  readonly onReject: OnReject | undefined;
}

export const APPROVE = "Approve";
export const REJECT = "Reject";

/** Internal: whether `value` is a human-input task built by any copy of this package. */
export function isHumanInputTask(value: unknown): value is HumanInputTask<never> {
  return hasBrand(value, "HumanInputTask");
}

const SHARED_OPTION_NAMES = ["subject", "body", "defaults", "assignees", "responseTimeout"];
const CHOICE_OPTION_NAMES: ReadonlySet<string> = new Set([
  ...SHARED_OPTION_NAMES,
  "options",
  "multiple",
]);
const APPROVAL_OPTION_NAMES: ReadonlySet<string> = new Set([...SHARED_OPTION_NAMES, "onReject"]);
const ON_REJECT: ReadonlySet<string> = new Set<OnReject>(["skip", "skipAll", "fail"]);

/**
 * A task that asks a human to choose, passed to `dag.task` in place of a handler.
 *
 * Its first run creates the request and parks the task in `awaiting_input`, freeing the worker;
 * a later run resumes with the response, pushes it to XCom as a {@link HumanInputResult}, and
 * succeeds. The choice never skips or fails anything by itself.
 *
 * ```ts
 * dag.task("choose_region", humanInput({
 *   subject: ({ report }: { report: Report }) => `Pick a region for ${report.version}`,
 *   options: ["us", "eu"],
 * }))({ report });
 * ```
 */
export function humanInput<TArgs extends object | void = void>(
  spec: HumanInputSpec<TArgs>,
): HumanInputTask<TArgs> {
  const given = checkSpec("humanInput", spec, CHOICE_OPTION_NAMES);
  const subject = checkSubject("humanInput", given["subject"]);
  const body = checkBody("humanInput", given["body"]);
  const options = checkOptions(given["options"]);
  const multiple = checkMultiple(given["multiple"]);
  const defaults = checkDefaults(given["defaults"], options, multiple);
  const assignees = checkAssignees("humanInput", given["assignees"]);
  const responseTimeout = checkResponseTimeout("humanInput", given["responseTimeout"]);

  return seal<TArgs>({
    kind: "choice",
    subject,
    body,
    options,
    defaults,
    multiple,
    assignees,
    responseTimeout,
    onReject: undefined,
  });
}

/**
 * A task that asks a human to approve or reject, passed to `dag.task` in place of a handler.
 *
 * Offers exactly "Approve" and "Reject", which is what the Airflow UI shows as an approval. On
 * "Reject" it applies {@link ApprovalSpec.onReject}.
 *
 * ```ts
 * dag.task("sign_off", approval({ subject: "Ship it?" }))().before(publish());
 * ```
 */
export function approval<TArgs extends object | void = void>(
  spec: ApprovalSpec<TArgs>,
): HumanInputTask<TArgs> {
  const given = checkSpec("approval", spec, APPROVAL_OPTION_NAMES);
  const subject = checkSubject("approval", given["subject"]);
  const body = checkBody("approval", given["body"]);
  const defaults = checkApprovalDefault(given["defaults"]);
  const assignees = checkAssignees("approval", given["assignees"]);
  const responseTimeout = checkResponseTimeout("approval", given["responseTimeout"]);
  const onReject = checkOnReject(given["onReject"]);

  return seal<TArgs>({
    kind: "approval",
    subject,
    body,
    options: [APPROVE, REJECT],
    defaults,
    multiple: false,
    assignees,
    responseTimeout,
    onReject,
  });
}

function seal<TArgs extends object | void>(task: HumanInputTask<never>): HumanInputTask<TArgs> {
  brand(task, "HumanInputTask");
  return Object.freeze(task) as HumanInputTask<TArgs>;
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

function checkSubject(factory: string, value: unknown): HumanInputText<never> {
  if (typeof value === "function") return value as HumanInputText<never>;
  if (typeof value !== "string") {
    throw new Error(`${factory}(...) needs a "subject": a string, or a function returning one`);
  }
  if (value.length === 0) throw new Error(`${factory}(...) option "subject" cannot be empty`);
  return value;
}

function checkBody(
  factory: string,
  value: unknown,
): HumanInputText<never, string | null> | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string" || typeof value === "function") {
    return value as HumanInputText<never, string | null>;
  }
  throw new Error(`${factory}(...) option "body" must be a string, or a function returning one`);
}

function checkOptions(value: unknown): readonly string[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error('humanInput(...) needs "options": a non-empty array of strings');
  }
  const seen = new Set<string>();
  for (const option of value) {
    if (typeof option !== "string" || option.length === 0) {
      throw new Error(
        'humanInput(...) option "options" holds a value that is not a non-empty string',
      );
    }
    if (seen.has(option)) {
      throw new Error(`humanInput(...) option "options" lists ${JSON.stringify(option)} twice`);
    }
    seen.add(option);
  }
  return Object.freeze([...seen]);
}

function checkMultiple(value: unknown): boolean {
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new Error('humanInput(...) option "multiple" must be a boolean');
}

function checkDefaults(
  value: unknown,
  options: readonly string[],
  multiple: boolean,
): readonly string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error('humanInput(...) option "defaults" must be a non-empty array of options');
  }
  const unknown = value.find((option) => !options.includes(option as string));
  if (unknown !== undefined) {
    throw new Error(
      `humanInput(...) option "defaults" holds ${JSON.stringify(unknown)}, which is not one ` +
        `of the options ${JSON.stringify(options)}`,
    );
  }
  if (!multiple && value.length > 1) {
    throw new Error(`humanInput(...) gives ${value.length} defaults, but "multiple" is not set`);
  }
  return Object.freeze([...(value as string[])]);
}

function checkApprovalDefault(value: unknown): readonly string[] | undefined {
  if (value === undefined) return undefined;
  if (value === APPROVE || value === REJECT) return Object.freeze([value]);
  throw new Error('approval(...) option "defaults" must be "Approve" or "Reject"');
}

function checkOnReject(value: unknown): OnReject {
  if (value === undefined) return "skip";
  if (typeof value === "string" && ON_REJECT.has(value)) return value as OnReject;
  throw new Error(
    `approval(...) option "onReject" holds ${JSON.stringify(value)}; use one of ` +
      [...ON_REJECT].map((onReject) => `"${onReject}"`).join(", "),
  );
}

function checkResponseTimeout(factory: string, value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (Number.isInteger(value) && (value as number) > 0) return value as number;
  throw new Error(
    `${factory}(...) option "responseTimeout" must be a positive whole number of seconds`,
  );
}

function checkAssignees(factory: string, value: unknown): readonly HITLUser[] {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value)) {
    throw new Error(`${factory}(...) option "assignees" must be an array of { id, name }`);
  }
  return Object.freeze(value.map((user: unknown) => checkAssignee(factory, user)));
}

function checkAssignee(factory: string, user: unknown): HITLUser {
  if (isAssignee(user)) return Object.freeze({ id: user.id, name: user.name });
  throw new Error(
    `${factory}(...) option "assignees" holds ${JSON.stringify(user)}; each assignee is ` +
      "{ id, name } with a non-empty string id",
  );
}

/** `{ id, name }` with a non-empty string id, and nothing else. */
function isAssignee(user: unknown): user is HITLUser {
  if (!isPlainRecord(user)) return false;
  const { id, name, ...rest } = user;
  return (
    typeof id === "string" &&
    id.length > 0 &&
    typeof name === "string" &&
    Object.keys(rest).length === 0
  );
}
