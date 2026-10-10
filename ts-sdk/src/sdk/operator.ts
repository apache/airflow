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

// Internal: what an operator the SDK runs itself implements, so the Dag, the bundle, the
// serializer and the runtime treat every operator alike and a new one is a single file.
//
// The coordinator types are imported as types only: the authoring layer must not load the
// coordinator at run time.

import type { CoordinatorClient } from "../coordinator/client.js";
import type { LogChannel } from "../coordinator/log-channel.js";
import type {
  RuntimeAwaitInputTask,
  RuntimeDeferTask,
  RuntimeRetryTask,
  RuntimeSucceedTask,
  RuntimeTaskState,
  StartupDetails,
} from "../coordinator/protocol.js";
import { brand, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";
import type { Dag } from "./dag.js";
import type { TaskContext } from "./task.js";

/** What an operator may end the task with. */
export type OperatorOutcome =
  | RuntimeSucceedTask
  | RuntimeRetryTask
  | RuntimeTaskState
  | RuntimeDeferTask
  | RuntimeAwaitInputTask;

/** What the SDK hands an operator at run time. Operators only read it. */
export interface OperatorContext {
  readonly details: StartupDetails;
  readonly dag: Dag;
  readonly ctx: TaskContext;
  readonly client: CoordinatorClient;
  readonly logs: LogChannel;
  /** Bind the task's declared inputs, applying the `withArgNames` renames of `argNames`. */
  resolveArgs(argNames?: ReadonlyMap<string, string>): Promise<object>;
  /** Push `value` under `return_value` when it is given, then succeed. */
  succeed(value?: JsonValue): Promise<RuntimeSucceedTask>;
  /** Fail, or hand the task back for retry when it has retries left. */
  fail(message: string): RuntimeRetryTask | RuntimeTaskState;
  /** Skip `taskIds`, as `SkipMixin.skip` does. */
  skip(taskIds: readonly string[]): Promise<void>;
  /** Park in `awaiting_input`. The next run calls `executeComplete`. */
  awaitInput(opts?: {
    timeoutSeconds?: number;
    kwargs?: Record<string, JsonValue>;
  }): RuntimeAwaitInputTask;
  /**
   * Defer to a Python trigger. The next run calls `executeComplete`. Without a `queue`, the
   * trigger gets the task's queue when triggerer queues are enabled.
   */
  defer(opts: {
    classpath: string;
    kwargs: Record<string, JsonValue>;
    timeoutSeconds?: number;
    queue?: string | null;
  }): RuntimeDeferTask;
}

/** A Dag a task depends on, which the UI draws as an edge out of the task's own Dag. */
export interface OperatorDagDependency {
  readonly target: string;
  /** Airflow's `dependency_type`, such as `"trigger"`. */
  readonly dependencyType: string;
}

// Carries an operator's argument and result types without carrying a value, as `TaskRef` does
// for its return type. The argument is a parameter so that an operator of any argument type is
// an `Operator<never, unknown>`.
declare const OPERATOR_TYPES: unique symbol;

/**
 * An operator the SDK runs itself, passed to `dag.task` in place of a handler.
 *
 * `TArgs` is what the task's factory is called with, and `TResult` what it returns.
 */
export interface Operator<TArgs extends object | void = void, TResult = unknown> {
  /** @internal Never set; see {@link OPERATOR_TYPES}. */
  readonly [OPERATOR_TYPES]?: (args: TArgs) => TResult;
  /** @internal `_operator_name` the UI shows: the Python operator this mirrors. */
  readonly operatorName: string;
  /** @internal Whether it may skip its downstream, as `SkipMixin` marks a Python operator. */
  readonly canSkipDownstream?: boolean;
  /** @internal An operator has no handler name to take a task id from, so `dag.task` requires one. */
  readonly requiresTaskId?: boolean;
  /** @internal How `dag.task` names the operator's tasks in its errors. Defaults to `operatorName`. */
  readonly label?: string;
  /** @internal A call that names the task, which `dag.task` shows when `requiresTaskId` is not met. */
  readonly taskIdExample?: string;
  /** @internal Whether the task's factory must be called without inputs. */
  readonly takesNoInputs?: boolean;
  /**
   * @internal The option that bounds how long it waits, which `dag.task` names when it rejects an
   * `executionTimeout`: the operator does not enforce that one.
   */
  readonly executionTimeoutAlternative?: string;
  /**
   * @internal Extra fields merged into the task's serialized record. `label` names the task, for an error
   * about a value it holds.
   */
  serialize?(label: string): Record<string, JsonValue>;
  /** @internal The Dags this task depends on. */
  getDagDependencies?(): readonly OperatorDagDependency[];
  /** @internal The task's first run. */
  execute(op: OperatorContext): Promise<OperatorOutcome>;
  /** @internal The run Airflow resumes with, after `awaitInput` or `defer`. */
  executeComplete?(op: OperatorContext, event: unknown): Promise<OperatorOutcome>;
}

// The operators this copy of the package built. The brand is shared by every copy, but another
// copy's operator cannot run here: on Reject it would read this copy's Dag private state.
const OWN_OPERATORS = new WeakSet<object>();

/** Internal: whether `value` is an operator built by any copy of this package. */
export function isOperator(value: unknown): value is Operator<never, unknown> {
  return hasBrand(value, "Operator");
}

/** Internal: whether `op` was built by this copy of the package. */
export function isOwnOperator(op: Operator<never, unknown>): boolean {
  return OWN_OPERATORS.has(op);
}

/** Internal: mark `op` as an operator and freeze it, as the factories that build one do. */
export function brandOperator<T extends Operator<never, unknown>>(op: T): T {
  brand(op, "Operator");
  OWN_OPERATORS.add(op);
  return Object.freeze(op);
}
