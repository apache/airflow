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

// The task-handler call surface — types every user task handler sees.

import type { TaskClient } from "./client.js";

/** Runtime metadata for the current task invocation. */
export interface TaskContext {
  /** Identifier of the Dag containing this task. */
  readonly dagId: string;
  /** Task ID for this handler invocation, including any TaskGroup prefix. */
  readonly taskId: string;
  /** Dag run identifier for the current task attempt. */
  readonly runId: string;
  /** Airflow try number for the current task attempt. */
  readonly tryNumber: number;
  /** -1 for non-mapped tasks, 0..N-1 for mapped instances. */
  readonly mapIndex: number;
  /**
   * AbortSignal that fires when Airflow terminates the task subprocess
   * with SIGTERM or SIGINT.
   *
   * Pass this signal to `fetch()`, timers, or other abortable APIs for
   * cooperative cancellation and cleanup.
   */
  readonly signal: AbortSignal;
}

/** Arguments passed to every task handler. */
export interface TaskHandlerArgs {
  /** Runtime metadata for the current task invocation. */
  readonly ctx: TaskContext;
  /** Client for reading and writing Airflow task-time data. */
  readonly client: TaskClient;
}

/**
 * Function signature for a TypeScript task handler.
 *
 * Non-`undefined` return values are automatically pushed to XCom under
 * the `"return_value"` key, matching Python `@task` behavior. Return
 * `undefined` or omit a return value to skip the automatic XCom push.
 */
export type TaskHandler<TReturn = unknown> = (args: TaskHandlerArgs) => TReturn | Promise<TReturn>;
