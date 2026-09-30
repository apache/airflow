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

// The answer to a TaskHandlerParseRequest: the task handlers this bundle
// registers for the Dags a Python file declares, as the Dag processor checks
// them against that file's `@task.stub` calls.

import type { TaskHandlerDeclaration, TaskHandlerParseRequest } from "../generated/supervisor.js";
import type { RuntimeTaskHandlerParsingResult } from "./protocol.js";
import { getArgNames } from "../sdk/arg-names.js";
import { bundleTaskHandlers, type Bundle } from "../sdk/bundle.js";
import type { TaskFunction } from "../sdk/task.js";

/**
 * Declare the task handlers registered for the requested Dags, each Dag's in
 * registration order. A requested Dag with no handler is left out rather than
 * mapped to an empty list, and a Dag declared in TypeScript is never declared.
 * No handler runs.
 *
 * @throws when the request's `dag_ids` is not a list of strings.
 */
export function declareTaskHandlers(
  bundle: Bundle,
  request: Pick<TaskHandlerParseRequest, "file" | "dag_ids">,
): RuntimeTaskHandlerParsingResult {
  const requested = new Set(readDagIds(request));
  const declared: [string, TaskHandlerDeclaration[]][] = [];
  for (const [dagId, handlers] of bundleTaskHandlers(bundle)) {
    if (!requested.has(dagId)) continue;
    declared.push([
      dagId,
      [...handlers].map(([taskId, handler]) => declareTaskHandler(taskId, handler)),
    ]);
  }
  return {
    type: "TaskHandlerParsingResult",
    fileloc: request.file,
    // Defined rather than assigned, so a Dag named `__proto__` stays a key.
    task_handlers: Object.fromEntries(declared),
  };
}

/**
 * Types are erased, so a handler's `withArgNames` renames are the only names
 * the SDK knows. Every other argument reaches the handler by folding, which is
 * why the list is open.
 */
function declareTaskHandler(taskId: string, handler: TaskFunction): TaskHandlerDeclaration {
  const names = new Set(getArgNames(handler).values());
  return {
    task_id: taskId,
    binding: "named_open",
    params: [...names].map((name) => ({
      name,
      value_schema: null,
      // A name the call did not pass reads as undefined, and the task still runs.
      required: false,
      // A rename never falls back to folding.
      exact_name: true,
    })),
  };
}

function readDagIds(request: Pick<TaskHandlerParseRequest, "dag_ids">): string[] {
  // Checked, or a request this SDK cannot read would be answered with no handlers at all.
  const dagIds: unknown = request.dag_ids;
  if (!Array.isArray(dagIds) || !dagIds.every((dagId) => typeof dagId === "string")) {
    throw new Error(
      `TaskHandlerParseRequest.dag_ids must be a list of strings, got ${JSON.stringify(dagIds)}`,
    );
  }
  return dagIds as string[];
}
