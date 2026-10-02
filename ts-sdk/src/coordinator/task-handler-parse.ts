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

// The answer to a TaskHandlerParseRequest: every task handler this bundle
// registers, as the Dag processor checks them against a Python file's
// `@task.stub` calls.

import type { TaskHandlerDeclaration, TaskHandlerParseRequest } from "../generated/supervisor.js";
import type { RuntimeTaskHandlerParsingResult } from "./protocol.js";
import { bundleTaskHandlers, type Bundle } from "../sdk/bundle.js";

/**
 * Declare every registered task handler, keyed by Dag id, each Dag's in
 * registration order. A Dag declared in TypeScript is never declared. No
 * handler runs.
 */
export function declareTaskHandlers(
  bundle: Bundle,
  request: Pick<TaskHandlerParseRequest, "file">,
): RuntimeTaskHandlerParsingResult {
  const declared: [string, TaskHandlerDeclaration[]][] = [];
  for (const [dagId, handlers] of bundleTaskHandlers(bundle)) {
    declared.push([dagId, [...handlers.keys()].map((taskId) => declareTaskHandler(taskId))]);
  }
  return {
    type: "TaskHandlerParsingResult",
    fileloc: request.file,
    // Defined rather than assigned, so a Dag named `__proto__` stays a key.
    task_handlers: Object.fromEntries(declared),
  };
}

/**
 * Types are erased, so the SDK cannot list the params a handler takes, and the
 * Dag processor checks only that the handler exists.
 */
function declareTaskHandler(taskId: string): TaskHandlerDeclaration {
  return { task_id: taskId, binding: "named", params: null };
}
