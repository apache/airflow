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

import type { TaskHandler } from "./task.js";

const registry = new Map<string, TaskHandler>();

/**
 * Register a TypeScript handler for an Airflow task.
 *
 * `taskId` must match the Dag-side operator's `task_id` exactly, including
 * any TaskGroup prefix.
 */
export function registerTask<TReturn = unknown>(
  taskId: string,
  handler: TaskHandler<TReturn>,
): void {
  if (!taskId || typeof taskId !== "string") {
    throw new Error("registerTask: taskId must be a non-empty string");
  }
  if (typeof handler !== "function") {
    throw new Error(`registerTask: handler for "${taskId}" must be a function`);
  }
  if (registry.has(taskId)) {
    throw new Error(`Task "${taskId}" is already registered`);
  }
  registry.set(taskId, handler as TaskHandler);
}

/** Look up a registered handler. Returns `undefined` when no handler exists. */
export function getRegisteredTask(taskId: string): TaskHandler | undefined {
  return registry.get(taskId);
}

/** List all registered task IDs. */
export function listRegisteredTasks(): string[] {
  return [...registry.keys()];
}

/** Clear the registry. Primarily for testing. */
export function clearRegistry(): void {
  registry.clear();
}
