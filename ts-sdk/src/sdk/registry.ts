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

// Mirrors the Python task-SDK KEY_REGEX and validate_key in airflow.sdk.definitions._internal.node.
const KEY_REGEX = /^[\p{L}\p{N}_.-]+$/u;
const MAX_KEY_LENGTH = 250;

function validateKey(name: string, value: string): void {
  if (typeof value !== "string" || !KEY_REGEX.test(value)) {
    throw new Error(
      `${name} must be made of alphanumeric characters, dashes, dots, and underscores`,
    );
  }
  if (value.length > MAX_KEY_LENGTH) {
    throw new Error(`${name} must be less than ${MAX_KEY_LENGTH} characters, not ${value.length}`);
  }
}

/** Identifies the Airflow task handled by a TypeScript function. */
export interface TaskRegistration {
  /** Identifier of the Dag containing this task. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
}

/** Registry of TypeScript task handlers keyed by Dag ID and task ID. */
export class TaskRegistry {
  readonly #tasks = new Map<string, Map<string, TaskHandler>>();

  /**
   * Register a TypeScript handler for an Airflow task.
   *
   * `dagId` must match the Python Dag's `dag_id`. `taskId` must match the
   * Dag-side operator's `task_id` exactly, including any TaskGroup prefix.
   */
  register<TReturn = unknown>(registration: TaskRegistration, handler: TaskHandler<TReturn>): void {
    const { dagId, taskId } = registration;
    validateKey("dagId", dagId);
    validateKey("taskId", taskId);
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${dagId}" task "${taskId}" must be a function`);
    }
    const dagTasks = this.#tasks.get(dagId) ?? new Map<string, TaskHandler>();
    if (dagTasks.has(taskId)) {
      throw new Error(`Task "${taskId}" is already registered for Dag "${dagId}"`);
    }
    dagTasks.set(taskId, handler as TaskHandler);
    this.#tasks.set(dagId, dagTasks);
  }

  /** Look up a registered handler. Returns `undefined` when no handler exists. */
  get(dagId: string, taskId: string): TaskHandler | undefined {
    return this.#tasks.get(dagId)?.get(taskId);
  }

  /** List all registered tasks. */
  list(): TaskRegistration[] {
    return [...this.#tasks.entries()].flatMap(([dagId, tasks]) =>
      [...tasks.keys()].map((taskId) => ({ dagId, taskId })),
    );
  }
}

const defaultRegistry = new TaskRegistry();

/** Register a TypeScript handler in the default task registry. */
export function registerTask<TReturn = unknown>(
  registration: TaskRegistration,
  handler: TaskHandler<TReturn>,
): void {
  defaultRegistry.register(registration, handler);
}

/** Look up a registered handler. Returns `undefined` when no handler exists. */
export function getRegisteredTask(dagId: string, taskId: string): TaskHandler | undefined {
  return defaultRegistry.get(dagId, taskId);
}

/** List all registered tasks. */
export function listRegisteredTasks(): TaskRegistration[] {
  return defaultRegistry.list();
}
