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

/** JSON-compatible value accepted by Airflow for XCom payloads. */
export type JsonValue =
  string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

/**
 * Options for pulling an XCom value.
 *
 * `dagId` and `runId` default to the running task's context. Pass them only
 * when pulling an XCom value from another Dag or run.
 */
export interface GetXComOpts {
  /** XCom key to read; defaults to `"return_value"`. */
  key?: string;
  dagId?: string;
  runId?: string;
  /** Task that pushed the XCom value. */
  taskId: string;
  /**
   * Map index of the task instance to read, or `null` (the default) to read
   * the task's result.
   *
   * With `null`, what is read depends on whether the task is mapped, as its
   * definition in the Dag says, not on how many instances it ran:
   *
   * - For a task that is not mapped, its value.
   * - For a mapped task, the values of all its instances as an array ordered
   *   by map index. Instances that pushed nothing are left out. If none
   *   pushed a value, including when the task expanded to no instances, the
   *   result is an empty array, or `null` on Airflow 3.4.
   *
   * A map index reads that one instance's value. One that selects no task
   * instance of the task, such as any map index of a task that is not
   * mapped, reads `null`.
   */
  mapIndex?: number | null;
  includePriorDates?: boolean;
}

/**
 * Options for pushing an XCom value.
 *
 * `dagId`, `taskId`, and `runId` default to the running task's context.
 */
export interface SetXComOpts {
  /** XCom key to write; defaults to `"return_value"`. */
  key?: string;
  value: JsonValue;
  dagId?: string;
  runId?: string;
  taskId?: string;
  mapIndex?: number | null;
}

/**
 * Pass as {@link TaskStateStoreSetOpts.retention} to store a task state
 * store key with no expiry, regardless of the deployment default.
 *
 * A symbol rather than a number, so a retention that reaches `Infinity` by
 * accident (a division by zero, for example) is rejected instead of silently
 * storing a key forever.
 */
export const NEVER_EXPIRE: unique symbol = Symbol("@apache-airflow/ts-sdk/NEVER_EXPIRE");

/** Options for a task state store write. */
export interface TaskStateStoreSetOpts {
  /**
   * How long to keep the key, in seconds, counted from the write; `0` expires
   * it immediately.
   *
   * Omitting it follows the deployment's `[state_store] default_retention_days`,
   * which the coordinator passes to the runtime. Pass {@link NEVER_EXPIRE} to
   * store the key with no expiry regardless of that default.
   */
  retention?: number | typeof NEVER_EXPIRE;
}

/** Airflow Connection details returned by a task client. */
export interface ConnectionResult {
  id: string;
  type: string;
  host?: string | null;
  schema?: string | null;
  login?: string | null;
  password?: string | null;
  port?: number | null;
  extra?: string | null;
}
