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

import type {
  ConnectionResult,
  GetXComOpts,
  JsonValue,
  SetXComOpts,
  TaskStateStoreSetOpts,
} from "./client-types.js";

/**
 * Client for reading and writing Airflow task-time data from a task handler.
 *
 * The active runtime selects the concrete transport and implements this
 * interface for the current task attempt.
 */
export interface TaskClient {
  /**
   * Look up an Airflow Variable.
   *
   * Returns `null` when the key is missing or stored with a null value.
   * Throws on any other error.
   *
   * This is intentionally JS-friendly behavior. Use
   * {@link getVariableOrThrow} when missing variables should raise.
   */
  getVariable(key: string): Promise<string | null>;

  /**
   * Look up an Airflow Variable and raise when it is missing.
   *
   * This matches Python `Variable.get` behavior when no default value is
   * supplied.
   *
   * @throws {@link Exceptions!VariableNotFoundError | VariableNotFoundError} when the key is missing.
   */
  getVariableOrThrow(key: string): Promise<string>;

  /**
   * Store an Airflow Variable, replacing any existing value.
   *
   * The value is stored as a string. Serialize structured data (for example
   * with `JSON.stringify`) before storing it.
   *
   * Omitting `description` clears the description the Variable had.
   */
  setVariable(key: string, value: string, description?: string | null): Promise<void>;

  /**
   * Delete an Airflow Variable.
   *
   * Resolves even when the key does not exist — the Execution API's delete
   * route is idempotent and does not report a missing key as an error.
   */
  deleteVariable(key: string): Promise<void>;

  /**
   * Pull an XCom value.
   *
   * Returns `null` when the row is missing. Locator fields default to the
   * current task's context.
   *
   * The generic `T` lets callers narrow the return type when the shape is
   * known:
   *
   * ```ts
   * const data = await client.getXCom<{ count: number }>({ key: "result" });
   * // data is { count: number } | null
   * ```
   */
  getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null>;

  /**
   * Push an XCom value.
   *
   * Target fields default to the current task's context.
   */
  setXCom(opts: SetXComOpts): Promise<void>;

  /**
   * Key/value state private to this task instance.
   *
   * See {@link TaskStateStore}.
   */
  readonly taskStateStore: TaskStateStore;

  /**
   * Look up an Airflow Connection by ID.
   *
   * Returns `null` when the connection does not exist. Throws on any other
   * error.
   *
   * This is intentionally JS-friendly behavior. Use
   * {@link getConnectionOrThrow} when missing connections should raise.
   */
  getConnection(connId: string): Promise<ConnectionResult | null>;

  /**
   * Look up an Airflow Connection by ID and raise when it is missing.
   *
   * This matches Python `BaseHook.get_connection` behavior.
   *
   * @throws {@link Exceptions!ConnectionNotFoundError | ConnectionNotFoundError} when the connection does not exist.
   */
  getConnectionOrThrow(connId: string): Promise<ConnectionResult>;
}

/**
 * Key/value state private to one task instance, kept across its retries within
 * the same Dag run.
 *
 * Reach it through {@link TaskClient.taskStateStore}:
 *
 * ```ts
 * const store = getClient().taskStateStore;
 * let jobId = await store.get<string>("job_id");
 * if (jobId == null) {
 *   jobId = await submit();
 *   await store.set("job_id", jobId, { retentionMs: NEVER_EXPIRE });
 * }
 * ```
 *
 * Values are stored in the metadata database as-is; the `[workers]
 * state_store_backend` that Python tasks can configure is not applied here.
 */
export interface TaskStateStore {
  /**
   * Look up a value.
   *
   * Returns `null` when the key is missing. Throws on any other error.
   *
   * The generic `T` lets callers narrow the return type when the shape is
   * known.
   *
   * @throws `TypeError` when `key` is not a string.
   * @throws `RangeError` when `key` is empty.
   */
  get<T = unknown>(key: string): Promise<T | null>;

  /**
   * Store a value, replacing any existing value for the key.
   *
   * @throws `TypeError` when `key` is not a string, or `value` is null,
   * undefined, NaN, or Infinity.
   * @throws `RangeError` when `key` is empty; when `retentionMs` is
   * negative, NaN, or Infinity; when the deployment's
   * `[state_store] default_retention_days` is invalid; or when the resolved
   * expiry would overflow the wire timestamp (year 9999).
   */
  set(key: string, value: NonNullable<JsonValue>, opts?: TaskStateStoreSetOpts): Promise<void>;

  /**
   * Delete a key.
   *
   * Resolves even when the key does not exist — the Execution API's delete
   * route is idempotent and does not report a missing key as an error.
   *
   * @throws `TypeError` when `key` is not a string.
   * @throws `RangeError` when `key` is empty.
   */
  delete(key: string): Promise<void>;

  /** Delete every key stored for this task instance. */
  clear(): Promise<void>;
}

/** Error thrown by {@link TaskClient.getVariableOrThrow}. */
export class VariableNotFoundError extends Error {
  constructor(public readonly key: string) {
    super(`Variable not found: ${key}`);
    this.name = "VariableNotFoundError";
  }
}

/** Error thrown by {@link TaskClient.getConnectionOrThrow}. */
export class ConnectionNotFoundError extends Error {
  constructor(public readonly connId: string) {
    super(`Connection not found: ${connId}`);
    this.name = "ConnectionNotFoundError";
  }
}
