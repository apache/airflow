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

import type { ConnectionResult, GetXComOpts, SetXComOpts } from "./client-types.js";

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
   * @throws {@link VariableNotFoundError} when the key is missing.
   */
  getVariableOrThrow(key: string): Promise<string>;

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
   * Look up an Airflow Connection by ID.
   *
   * Returns `null` when the connection does not exist. Throws on any other
   * error.
   */
  getConnection(connId: string): Promise<ConnectionResult | null>;
}

/** Error thrown by {@link TaskClient.getVariableOrThrow}. */
export class VariableNotFoundError extends Error {
  constructor(public readonly key: string) {
    super(`Variable not found: ${key}`);
    this.name = "VariableNotFoundError";
  }
}
