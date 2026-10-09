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

import type { Asset, AssetRef } from "./asset.js";
import type { ConnectionResult, GetXComOpts, JsonValue, SetXComOpts } from "./client-types.js";

/**
 * Key-value state scoped to one asset, from
 * {@link AssetStateStores.forAsset | forAsset}.
 *
 * State outlives the task and the Dag run that wrote it: it is shared by every
 * task that addresses the same asset, and stays until a task deletes it or the
 * asset is no longer active. There is no retention option.
 *
 * Values go straight to the supervisor. A Python worker-side
 * `[workers] state_store_backend` is not loaded, so values are stored in the
 * metadata database as they are.
 */
export interface AssetStateStore {
  /**
   * Look up a value.
   *
   * Returns `null` when the key is missing, and also when the asset itself is
   * unknown: the supervisor reports both the same way.
   */
  get<T = unknown>(key: string): Promise<T | null>;

  /**
   * Store a JSON-compatible value, replacing any existing value for the key.
   *
   * @throws {@link TypeError} when `value` is null or undefined.
   */
  set(key: string, value: NonNullable<JsonValue>): Promise<void>;

  /** Delete a key. Resolves when the key does not exist. */
  delete(key: string): Promise<void>;

  /** Delete every key of this asset. */
  clear(): Promise<void>;
}

/** Entry point to asset-scoped state, as {@link TaskClient.assetStateStore}. */
export interface AssetStateStores {
  /**
   * Bind a store to one asset.
   *
   * An {@link Asset} and `Asset.ref({ name })` address the asset by name;
   * `Asset.ref({ uri })` addresses it by URI. The runtime does not see the
   * task's inlets and outlets, so nothing checks that the task declares the
   * asset; the Python Dag must declare it for the asset to stay active.
   *
   * @throws {@link TypeError} when `asset` is not an `Asset` or a reference
   * from `Asset.ref()`.
   */
  forAsset(asset: Asset | AssetRef): AssetStateStore;
}

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

  /**
   * Key-value state scoped to an asset, shared across tasks and Dag runs.
   * Every method rejects a key that is not a non-empty string.
   *
   * ```ts
   * const state = client.assetStateStore.forAsset(new Asset({ name: "orders" }));
   * const watermark = await state.get<string>("watermark");
   * await state.set("watermark", "2026-10-08T00:00:00Z");
   * ```
   */
  readonly assetStateStore: AssetStateStores;
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
