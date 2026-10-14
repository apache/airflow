/*
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

package org.apache.airflow.sdk

import org.apache.airflow.sdk.execution.AssetRef
import org.apache.airflow.sdk.execution.Client

/**
 * Looks up the state store of an asset by the asset's name or URI.
 *
 * The Java SDK does not receive the inlets and outlets declared on the
 * `@task.stub`, so it cannot know which assets a task uses. The task code
 * passes the name or URI of the asset it wants instead.
 */
class AssetStateStores internal constructor(
  private val impl: Client,
) {
  /**
   * Returns the state store of the asset whose name is [name].
   *
   * @throws IllegalArgumentException if [name] is empty.
   */
  fun byName(name: String): AssetStateStore {
    require(name.isNotEmpty()) { "Asset name must not be empty" }
    return AssetStateStore(AssetRef.Name(name), impl)
  }

  /**
   * Returns the state store of the asset whose URI is [uri].
   *
   * @throws IllegalArgumentException if [uri] is empty.
   */
  fun byUri(uri: String): AssetStateStore {
    require(uri.isNotEmpty()) { "Asset URI must not be empty" }
    return AssetStateStore(AssetRef.Uri(uri), impl)
  }
}

/**
 * Holds the key-value state of one asset. Every task and Dag run that uses the
 * asset shares the same entries.
 *
 * Entries do not expire. They stay until they are deleted, or until no Dag
 * references the asset anymore.
 *
 * If no active asset matches the name or URI used to look up this store, [get]
 * returns `null` and the other methods throw [ApiError].
 *
 * The SDK sends values to Airflow as-is and does not apply the `[workers]
 * state_store_backend` that Python tasks use.
 */
class AssetStateStore internal constructor(
  private val asset: AssetRef,
  private val impl: Client,
) {
  /**
   * Reads the value stored under [key].
   *
   * Integers come back as `Long`, decimals as `Double`, JSON objects as `Map`,
   * and JSON arrays as `List`. So a value stored as an `Integer` comes back as
   * a `Long`.
   *
   * @return The stored value, or `null` if the key is not set.
   * @throws ApiError if the API call fails.
   */
  fun get(key: String): Any? = impl.getAssetStateStore(asset, key)?.value

  /**
   * Stores [value] under [key], replacing any existing value.
   *
   * The SDK sends the value as JSON. Do not store a `ByteArray`. The supervisor
   * cannot decode it and never answers, so the task hangs. Encode binary data
   * as a string instead, for example with Base64.
   *
   * @param key State key.
   * @param value Value to store. Must be JSON-serializable.
   * @throws IllegalArgumentException if the SDK cannot send the type of
   *   [value], for example a `BigDecimal`.
   * @throws ApiError if the API call fails.
   */
  fun set(
    key: String,
    value: Any,
  ) = impl.setAssetStateStore(asset, key, value)

  /**
   * Deletes the value stored under [key]. Does nothing if the key is not set.
   *
   * @throws ApiError if the API call fails.
   */
  fun delete(key: String) = impl.deleteAssetStateStore(asset, key)

  /**
   * Deletes every key stored for this asset.
   *
   * @throws ApiError if the API call fails.
   */
  fun clear() = impl.clearAssetStateStore(asset)
}
