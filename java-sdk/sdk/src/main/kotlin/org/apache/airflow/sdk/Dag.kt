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

/**
 * Collection of tasks with directional dependencies.
 *
 * @param id The Dag's id. This must consist exclusively of alphanumeric characters,
 *   dashes, dots and underscores (all ASCII).
 */
class Dag(
  // TODO: charset check?
  val id: String,
) {
  internal var tasks = mutableMapOf<String, Class<out Task>>()
  internal var dependants = mutableMapOf<String, MutableSet<String>>()

  @JvmOverloads
  fun addTask(
    id: String,
    definition: Class<out Task>,
    dependsOn: List<String> = emptyList(),
  ) {
    // TODO: Check duplicate key.
    tasks[id] = definition
    for (parent in dependsOn) {
      dependants.getOrPut(parent) { mutableSetOf() }.add(id)
    }
  }
}
