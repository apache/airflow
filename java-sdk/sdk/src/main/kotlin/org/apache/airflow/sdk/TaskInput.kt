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
 * Marks a class as a task's input bundle: when the Python Dag file calls the
 * stub task with keyword arguments, each public field receives the runtime
 * binding whose name matches the field ([ArgName] value, or the verbatim
 * field name).
 *
 * A task method may declare at most one `TaskInput` parameter and, if it
 * does, no other data parameters — the bundle owns the whole named-argument
 * surface, so field names and flat positions cannot shift each other.
 *
 * ```java
 * public static class ScoreInput implements TaskInput {
 *   @ArgName("region_code") public String region;  // explicit wire name
 *   public double threshold;                       // binds "threshold"
 * }
 *
 * @Builder.Task
 * public Result score(Client client, ScoreInput input) { ... }
 * ```
 *
 * The class needs a public no-argument constructor and public non-final
 * fields.
 */
interface TaskInput
