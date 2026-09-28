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
 * Marks a class as a task's input: when the Python Dag file calls the stub
 * task with keyword arguments, each public field receives the argument whose
 * name matches it, ignoring case and underscores. That is the same fold the
 * Go and TypeScript SDKs apply, so one Python signature binds identically in
 * every SDK with nothing declared; [ArgName] pins a name the fold cannot
 * reach.
 *
 * ```java
 * public static class ScoreInput implements TaskInput {
 *   // Pinned to region_code. Or drop it: public String regionCode;
 *   @ArgName("region_code")
 *   public String region;
 *
 *   public double threshold; // binds threshold
 * }
 *
 * @Builder.TaskHandler(dag = "etl", task = "score")
 * public Result score(Client client, ScoreInput input) { ... }
 * ```
 *
 * A `TaskInput` binds the same way whichever authoring API declares it: as a
 * `@Builder.TaskHandler` parameter, as above, or as the input type of an
 * [InputTask]. A task method may declare at most one `TaskInput` parameter
 * and, if it does, no other data parameters — the `TaskInput` owns the whole named-argument
 * surface, so field names and flat positions cannot shift each other.
 *
 * The class needs a public no-argument constructor and public non-final
 * fields. No two of those fields may claim argument names that differ only in
 * case or underscores, since the fold cannot tell them apart.
 *
 * A name mismatch in either direction is logged rather than failed: a field
 * nothing supplies keeps its Java default, and an argument no field claims
 * changes nothing the task reads. Once a field has claimed its argument,
 * declare a boxed type when that argument may resolve to nothing.
 *
 * @see InputTask
 */
interface TaskInput
