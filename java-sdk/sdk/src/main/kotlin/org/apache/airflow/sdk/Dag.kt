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

import java.time.Duration
import java.time.Instant

/**
 * A Dag (Directed Acyclic Graph) is a collection of tasks with directional dependencies.
 *
 * A Dag has a schedule, a start date and an end date (optional). For each schedule
 * (say daily or hourly), the Dag needs to run each individual task as their
 * dependencies are met.
 *
 * @param dagId The id of the Dag; must consist exclusively of alphanumeric characters,
 *   dashes, dots and underscores (all ASCII).
 * @param description The description for the Dag to e.g. be shown on the webserver.
 * @param schedule Defines when Dag runs are scheduled. Can be a cron expression string
 *   (e.g. "0 0 * * *"), a preset (e.g. "@daily", "@hourly", "@once", "@continuous"),
 *   or null for no schedule.
 * @param startDate The timestamp from which the scheduler will attempt to backfill.
 * @param endDate A date beyond which your Dag won't run; leave null for open-ended scheduling.
 * @param defaultArgs A map of default parameters to be used as constructor keyword
 *   parameters when initialising operators.
 * @param maxActiveTasks The number of task instances allowed to run concurrently per Dag run.
 * @param maxActiveRuns Maximum number of active Dag runs.
 * @param maxConsecutiveFailedDagRuns Maximum number of consecutive failed Dag runs,
 *   beyond this the scheduler will disable the Dag.
 * @param dagrunTimeout Duration a DagRun is allowed to run before it times out or fails.
 * @param catchup Perform scheduler catchup (or only run latest)? Defaults to false.
 * @param docMd Documentation in markdown format.
 * @param accessControl Optional Dag-level access control actions, e.g.
 *   mapOf("role1" to mapOf("DAGs" to setOf("can_read", "can_edit"))).
 * @param isPausedUponCreation Whether the Dag is paused when created for the first time.
 * @param tags Set of tags to help filtering Dags in the UI.
 * @param ownerLinks Map of owners and their links, clickable on the Dags view UI.
 * @param failFast Fails currently running tasks when a task in Dag fails.
 * @param dagDisplayName The display name of the Dag on the UI. Defaults to dagId.
 * @param renderTemplateAsNativeObj If true, uses native rendering for templates.
 * @param params A map of Dag-level parameters accessible in templates, namespaced under
 *   "params". These can be overridden at the task level.
 */
class Dag
  @JvmOverloads
  constructor(
    val dagId: String,
    val description: String? = null,
    val schedule: String? = null,
    val startDate: Instant? = null,
    val endDate: Instant? = null,
    val defaultArgs: Map<String, Any> = emptyMap(),
    val maxActiveTasks: Int = DEFAULT_MAX_ACTIVE_TASKS,
    val maxActiveRuns: Int = DEFAULT_MAX_ACTIVE_RUNS,
    val maxConsecutiveFailedDagRuns: Int = DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS,
    val dagrunTimeout: Duration? = null,
    val catchup: Boolean = false,
    val docMd: String? = null,
    val accessControl: Map<String, Map<String, Set<String>>>? = null,
    val isPausedUponCreation: Boolean? = null,
    val tags: Set<String> = emptySet(),
    val ownerLinks: Map<String, String> = emptyMap(),
    val failFast: Boolean = false,
    val dagDisplayName: String? = null,
    val renderTemplateAsNativeObj: Boolean = false,
    val params: Map<String, Any>? = null,
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

    companion object {
      const val DEFAULT_MAX_ACTIVE_TASKS = 16
      const val DEFAULT_MAX_ACTIVE_RUNS = 16
      const val DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS = 0
    }
  }
