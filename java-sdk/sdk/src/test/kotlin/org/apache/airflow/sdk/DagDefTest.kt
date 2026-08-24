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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime

internal class DagDefTest {
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  @Test
  @DisplayName("Should index tasks by taskId in registration order")
  fun shouldIndexTasksByTaskId() {
    val extract = TaskDef("extract", NoOp::class.java)
    val load = TaskDef("load", NoOp::class.java)

    val dag = DagDef("dag").addTask(extract).addTask(load)

    Assertions.assertEquals(listOf("extract", "load"), dag.tasks.keys.toList())
    Assertions.assertEquals(mapOf("extract" to extract, "load" to load), dag.tasks)
  }

  @Test
  @DisplayName("Should register a task from its id and implementation class")
  fun shouldRegisterTaskFromIdAndImplementationClass() {
    val dag = DagDef("dag").addTask("extract", NoOp::class.java)

    val task = dag.tasks.getValue("extract")
    Assertions.assertEquals("extract", task.id)
    Assertions.assertEquals(NoOp::class.java, task.definition)
    Assertions.assertEquals(dag, task.owner)
  }

  @Test
  @DisplayName("Should reject duplicate task ids")
  fun shouldRejectDuplicateTaskIds() {
    val dag = DagDef("dag").addTask(TaskDef("extract", NoOp::class.java))

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dag.addTask(TaskDef("extract", NoOp::class.java))
      }

    Assertions.assertEquals("Tasks in Dag have duplicate ID: extract", error.message)
  }

  @Test
  @DisplayName("Should reject a task already registered with another dag")
  fun shouldRejectTaskOwnedByAnotherDag() {
    val extract = TaskDef("extract", NoOp::class.java)
    DagDef("first").addTask(extract)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("second").addTask(extract)
      }

    Assertions.assertEquals("Task 'extract' already belongs to Dag 'first'", error.message)
  }

  @Test
  @DisplayName("Should reject the same task registered twice with one dag")
  fun shouldRejectTaskRegisteredTwice() {
    val extract = TaskDef("extract", NoOp::class.java)
    val dag = DagDef("dag").addTask(extract)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dag.addTask(extract)
      }

    Assertions.assertEquals("Task 'extract' already belongs to Dag 'dag'", error.message)
  }

  @Test
  @DisplayName("Should store validated dag config values keyed by schema name")
  fun shouldStoreDagConfigValues() {
    val dag =
      DagDef("dag")
        .config("schedule", "@daily")
        .config("description", "demo")
        .config("catchup", true)
        .config("max_active_runs", 3)
        .config("dagrun_timeout", Duration.ofMinutes(5))
        .config("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"))
        .config("tags", listOf("a", "b"))

    Assertions.assertEquals(
      mapOf(
        "schedule" to "@daily",
        "description" to "demo",
        "catchup" to true,
        "max_active_runs" to 3,
        "dagrun_timeout" to Duration.ofMinutes(5),
        "start_date" to OffsetDateTime.parse("2026-01-01T00:00:00Z"),
        "tags" to listOf("a", "b"),
      ),
      dag.dagConfig,
    )
  }

  @Test
  @DisplayName("Should reject unknown dag config keys")
  fun shouldRejectUnknownDagConfigKey() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("dag").config("scheduel", "@daily")
      }

    Assertions.assertEquals("Unknown Dag config key: 'scheduel'", error.message)
  }

  @Test
  @DisplayName("Should reject dag config values of the wrong type")
  fun shouldRejectMismatchedDagConfigValue() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("dag").config("catchup", "yes")
      }

    Assertions.assertEquals(
      "Value for Dag config key 'catchup' must be a Boolean, got: java.lang.String",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject null dag config values")
  fun shouldRejectNullDagConfigValue() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("dag").config("description", null)
      }

    Assertions.assertEquals("Value for Dag config key 'description' must not be null", error.message)
  }

  @Test
  @DisplayName("Should reject non-integral values for integer dag config keys")
  fun shouldRejectFractionalIntegerValue() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("dag").config("max_active_runs", 1.5)
      }

    Assertions.assertEquals(
      "Value for Dag config key 'max_active_runs' must be an integral Number, got: java.lang.Double",
      error.message,
    )
  }

  @Test
  @DisplayName("Should store validated task config values on the task definition")
  fun shouldStoreTaskConfigValues() {
    val def =
      TaskDef("extract", NoOp::class.java)
        .config("retries", 2)
        .config("queue", "q")
        .config("retry_delay", Duration.ofMinutes(5))
        .config("retry_exponential_backoff", 1.5)

    Assertions.assertEquals(
      mapOf(
        "retries" to 2,
        "queue" to "q",
        "retry_delay" to Duration.ofMinutes(5),
        "retry_exponential_backoff" to 1.5,
      ),
      def.configValues,
    )
  }

  @Test
  @DisplayName("Should reject unknown task config keys")
  fun shouldRejectUnknownTaskConfigKey() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        TaskDef("extract", NoOp::class.java).config("retrys", 1)
      }

    Assertions.assertEquals("Unknown task config key: 'retrys'", error.message)
  }

  @Test
  @DisplayName("Should record upstream task definitions from dependsOn")
  fun shouldRecordUpstreams() {
    val extract = TaskDef("extract", NoOp::class.java)
    val load = TaskDef("load", NoOp::class.java).dependsOn(extract)
    DagDef("dag").addTask(extract).addTask(load)

    Assertions.assertEquals(emptySet<TaskDef>(), extract.upstreams)
    Assertions.assertEquals(setOf(extract), load.upstreams)
  }

  @Test
  @DisplayName("Should wire upstreams passed to the addTask overload")
  fun shouldWireUpstreamsFromAddTaskOverload() {
    val extract = TaskDef("extract", NoOp::class.java)
    val load = TaskDef("load", NoOp::class.java)
    DagDef("dag").addTask(extract).addTask(load, listOf(extract))

    Assertions.assertEquals(setOf(extract), load.upstreams)
  }
}
