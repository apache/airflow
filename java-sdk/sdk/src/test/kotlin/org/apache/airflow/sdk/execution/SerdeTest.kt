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

package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.In
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.execution.comm.DagFileParseRequest
import org.apache.airflow.sdk.internal.Refs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime

private class SerdeNoopTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

@Suppress("UNCHECKED_CAST")
private fun taskData(
  serialized: Map<String, Any?>,
  index: Int,
): Map<String, Any?> {
  val tasks = serialized["tasks"] as List<Map<String, Any?>>
  assertEquals("operator", tasks[index]["__type"])
  return tasks[index]["__var"] as Map<String, Any?>
}

internal class SerdeTest {
  @Test
  @DisplayName("Should always emit required dag fields and config-backed defaults")
  fun shouldEmitRequiredDagFields() {
    val serialized = serializeDag(DagDef("d"), "/bundles/app/dags.jar", "app/dags.jar")

    assertEquals("d", serialized["dag_id"])
    assertEquals("/bundles/app/dags.jar", serialized["fileloc"])
    assertEquals("app/dags.jar", serialized["relative_fileloc"])
    assertEquals("UTC", serialized["timezone"])
    assertEquals(
      mapOf("__type" to "airflow.timetables.simple.NullTimetable", "__var" to emptyMap<String, Any?>()),
      serialized["timetable"],
    )
    assertEquals(emptyList<Any?>(), serialized["tasks"])
    assertEquals(emptyList<Any?>(), serialized["dag_dependencies"])
    assertEquals(emptyMap<String, Any?>(), serialized["edge_info"])
    assertEquals(emptyList<Any?>(), serialized["params"])
    assertNull(serialized["deadline"])
    assertNull(serialized["allowed_run_types"])
    assertEquals(16, serialized["max_active_tasks"])
    assertEquals(16, serialized["max_active_runs"])
    assertEquals(0, serialized["max_consecutive_failed_dag_runs"])
    assertEquals(false, serialized["catchup"])
    assertEquals(false, serialized["disable_bundle_versioning"])
    assertFalse("description" in serialized)
    assertFalse("fail_fast" in serialized)
    assertFalse("tags" in serialized)
  }

  @Test
  @DisplayName("Should map schedule strings to the matching timetable")
  fun shouldMapScheduleToTimetable() {
    val cron = serializeDag(DagDef("d").config("schedule", "@daily"), "", ".")
    assertEquals(
      mapOf(
        "__type" to "airflow.timetables.trigger.CronTriggerTimetable",
        "__var" to
          mapOf(
            "expression" to "@daily",
            "timezone" to "UTC",
            "interval" to 0.0,
            "run_immediately" to false,
          ),
      ),
      cron["timetable"],
    )

    val once = serializeDag(DagDef("d").config("schedule", "@once"), "", ".")
    assertEquals(
      mapOf("__type" to "airflow.timetables.simple.OnceTimetable", "__var" to emptyMap<String, Any?>()),
      once["timetable"],
    )

    val continuous = serializeDag(DagDef("d").config("schedule", "@continuous"), "", ".")
    assertEquals(
      mapOf("__type" to "airflow.timetables.simple.ContinuousTimetable", "__var" to emptyMap<String, Any?>()),
      continuous["timetable"],
    )
  }

  @Test
  @DisplayName("Should apply dag config values with Python's emit rules")
  fun shouldApplyDagConfig() {
    val dag =
      DagDef("d")
        .config("description", "demo")
        .config("tags", listOf("b", "a"))
        .config("catchup", true)
        .config("fail_fast", true)
        .config("max_active_runs", 3)
        .config("dagrun_timeout", Duration.ofMinutes(5))
        .config("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"))

    val serialized = serializeDag(dag, "", ".")

    assertEquals("demo", serialized["description"])
    assertEquals(listOf("a", "b"), serialized["tags"])
    assertEquals(true, serialized["catchup"])
    assertEquals(true, serialized["fail_fast"])
    assertEquals(3, serialized["max_active_runs"])
    assertEquals(300.0, serialized["dagrun_timeout"])
    assertEquals(1.7672256E9, serialized["start_date"])
  }

  @Test
  @DisplayName("Should serialize tasks with identity fields, config, and sorted downstream ids")
  fun shouldSerializeTasks() {
    val extractDef =
      TaskDef("extract", SerdeNoopTask::class.java)
        .config("retries", 2)
        .config("queue", "q")
        .config("retry_delay", Duration.ofMinutes(10))
    val transformDef =
      TaskDef("transform", SerdeNoopTask::class.java)
        .dependsOn(extractDef)
        // Explicitly at schema defaults: omitted from the serialized form.
        .config("retries", 0)
        .config("queue", "default")
        .config("retry_delay", Duration.ofMinutes(5))
    val dag = DagDef("d").addTask(extractDef).addTask(transformDef)

    val serialized = serializeDag(dag, "", ".")

    val extract = taskData(serialized, 0)
    assertEquals("extract", extract["task_id"])
    assertEquals("SerdeNoopTask", extract["task_type"])
    assertEquals("org.apache.airflow.sdk.execution", extract["_task_module"])
    assertEquals("java", extract["language"])
    assertEquals(emptyList<Any?>(), extract["template_fields"])
    assertEquals(2, extract["retries"])
    assertEquals("q", extract["queue"])
    assertEquals(600.0, extract["retry_delay"])
    assertEquals(listOf("transform"), extract["downstream_task_ids"])

    val transform = taskData(serialized, 1)
    assertEquals("transform", transform["task_id"])
    assertFalse("retries" in transform)
    assertFalse("queue" in transform)
    assertFalse("retry_delay" in transform)
    assertFalse("downstream_task_ids" in transform)

    assertEquals(
      mapOf("extract" to listOf("operator", "extract"), "transform" to listOf("operator", "transform")),
      (serialized["task_group"] as Map<*, *>)["children"],
    )
  }

  @Test
  @DisplayName("Should serialize wiring-registered dags with their data-flow edges")
  fun shouldSerializeWiredDag() {
    val dag = DagDef("d")
    val extracted = Refs.register<Long>(dag, TaskDef("extract", SerdeNoopTask::class.java), listOf())
    Refs.register<Unit>(dag, TaskDef("transform", SerdeNoopTask::class.java), listOf<In<*>>(extracted))

    val serialized = serializeDag(dag, "", ".")

    assertEquals(listOf("transform"), taskData(serialized, 0)["downstream_task_ids"])
    assertFalse("_arg_bindings" in taskData(serialized, 1))
  }

  @Test
  @DisplayName("Should wrap parsed dags in a DagFileParsingResult body")
  fun shouldBuildParsingResult() {
    val bundle = Bundle(listOf(DagDef("d").addTask(TaskDef("t", SerdeNoopTask::class.java))))
    val request =
      DagFileParseRequest().also {
        it.file = "/bundles/app/dags.jar"
        it.bundlePath = "/bundles"
      }

    val result = parseDags(bundle, request)

    assertEquals("DagFileParsingResult", result["type"])
    assertEquals("/bundles/app/dags.jar", result["fileloc"])
    val dags = result["serialized_dags"] as List<*>
    assertEquals(1, dags.size)
    val data = (dags[0] as Map<*, *>)["data"] as Map<*, *>
    assertEquals(3, data["__version"])
    val dag = data["dag"] as Map<*, *>
    assertEquals("d", dag["dag_id"])
    assertEquals("app/dags.jar", dag["relative_fileloc"])
  }

  @Test
  @DisplayName("Should encode temporals and nested maps with the type/var envelope")
  fun shouldEncodeValuesWithTypeEnvelope() {
    assertEquals(
      mapOf("__type" to "timedelta", "__var" to 90.0),
      serializeValue(Duration.ofSeconds(90)),
    )
    assertEquals(
      mapOf("__type" to "datetime", "__var" to 1.7672256E9),
      serializeValue(OffsetDateTime.parse("2026-01-01T00:00:00Z")),
    )
    assertEquals(
      mapOf("__type" to "dict", "__var" to mapOf("k" to listOf(1, 2))),
      serializeValue(mapOf("k" to listOf(1, 2))),
    )
    assertEquals(42, unwrapTypeEncoding(mapOf("__type" to "timedelta", "__var" to 42)))
    assertEquals(mapOf("plain" to 1), unwrapTypeEncoding(mapOf("plain" to 1)))
  }
}
