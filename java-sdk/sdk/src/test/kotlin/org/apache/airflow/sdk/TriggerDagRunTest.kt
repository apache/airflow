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

import org.apache.airflow.sdk.execution.serializeDag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime

internal class TriggerDagRunTest {
  @Suppress("UNCHECKED_CAST")
  private fun serializeTrigger(trigger: TriggerDagRun): Map<String, Any?> {
    val dag = DagDef("d")
    dag.task("trigger", trigger)
    val tasks = serializeDag(dag, "", ".")["tasks"] as List<Map<String, Any?>>
    return tasks.single()["__var"] as Map<String, Any?>
  }

  @Test
  @DisplayName("Should reject a trigger that names no Dag")
  fun shouldRejectEmptyDagId() {
    val error = assertThrows(IllegalArgumentException::class.java) { TriggerDagRun("") }

    assertEquals("TriggerDagRun needs the ID of the Dag to trigger", error.message)
  }

  @Test
  @DisplayName("Should point at the constructor for the Dag being triggered")
  fun shouldRejectTriggerDagIdAsConfig() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TriggerDagRun("downstream").config("trigger_dag_id", "other")
      }

    assertEquals(
      "The Dag to trigger is the TriggerDagRun argument, not the config key 'trigger_dag_id'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a poke interval that is not a whole number of seconds")
  fun shouldRejectFractionalPokeInterval() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TriggerDagRun("downstream").config("poke_interval", Duration.ofMillis(1500))
      }

    assertEquals(
      "Value for TriggerDagRun config key 'poke_interval' is PT1.5S; it must be a whole, " +
        "non-negative number of seconds",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a state no Dag run can be in")
  fun shouldRejectUnknownState() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TriggerDagRun("downstream").config("allowed_states", listOf("success", "done"))
      }

    assertEquals(
      "Value for TriggerDagRun config key 'allowed_states' holds done, which is not a Dag run " +
        "state; use one of queued, running, success, failed",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject an unknown setting")
  fun shouldRejectUnknownKey() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TriggerDagRun("downstream").config("waitForCompletion", true)
      }

    assertEquals("Unknown TriggerDagRun config key: 'waitForCompletion'", error.message)
  }

  @Test
  @DisplayName("Should write a trigger task as Python writes a TriggerDagRunOperator")
  fun shouldSerializeDefaults() {
    val data = serializeTrigger(TriggerDagRun("downstream"))

    assertEquals("TriggerDagRunOperator", data["task_type"])
    assertEquals("airflow.providers.standard.operators.trigger_dagrun", data["_task_module"])
    assertEquals("#ffefeb", data["ui_color"])
    assertEquals(mapOf("Triggered DAG" to "_link_TriggerDagRunLink"), data["_operator_extra_links"])
    assertEquals(mapOf("conf" to "py"), data["template_fields_renderers"])
    assertEquals("downstream", data["trigger_dag_id"])
    assertEquals("NOTSET", data["logical_date"])
    assertEquals(false, data["wait_for_completion"])
    assertEquals(false, data["skip_when_already_exists"])
    // A Java task's markers belong to a task the Java runtime runs a body for.
    assertFalse("language" in data)
    assertFalse("is_stub" in data)
    assertFalse("reset_dag_run" in data)
  }

  @Test
  @DisplayName("Should write a logical date as Python writes a template field, and run_after type-encoded")
  fun shouldSerializeTemporalSettings() {
    val data =
      serializeTrigger(
        TriggerDagRun("downstream")
          .config("logical_date", OffsetDateTime.parse("2026-09-30T01:02:03Z"))
          .config("run_after", OffsetDateTime.parse("2026-09-30T00:00:00Z"))
          .config("poke_interval", Duration.ofSeconds(30)),
      )

    assertEquals("2026-09-30 01:02:03+00:00", data["logical_date"])
    assertEquals(mapOf("__type" to "datetime", "__var" to 1790726400.0), data["run_after"])
    assertEquals(30, data["poke_interval"])
  }

  @Test
  @DisplayName("Should record the Dags a Dag triggers, sorted as Python sorts them")
  fun shouldRecordDagDependencies() {
    val dag = DagDef("d")
    dag.task("second", TriggerDagRun("beta"))
    dag.task("first", TriggerDagRun("alpha"))

    val dependencies = serializeDag(dag, "", ".")["dag_dependencies"] as List<*>

    assertEquals(
      listOf(
        mapOf(
          "source" to "d",
          "target" to "alpha",
          "label" to "first",
          "dependency_type" to "trigger",
          "dependency_id" to "first",
        ),
        mapOf(
          "source" to "d",
          "target" to "beta",
          "label" to "second",
          "dependency_type" to "trigger",
          "dependency_id" to "second",
        ),
      ),
      dependencies,
    )
  }

  @Test
  @DisplayName("Should carry a trigger task's own Airflow settings")
  fun shouldCarryTaskConfig() {
    val dag = DagDef("d")
    dag.task("trigger", TriggerDagRun("downstream")).config("retries", 2)

    @Suppress("UNCHECKED_CAST")
    val tasks = serializeDag(dag, "", ".")["tasks"] as List<Map<String, Any?>>

    assertEquals(2, (tasks.single()["__var"] as Map<*, *>)["retries"])
  }

  @Test
  @DisplayName("Should declare a trigger task inside a task group under the group's prefix")
  fun shouldDeclareTriggerInGroup() {
    val dag = DagDef("d")
    val ref = dag.taskGroup("downstream").task("trigger", TriggerDagRun("other"))

    assertEquals("downstream.trigger", ref.def.id)
    assertEquals(listOf("downstream.trigger"), dag.tasks.keys.toList())
  }

  @Test
  @DisplayName("Should not let a setting made after registration reach the registered task")
  fun shouldSnapshotTriggerOnRegistration() {
    val dag = DagDef("d")
    val nested = mutableMapOf<String, Any>("n" to 1)
    val list = mutableListOf<Any>("a")
    val trigger = TriggerDagRun("other").config("conf", mapOf("nested" to nested, "list" to list))
    val ref = dag.task("trigger", trigger)

    trigger.config("note", "late")
    nested["n"] = 2
    list += "b"

    val registered = ref.def.trigger!!.settings
    assertFalse("note" in registered)
    assertEquals(mapOf("nested" to mapOf("n" to 1), "list" to listOf("a")), registered["conf"])
  }

  @Test
  @DisplayName("Should keep two tasks registered from one builder apart")
  fun shouldKeepTasksFromOneBuilderApart() {
    val dag = DagDef("d")
    val trigger = TriggerDagRun("other").config("note", "first")
    val first = dag.task("first", trigger)
    trigger.config("note", "second")
    val second = dag.task("second", trigger)

    assertEquals("first", first.def.trigger!!.settings["note"])
    assertEquals("second", second.def.trigger!!.settings["note"])
  }
}
