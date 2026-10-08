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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/** A condition the tests register; what it decides is driven from the runtime tests. */
class HasRows : ConditionTask {
  override fun decide(
    context: Context,
    client: Client,
  ): Boolean = decision

  companion object {
    var decision: Boolean = true
  }
}

internal class ConditionTest {
  private fun dagWithSides(): Triple<DagDef, ConditionRef, Pair<TaskRef<Unit>, TaskRef<Unit>>> {
    val dag = DagDef("d")
    val load = dag.task<Unit>("load", NoopTask::class.java)
    val reportEmpty = dag.task<Unit>("report_empty", NoopTask::class.java)
    return Triple(dag, dag.If(HasRows::class.java), load to reportEmpty)
  }

  @Test
  @DisplayName("Should take the condition's task ID from its class when none is given")
  fun shouldDeriveTaskIdFromClass() {
    val (dag, condition, sides) = dagWithSides()
    condition.then(sides.first)

    assertEquals("hasRows", condition.id)
    assertTrue("hasRows" in dag.tasks)
  }

  @Test
  @DisplayName("Should run each named side after the condition")
  fun shouldRunEachSideAfterTheCondition() {
    val (dag, condition, sides) = dagWithSides()
    val (load, reportEmpty) = sides
    condition.then(load).orElse(reportEmpty)

    val decider = dag.tasks.getValue("hasRows")
    assertEquals(setOf(decider), load.def.upstreams)
    assertEquals(setOf(decider), reportEmpty.def.upstreams)
  }

  @Test
  @DisplayName("Should mark only a deciding task as able to skip what runs after it")
  fun shouldMarkDeciderAsSkipping() {
    val (dag, condition, sides) = dagWithSides()
    condition.then(sides.first)

    val tasks = serializeDag(dag, "", ".")["tasks"] as List<*>

    @Suppress("UNCHECKED_CAST")
    val byId = tasks.associate { task -> ((task as Map<String, Any?>)["__var"] as Map<String, Any?>).let { it["task_id"] to it } }
    assertEquals(true, (byId["hasRows"] as Map<*, *>)["_can_skip_downstream"])
    assertFalse("_can_skip_downstream" in (byId["load"] as Map<*, *>))
  }

  @Test
  @DisplayName("Should reject naming the same side twice")
  fun shouldRejectNamingASideTwice() {
    val (_, condition, sides) = dagWithSides()
    condition.then(sides.first)

    val error = assertThrows(IllegalArgumentException::class.java) { condition.then(sides.second) }

    assertEquals("Condition 'hasRows' already runs 'load' on its then side; name each side once", error.message)
  }

  @Test
  @DisplayName("Should reject a condition whose sides are the same task")
  fun shouldRejectIdenticalSides() {
    val (_, condition, sides) = dagWithSides()
    condition.then(sides.first)

    val error = assertThrows(IllegalArgumentException::class.java) { condition.orElse(sides.first) }

    assertEquals(
      "Condition 'hasRows' already runs 'load' on its other side, so the condition would decide nothing",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a side declared in another Dag")
  fun shouldRejectSideFromAnotherDag() {
    val (_, condition, _) = dagWithSides()
    val other = DagDef("other").task<Unit>("load", NoopTask::class.java)

    val error = assertThrows(IllegalArgumentException::class.java) { condition.then(other) }

    assertEquals(
      "Condition 'hasRows' of Dag 'd' cannot run task 'load' of Dag 'other'; name a task of the same Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject registering a condition that names no task to run")
  fun shouldRejectConditionWithoutThen() {
    val (dag, _, _) = dagWithSides()

    val error = assertThrows(IllegalArgumentException::class.java) { Bundle().register(dag) }

    assertEquals(
      "Condition 'hasRows' names no task to run when it holds; call then(...), in Dag 'd'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject declaring one task as a condition twice")
  fun shouldRejectDecidingTwice() {
    val (_, condition, _) = dagWithSides()

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        ConditionRef.of(TaskRef(condition.nodes().single()))
      }

    assertEquals("Task 'hasRows' already decides what to skip; declare it once", error.message)
  }

  @Test
  @DisplayName("Should reject naming a side after the Dag was registered")
  fun shouldRejectNamingASideAfterRegistration() {
    val (dag, condition, sides) = dagWithSides()
    condition.then(sides.first)
    Bundle().register(dag)

    val error = assertThrows(IllegalArgumentException::class.java) { condition.orElse(sides.second) }

    assertEquals(
      "Condition 'hasRows' of Dag 'd' is already registered; name every case before the Dag is " +
        "added to a Bundle",
      error.message,
    )
  }

  @Test
  @DisplayName("Should declare a condition inside a task group under the group's prefix")
  fun shouldDeclareConditionInGroup() {
    val dag = DagDef("d")
    val checks = dag.taskGroup("checks")
    val load = checks.task<Unit>("load", NoopTask::class.java)
    val condition = checks.If("has_rows", HasRows::class.java)
    condition.then(load)

    assertEquals("checks.has_rows", condition.id)
    assertEquals(listOf("checks.load", "checks.has_rows"), dag.tasks.keys.toList())
  }
}
