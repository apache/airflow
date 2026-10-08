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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/** A case of the switches under test; a second class so a switch can tell two cases apart. */
internal class HandleLong : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** A switch that chooses whatever [choice] holds. */
class PickPath : SwitchTask {
  override fun choose(
    context: Context,
    client: Client,
  ): Class<out Task> = choice

  companion object {
    var choice: Class<out Task> = HandleLong::class.java
  }
}

internal class SwitchTest {
  private fun dagWithCases(): Triple<DagDef, SwitchRef, Pair<TaskRef<Unit>, TaskRef<Unit>>> {
    val dag = DagDef("d")
    val handleLong = dag.task<Unit>("handle_long", HandleLong::class.java)
    val handleShort = dag.task<Unit>("handle_short", NoopTask::class.java)
    return Triple(dag, dag.Switch(PickPath::class.java), handleLong to handleShort)
  }

  @Test
  @DisplayName("Should take the switch's task ID from its class when none is given")
  fun shouldDeriveTaskIdFromClass() {
    val (dag, switch, cases) = dagWithCases()
    switch.Case(cases.first)

    assertEquals("pickPath", switch.id)
    assertEquals(setOf(dag.tasks.getValue("pickPath")), cases.first.def.upstreams)
  }

  @Test
  @DisplayName("Should reject naming the same case twice")
  fun shouldRejectDuplicateCase() {
    val (_, switch, cases) = dagWithCases()
    switch.Case(cases.first)

    val error = assertThrows(IllegalArgumentException::class.java) { switch.Case(cases.first) }

    assertEquals(
      "Switch 'pickPath' already chooses between 'handle_long' and others; name each case once",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject two cases a class-named switch could not tell apart")
  fun shouldRejectCasesSharingAClass() {
    val dag = DagDef("d")
    val first = dag.task<Unit>("first", HandleLong::class.java)
    val second = dag.task<Unit>("second", HandleLong::class.java)
    val switch = dag.Switch(PickPath::class.java).Case(first)

    val error = assertThrows(IllegalArgumentException::class.java) { switch.Case(second) }

    assertEquals(
      "Switch 'pickPath' cannot choose between 'first' and 'second': both run " +
        "'org.apache.airflow.sdk.HandleLong', and a switch names its case by class",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a case declared in another Dag")
  fun shouldRejectCaseFromAnotherDag() {
    val (_, switch, _) = dagWithCases()
    val other = DagDef("other").task<Unit>("handle_long", HandleLong::class.java)

    val error = assertThrows(IllegalArgumentException::class.java) { switch.Case(other) }

    assertEquals(
      "Switch 'pickPath' of Dag 'd' cannot run task 'handle_long' of Dag 'other'; " +
        "name a task of the same Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject registering a switch with nothing to choose between")
  fun shouldRejectSwitchWithoutCases() {
    val (dag, _, _) = dagWithCases()

    val error = assertThrows(IllegalArgumentException::class.java) { Bundle().register(dag) }

    assertEquals(
      "Switch 'pickPath' has no task to choose between; call Case(...), in Dag 'd'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a switch over a task that chooses nothing")
  fun shouldRejectSwitchOverAPlainTask() {
    val dag = DagDef("d")

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        SwitchRef.of(dag.task<Unit>("plain", NoopTask::class.java))
      }

    assertEquals(
      "Task 'plain' runs 'org.apache.airflow.sdk.NoopTask', which chooses nothing; " +
        "a switch runs a SwitchTask",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject naming a case after the Dag was registered")
  fun shouldRejectCaseAfterRegistration() {
    val (dag, switch, cases) = dagWithCases()
    switch.Case(cases.first)
    Bundle().register(dag)

    val error = assertThrows(IllegalArgumentException::class.java) { switch.Case(cases.second) }

    assertEquals(
      "Switch 'pickPath' of Dag 'd' is already registered; name every case before the Dag is " +
        "added to a Bundle",
      error.message,
    )
  }

  @Test
  @DisplayName("Should declare a switch inside a task group under the group's prefix")
  fun shouldDeclareSwitchInGroup() {
    val dag = DagDef("d")
    val reports = dag.taskGroup("reports")
    val long = reports.task<Unit>("long", HandleLong::class.java)
    val switch = reports.Switch("pick", PickPath::class.java).Case(long)

    assertEquals("reports.pick", switch.id)
    assertEquals(listOf("reports.long", "reports.pick"), dag.tasks.keys.toList())
  }

  @Test
  @DisplayName("Should carry the task ID of a case rather than its class")
  fun taskIdNamesOneTask() {
    assertEquals(TaskId.of("handle_long"), TaskId.of("handle_long"))
    assertEquals("handle_long", TaskId.of("handle_long").toString())
  }
}
