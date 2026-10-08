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

/** A case of the branches under test; a second class so a branch can tell two cases apart. */
internal class HandleLong : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** A branch that chooses whatever [choice] holds. */
class PickPath : BranchTask {
  override fun choose(
    context: Context,
    client: Client,
  ): Class<out Task> = choice

  companion object {
    var choice: Class<out Task> = HandleLong::class.java
  }
}

internal class BranchTest {
  private fun dagWithCases(): Triple<DagDef, BranchRef, Pair<TaskRef<Unit>, TaskRef<Unit>>> {
    val dag = DagDef("d")
    val handleLong = dag.task<Unit>("handle_long", HandleLong::class.java)
    val handleShort = dag.task<Unit>("handle_short", NoopTask::class.java)
    return Triple(dag, dag.Branch(PickPath::class.java), handleLong to handleShort)
  }

  @Test
  @DisplayName("Should take the branch's task ID from its class when none is given")
  fun shouldDeriveTaskIdFromClass() {
    val (dag, branch, cases) = dagWithCases()
    branch.option(cases.first)

    assertEquals("pickPath", branch.id)
    assertEquals(setOf(dag.tasks.getValue("pickPath")), cases.first.def.upstreams)
  }

  @Test
  @DisplayName("Should reject naming the same case twice")
  fun shouldRejectDuplicateCase() {
    val (_, branch, cases) = dagWithCases()
    branch.option(cases.first)

    val error = assertThrows(IllegalArgumentException::class.java) { branch.option(cases.first) }

    assertEquals(
      "Branch 'pickPath' already chooses between 'handle_long' and others; name each case once",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject two cases a class-named branch could not tell apart")
  fun shouldRejectCasesSharingAClass() {
    val dag = DagDef("d")
    val first = dag.task<Unit>("first", HandleLong::class.java)
    val second = dag.task<Unit>("second", HandleLong::class.java)
    val branch = dag.Branch(PickPath::class.java).option(first)

    val error = assertThrows(IllegalArgumentException::class.java) { branch.option(second) }

    assertEquals(
      "Branch 'pickPath' cannot choose between 'first' and 'second': both run " +
        "'org.apache.airflow.sdk.HandleLong', and a branch names its case by class",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a case declared in another Dag")
  fun shouldRejectCaseFromAnotherDag() {
    val (_, branch, _) = dagWithCases()
    val other = DagDef("other").task<Unit>("handle_long", HandleLong::class.java)

    val error = assertThrows(IllegalArgumentException::class.java) { branch.option(other) }

    assertEquals(
      "Branch 'pickPath' of Dag 'd' cannot run task 'handle_long' of Dag 'other'; " +
        "name a task of the same Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject registering a branch with nothing to choose between")
  fun shouldRejectBranchWithoutCases() {
    val (dag, _, _) = dagWithCases()

    val error = assertThrows(IllegalArgumentException::class.java) { Bundle().register(dag) }

    assertEquals(
      "Branch 'pickPath' has no task to choose between; call option(...), in Dag 'd'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a branch over a task that chooses nothing")
  fun shouldRejectBranchOverAPlainTask() {
    val dag = DagDef("d")

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        BranchRef.of(dag.task<Unit>("plain", NoopTask::class.java))
      }

    assertEquals(
      "Task 'plain' runs 'org.apache.airflow.sdk.NoopTask', which chooses nothing; " +
        "a branch runs a BranchTask",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject naming a case after the Dag was registered")
  fun shouldRejectCaseAfterRegistration() {
    val (dag, branch, cases) = dagWithCases()
    branch.option(cases.first)
    Bundle().register(dag)

    val error = assertThrows(IllegalArgumentException::class.java) { branch.option(cases.second) }

    assertEquals(
      "Branch 'pickPath' of Dag 'd' is already registered; name every case before the Dag is " +
        "added to a Bundle",
      error.message,
    )
  }

  @Test
  @DisplayName("Should declare a branch inside a task group under the group's prefix")
  fun shouldDeclareBranchInGroup() {
    val dag = DagDef("d")
    val reports = dag.taskGroup("reports")
    val long = reports.task<Unit>("long", HandleLong::class.java)
    val branch = reports.Branch("pick", PickPath::class.java).option(long)

    assertEquals("reports.pick", branch.id)
    assertEquals(listOf("reports.long", "reports.pick"), dag.tasks.keys.toList())
  }

  @Test
  @DisplayName("Should carry the task ID of a case rather than its class")
  fun taskIdNamesOneTask() {
    assertEquals(TaskId.of("handle_long"), TaskId.of("handle_long"))
    assertEquals("handle_long", TaskId.of("handle_long").toString())
  }
}
