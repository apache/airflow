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

internal class DagTest {
  private class NoopTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  @Test
  @DisplayName("Should accept valid dag and task ids")
  fun shouldAcceptValidIds() {
    for (id in listOf("simple", "with-dash", "with.dot", "with_underscore", "0numeric", "ünïcode", "任務", "a".repeat(250))) {
      Dag(id).addTask(id, NoopTask::class.java)
    }
  }

  @Test
  @DisplayName("Should reject dag ids with invalid characters")
  fun shouldRejectInvalidDagIds() {
    for (id in listOf("", "with space", "with/slash", "with:colon", "with\ttab")) {
      val error =
        Assertions.assertThrows(IllegalArgumentException::class.java) { Dag(id) }

      Assertions.assertEquals(
        "Dag ID '$id' must be made of alphanumeric characters, dashes, dots, and underscores",
        error.message,
      )
    }
  }

  @Test
  @DisplayName("Should reject dag ids longer than 250 characters")
  fun shouldRejectTooLongDagIds() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) { Dag("a".repeat(251)) }

    Assertions.assertEquals("Dag ID must be less than 250 characters, not 251", error.message)
  }

  @Test
  @DisplayName("Should reject task ids with invalid characters")
  fun shouldRejectInvalidTaskIds() {
    val dag = Dag("dag")

    for (id in listOf("", "with space", "with/slash")) {
      val error =
        Assertions.assertThrows(IllegalArgumentException::class.java) {
          dag.addTask(id, NoopTask::class.java)
        }

      Assertions.assertEquals(
        "Task ID '$id' must be made of alphanumeric characters, dashes, dots, and underscores",
        error.message,
      )
    }
  }

  @Test
  @DisplayName("Should reject task ids longer than 250 characters")
  fun shouldRejectTooLongTaskIds() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Dag("dag").addTask("a".repeat(251), NoopTask::class.java)
      }

    Assertions.assertEquals("Task ID must be less than 250 characters, not 251", error.message)
  }
}
