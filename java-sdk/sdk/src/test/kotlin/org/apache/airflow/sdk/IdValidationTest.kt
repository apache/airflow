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
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

private fun dagTooLongWarning(
  id: String,
  length: Int,
) = IdWarning(
  "Dag id is longer than 250 characters; the Airflow server will reject it",
  mapOf("dag_id" to id, "length" to length),
)

private fun dagCharsetWarning(id: String) =
  IdWarning(
    "Dag id must be made of alphanumeric characters, dashes, dots, and underscores; " +
      "the Airflow server will reject it",
    mapOf("dag_id" to id),
  )

private fun dagDoubleDotWarning(id: String) =
  IdWarning(
    "Dag id contains '..'; the Airflow server will reject it " +
      "unless [core] allow_double_dot_in_ids is enabled",
    mapOf("dag_id" to id),
  )

internal class IdValidationTest {
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  @Test
  @DisplayName("dag id warnings — exact findings across every branch")
  fun dagIdWarnings() {
    val astral = "𠀀"
    val tooLongAndInvalid = "a".repeat(250) + " b"
    val cases: List<Pair<String, List<IdWarning>>> =
      listOf(
        "simple" to emptyList(),
        "with-dash" to emptyList(),
        "with.dot" to emptyList(),
        "with_underscore" to emptyList(),
        "0numeric" to emptyList(),
        "café_dag" to emptyList(),
        "任務" to emptyList(),
        "a".repeat(250) to emptyList(),
        astral.repeat(250) to emptyList(),
        "a".repeat(251) to listOf(dagTooLongWarning("a".repeat(251), 251)),
        "任".repeat(251) to listOf(dagTooLongWarning("任".repeat(251), 251)),
        astral.repeat(251) to listOf(dagTooLongWarning(astral.repeat(251), 251)),
        "with space" to listOf(dagCharsetWarning("with space")),
        "with/slash" to listOf(dagCharsetWarning("with/slash")),
        "with:colon" to listOf(dagCharsetWarning("with:colon")),
        "with\ttab" to listOf(dagCharsetWarning("with\ttab")),
        "a..b c" to listOf(dagCharsetWarning("a..b c")),
        "a..b" to listOf(dagDoubleDotWarning("a..b")),
        tooLongAndInvalid to
          listOf(dagTooLongWarning(tooLongAndInvalid, 252), dagCharsetWarning(tooLongAndInvalid)),
      )
    cases.forEach { (id, expected) ->
      assertEquals(expected, IdValidation.findSuspiciousIds(listOf(DagDef(id))), "id=$id")
    }
  }

  @Test
  @DisplayName("a task warning carries its dag id")
  fun taskWarningCarriesDagId() {
    val dag = DagDef("my_dag").addTask("bad task", NoOp::class.java)

    assertEquals(
      listOf(
        IdWarning(
          "Task id must be made of alphanumeric characters, dashes, dots, and underscores; " +
            "the Airflow server will reject it",
          mapOf("dag_id" to "my_dag", "task_id" to "bad task"),
        ),
      ),
      IdValidation.findSuspiciousIds(listOf(dag)),
    )
  }
}
