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

import org.apache.airflow.sdk.ArgName
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.InputTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.execution.comm.TaskHandlerParseRequest
import org.apache.airflow.sdk.internal.TaskParams
import org.apache.airflow.sdk.internal.TypeRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

private val NULLABLE_STRING = mapOf("anyOf" to listOf(mapOf("type" to "string"), mapOf("type" to "null")))

/** Stands in for a class the annotation processor generates for a handler with flat data parameters. */
private class GeneratedFlat : Task {
  companion object {
    @JvmField
    val AIRFLOW_TASK_PARAMS: TaskParams =
      TaskParams.of(
        TaskParams.param("rows", java.lang.Long.TYPE),
        TaskParams.param("regions", object : TypeRef<List<String>>() {}),
      )
  }

  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** Stands in for a generated class whose handler takes only the injected Client and Context. */
private class GeneratedNoData : Task {
  companion object {
    @JvmField
    val AIRFLOW_TASK_PARAMS: TaskParams = TaskParams.of()
  }

  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private open class BaseScoreInput : TaskInput {
  @JvmField
  var threshold: Double = 0.0
}

private class ScoreInput : BaseScoreInput() {
  @JvmField
  @ArgName("region_code")
  var region: String? = null
}

/** Stands in for a generated class whose handler takes a [TaskInput]. */
private class GeneratedInput : Task {
  companion object {
    @JvmField
    val AIRFLOW_TASK_PARAMS: TaskParams = TaskParams.input(ScoreInput::class.java)
  }

  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private class Score : InputTask<ScoreInput> {
  override fun execute(
    context: Context,
    client: Client,
    input: ScoreInput,
  ) = Unit
}

private class Audit : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private fun request(vararg dagIds: String) =
  TaskHandlerParseRequest().apply {
    file = "/bundles/java/etl.jar"
    this.dagIds = dagIds.toList()
    bundlePath = "/bundles/java"
    bundleName = "java-task-handlers"
  }

private fun param(
  name: String,
  schema: Map<String, Any?>,
  required: Boolean = false,
  exactName: Boolean = false,
) = mapOf("name" to name, "required" to required, "exact_name" to exactName, "value_schema" to schema)

private val SCORE_INPUT_PARAMS =
  listOf(
    param("region_code", NULLABLE_STRING, exactName = true),
    param("threshold", mapOf("type" to "number", "format" to "double")),
  )

class TaskHandlerParseTest {
  @Test
  @DisplayName("Should declare each requested Dag's task handlers in registration order")
  fun declaresRequestedDagsInRegistrationOrder() {
    val bundle =
      Bundle()
        .register("etl", "extract", GeneratedFlat::class.java)
        .register("etl", "score", GeneratedInput::class.java)
        .register("etl", "summarize", Score::class.java)
        .register("etl", "audit", Audit::class.java)
        .register("etl", "notify", GeneratedNoData::class.java)
        .register("report", "audit", Audit::class.java)
        .register(DagDef("native").addTask("audit", Audit::class.java))

    val result = parseTaskHandlers(bundle, request("etl", "native", "missing", "etl"))

    val regions =
      mapOf("anyOf" to listOf(mapOf("type" to "array", "items" to NULLABLE_STRING), mapOf("type" to "null")))
    val expected =
      mapOf(
        "type" to "TaskHandlerParsingResult",
        "fileloc" to "/bundles/java/etl.jar",
        "task_handlers" to
          mapOf(
            "etl" to
              listOf(
                mapOf(
                  "task_id" to "extract",
                  "binding" to "positional",
                  "params" to
                    listOf(
                      param("rows", mapOf("type" to "integer", "format" to "int64"), required = true),
                      param("regions", regions, required = true),
                    ),
                ),
                mapOf("task_id" to "score", "binding" to "named", "params" to SCORE_INPUT_PARAMS),
                mapOf("task_id" to "summarize", "binding" to "named", "params" to SCORE_INPUT_PARAMS),
                mapOf("task_id" to "audit", "binding" to "named", "params" to emptyList<Any>()),
                mapOf("task_id" to "notify", "binding" to "named", "params" to emptyList<Any>()),
              ),
          ),
      )
    assertEquals(expected, result)
  }

  @Test
  @DisplayName("Should send an empty map when no requested Dag has task handlers")
  fun sendsEmptyTaskHandlersWhenNothingMatches() {
    val bundle = Bundle().register("etl", "audit", Audit::class.java)

    val result = parseTaskHandlers(bundle, request("other"))

    assertEquals(emptyMap<String, Any?>(), result["task_handlers"])
  }
}
