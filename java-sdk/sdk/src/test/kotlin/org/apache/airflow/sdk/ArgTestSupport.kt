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

import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.apache.airflow.sdk.internal.Refs
import org.apache.airflow.sdk.execution.comm.TaskInstance as CommTaskInstance

/** Records getXCom calls and serves canned values keyed by task id. */
internal class FakeXComTransport(
  val xcoms: Map<String, Any?> = emptyMap(),
) : org.apache.airflow.sdk.execution.Client {
  val pulls = mutableListOf<Pair<String, Int?>>()

  override fun getConnection(id: String): ConnectionResult = throw NotImplementedError()

  override fun getVariable(key: String): VariableResult = throw NotImplementedError()

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResult {
    pulls += taskId to mapIndex
    return XComResult().also {
      it.key = key
      it.value = xcoms[taskId]
    }
  }

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) = throw NotImplementedError()
}

internal fun startupDetails(argBindings: List<Map<String, Any?>>?): StartupDetails =
  StartupDetails().also { details ->
    details.ti =
      CommTaskInstance().also {
        it.dagId = "d"
        it.runId = "r"
        it.taskId = "t"
        it.tryNumber = 1
      }
    details.tiContext = TIRunContext().also { it.argBindings = argBindings }
  }

internal fun clientWith(
  argBindings: List<Map<String, Any?>>?,
  xcoms: Map<String, Any?> = emptyMap(),
): Pair<Client, FakeXComTransport> {
  val transport = FakeXComTransport(xcoms)
  return Client(startupDetails(argBindings), transport) to transport
}

internal fun taskContext(): Context =
  Context(
    dagRun = DagRun("d", "r", null, null, null, null, null, emptyMap()),
    ti = TaskInstance("d", "r", "t", null, 1),
  )

internal class NoopTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** A context whose task was Java-wired with the given inputs. */
internal fun contextWiredWith(inputs: List<In<*>>): Context {
  val def = TaskDef("t", NoopTask::class.java)
  Refs.register<Unit>(DagDef("d"), def, inputs)
  return taskContext().also { it.taskDef = def }
}
