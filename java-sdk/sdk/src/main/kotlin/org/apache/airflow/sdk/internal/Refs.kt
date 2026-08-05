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

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.In
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskRef

/**
 * Registration hooks called by processor-generated twin classes. Public
 * so that generated code can call them; not user-facing API.
 */
object Refs {
  /**
   * Registers one task from a flow-twin call: records [inputs] as the task's
   * data parameters in declaration order, wires a dependency edge for every
   * upstream [TaskRef] among them, and adds the task to [dag].
   *
   * @return The handle representing this task's output.
   */
  @JvmStatic
  fun <T> register(
    dag: DagDef,
    def: TaskDef,
    inputs: List<In<*>>,
  ): TaskRef<T> {
    inputs.filterIsInstance<TaskRef<*>>().forEach { def.dependsOn(it.def) }
    def.inputs += inputs
    dag.addTask(def)
    return TaskRef(def)
  }

  /**
   * Verifies that the user's `@Wiring` method registered every
   * `@Builder.Task` method of the Dag class.
   *
   * @throws IllegalArgumentException naming the tasks the wiring missed.
   */
  @JvmStatic
  fun requireRegistered(
    dag: DagDef,
    taskIds: List<String>,
  ) {
    val missing = taskIds.filterNot { it in dag.tasks }
    require(missing.isEmpty()) {
      "Wiring for Dag '${dag.id}' did not register task(s) ${missing.joinToString { "'$it'" }}: " +
        "every @Builder.Task method must be invoked in the @Wiring method"
    }
  }
}
