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

/**
 * What one deciding task can run, and what it therefore skips.
 *
 * A [TaskDef] carrying one is a decider: the runtime asks it which of its
 * [cases] the task chose, skips the rest, and the serialized task carries
 * `_can_skip_downstream` so Airflow re-skips a cleared one.
 */
internal interface DeciderDef {
  /** Every task this decider can run, in the order they were named. */
  val cases: List<TaskDef>

  /** Why this decider is incomplete, or null when it is ready to register. */
  fun describe(decider: TaskDef): String?

  /**
   * Runs the deciding task [instance] and reports what it chose.
   *
   * @throws Exception whatever the task body throws, which fails the task
   *    without pushing or skipping anything.
   */
  fun decide(
    decider: TaskDef,
    instance: Task,
    context: Context,
    client: Client,
  ): Decision
}

/**
 * What a deciding task chose: the value it pushes as its return value, and
 * the task IDs of the cases it did not take.
 */
internal class Decision(
  val value: Any,
  val skipped: List<String>,
)

/** Mirrors `SkipMixin.skip` in `task-sdk/src/airflow/sdk/bases/skipmixin.py`. */
internal const val SKIPMIXIN_XCOM_KEY = "skipmixin_key"
internal const val SKIPMIXIN_SKIPPED = "skipped"

/**
 * Rejects a case named after the Dag was registered, which the checks that run
 * at registration would never see.
 */
internal fun requireUnregistered(
  decider: TaskDef,
  what: String,
) {
  val owner = decider.owner
  require(owner?.registered != true) {
    "$what '${decider.id}' of Dag '${owner?.id}' is already registered; name every case before " +
      "the Dag is added to a Bundle"
  }
}

/** Rejects a case that belongs to another Dag, which no edge of this one can reach. */
internal fun requireSameDag(
  decider: TaskDef,
  case: TaskDef,
  what: String,
) {
  val owner = decider.owner
  val caseOwner = case.owner
  require(owner == null || caseOwner == null || owner === caseOwner) {
    "$what '${decider.id}' of Dag '${owner?.id}' cannot run task '${case.id}' of Dag " +
      "'${caseOwner?.id}'; name a task of the same Dag"
  }
}
