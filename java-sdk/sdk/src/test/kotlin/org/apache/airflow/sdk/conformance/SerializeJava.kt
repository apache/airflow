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

// Serializes the shared test Dags with this SDK, for
// scripts/ci/lang_sdk_serialization/compare.py:
//
//   java -cp <sdk test runtime classpath> org.apache.airflow.sdk.conformance.SerializeJavaKt \
//       scripts/ci/lang_sdk_serialization/test_dags.yaml serialized_java.json
//
// Each Dag is built with the interface API and written as the runtime answers
// a parse request, keyed by Dag ID. A task's `upstream` becomes an ordering
// edge, which a Java task serializes the same way as a data edge.
package org.apache.airflow.sdk.conformance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.BranchRef
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.ConditionRef
import org.apache.airflow.sdk.ConditionTask
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.Deps
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskGroupRef
import org.apache.airflow.sdk.TaskId
import org.apache.airflow.sdk.TaskIdBranchTask
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.execution.serializeDag
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.FieldType
import org.apache.airflow.sdk.internal.SchemaFields
import java.io.File
import java.time.Duration
import java.time.OffsetDateTime

class ConformanceTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceCondition : ConditionTask {
  override fun decide(
    context: Context,
    client: Client,
  ) = true
}

// Names its case by task ID: every conformance task runs the same class, so a
// branch that named a case by class could not tell two of them apart.
class ConformanceBranch : TaskIdBranchTask {
  override fun choose(
    context: Context,
    client: Client,
  ) = TaskId.of("")
}

fun main(args: Array<String>) {
  require(args.size == 2) { "usage: SerializeJava <test_dags.yaml> <output.json>" }
  val cases = YAMLMapper().readTree(File(args[0])).path("dags")
  val bundle = Bundle()
  cases.forEach { bundle.register(buildDag(it)) }
  val serialized =
    bundle.dags.values.associate { dag ->
      dag.id to mapOf("__version" to 3, "dag" to serializeDag(dag, "", "."))
    }
  ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(File(args[1]), serialized)
}

private fun buildDag(case: JsonNode): DagDef {
  val dag = DagDef(case.path("dag_id").asText())
  case.path("spec").fields().forEach { (key, value) -> dag.config(key, toValue(SchemaFields.DAG, key, value)) }

  // A group ID is fully qualified, so its parent is whatever comes before the last dot.
  val groups = linkedMapOf<String, TaskGroupRef>()
  case.path("groups").forEach { node ->
    val groupId = node.asText()
    val parentId = groupId.substringBeforeLast('.', "")
    val localId = groupId.substringAfterLast('.')
    groups[groupId] = if (parentId.isEmpty()) dag.taskGroup(localId) else groups.getValue(parentId).taskGroup(localId)
  }

  val tasks = linkedMapOf<String, TaskRef<*>>()
  // A decider names tasks that may be declared after it, so its cases are wired once every task exists.
  val deciders = mutableListOf<Pair<Deps.Flow, JsonNode>>()
  case.path("tasks").forEach { task ->
    val groupId = task.path("group").asText("")
    val localId = task.path("task_id").asText()
    val branch = task.path("branch")
    val definition =
      when {
        branch.isMissingNode -> ConformanceTask::class.java
        branch.has("cases") -> ConformanceBranch::class.java
        else -> ConformanceCondition::class.java
      }
    val ref =
      if (groupId.isEmpty()) {
        dag.task<Any?>(localId, definition)
      } else {
        groups.getValue(groupId).task<Any?>(localId, definition)
      }
    if (!branch.isMissingNode) {
      deciders += (if (branch.has("cases")) BranchRef.of(ref) else asCondition(ref)) to branch
    }
    task.path("spec").fields().forEach { (key, value) -> ref.config(key, toValue(SchemaFields.TASK, key, value)) }
    // A task's `upstream` handles and its `literals` are its call arguments, in that order, so the
    // Dag carries the binding spec a stub call would. Names are positional, as the Go SDK names
    // them: the interface API has no signature to read parameter names from.
    val inputs: List<Arg<*>> =
      task.path("upstream").map { tasks.getValue(it.asText()) } +
        task.path("literals").map { Arg.lit(toJsonValue(it)) }
    inputs.filterIsInstance<TaskRef<*>>().forEach { ref.after(it) }
    ref.def.inputs += inputs
    ref.def.inputNames += inputs.indices.map { "arg$it" }
    tasks[ref.def.id] = ref
  }

  deciders.forEach { (decider, branch) ->
    when (decider) {
      is BranchRef -> branch.path("cases").forEach { decider.option(tasks.getValue(it.asText())) }
      is ConditionRef -> {
        decider.then(tasks.getValue(branch.path("then").asText()))
        branch.path("else").takeIf { !it.isMissingNode }?.let { decider.orElse(tasks.getValue(it.asText())) }
      }
      else -> throw IllegalStateException("Unknown decider: $decider")
    }
  }

  case.path("order_edges").forEach { edge ->
    val node = { id: String -> groups[id] as Deps.Flow? ?: tasks.getValue(id) }
    node(edge[0].asText()).before(node(edge[1].asText()))
  }
  return dag
}

/** The handle of a task declared as a decider, whose type argument no caller reads. */
@Suppress("UNCHECKED_CAST")
private fun asCondition(ref: TaskRef<*>): ConditionRef = ConditionRef.of(ref as TaskRef<Boolean>)

/** Reads a YAML value as the Java type the config key takes. */
private fun toValue(
  table: Map<String, Field>,
  key: String,
  node: JsonNode,
): Any =
  when (requireNotNull(table[key]) { "Unknown config key: '$key'" }.type) {
    FieldType.STRING -> node.asText()
    FieldType.BOOLEAN -> node.asBoolean()
    FieldType.INTEGER -> node.asInt()
    FieldType.NUMBER -> node.numberValue()
    // `!datetime` is an ISO 8601 timestamp, and `!timedelta` a number of seconds.
    FieldType.DATETIME -> OffsetDateTime.parse(node.asText())
    FieldType.TIMEDELTA -> Duration.ofNanos((node.asText().toDouble() * 1e9).toLong())
    FieldType.STRING_ARRAY -> node.map { it.asText() }
  }

/** Reads a YAML literal as the plain value `Serde` writes out. */
private fun toJsonValue(node: JsonNode): Any? =
  when {
    node.isNull -> null
    node.isTextual -> node.asText()
    node.isBoolean -> node.asBoolean()
    node.isIntegralNumber -> node.numberValue()
    node.isNumber -> node.asDouble()
    node.isArray -> node.map { toJsonValue(it) }
    else -> node.fields().asSequence().associate { (key, value) -> key to toJsonValue(value) }
  }
