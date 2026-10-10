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
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.ConditionRef
import org.apache.airflow.sdk.ConditionTask
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.Deps
import org.apache.airflow.sdk.SwitchRef
import org.apache.airflow.sdk.SwitchTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskGroupRef
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.TriggerDagRun
import org.apache.airflow.sdk.execution.serializeDag
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.FieldType
import org.apache.airflow.sdk.internal.SchemaFields
import java.io.File
import java.time.Duration
import java.time.OffsetDateTime

// A switch names its cases by class, so each task of a Dag runs a class of its own. The pool has
// room for the largest Dag in test_dags.yaml and a few more; the serialized task type is not compared.
class ConformanceTask1 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask2 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask3 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask4 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask5 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask6 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask7 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask8 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask9 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask10 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask11 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ConformanceTask12 : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private val taskPool: List<Class<out Task>> =
  listOf(
    ConformanceTask1::class.java,
    ConformanceTask2::class.java,
    ConformanceTask3::class.java,
    ConformanceTask4::class.java,
    ConformanceTask5::class.java,
    ConformanceTask6::class.java,
    ConformanceTask7::class.java,
    ConformanceTask8::class.java,
    ConformanceTask9::class.java,
    ConformanceTask10::class.java,
    ConformanceTask11::class.java,
    ConformanceTask12::class.java,
  )

class ConformanceCondition : ConditionTask {
  override fun decide(
    context: Context,
    client: Client,
  ) = true
}

// Never run, only serialized, so any task class will do.
class ConformanceSwitch : SwitchTask {
  override fun choose(
    context: Context,
    client: Client,
  ): Class<out Task> = ConformanceTask1::class.java
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
  val unusedTaskClasses = taskPool.iterator()
  // A decider names tasks that may be declared after it, so its cases are wired once every task exists.
  val deciders = mutableListOf<Pair<Deps.Flow, JsonNode>>()
  case.path("tasks").forEach { task ->
    val groupId = task.path("group").asText("")
    val localId = task.path("task_id").asText()
    val branch = task.path("branch")
    val trigger = task.path("trigger_dag_run")
    val definition =
      when {
        branch.isMissingNode -> {
          require(unusedTaskClasses.hasNext()) {
            "Dag '${dag.id}' has more tasks than the ${taskPool.size} classes in taskPool; add a ConformanceTask class"
          }
          unusedTaskClasses.next()
        }
        branch.has("cases") -> ConformanceSwitch::class.java
        else -> ConformanceCondition::class.java
      }
    val ref =
      when {
        !trigger.isMissingNode ->
          triggerDagRun(trigger).let {
            if (groupId.isEmpty()) dag.task(localId, it) else groups.getValue(groupId).task(localId, it)
          }
        groupId.isEmpty() -> dag.task<Any?>(localId, definition)
        else -> groups.getValue(groupId).task<Any?>(localId, definition)
      }
    if (!branch.isMissingNode) {
      deciders += (if (branch.has("cases")) SwitchRef.of(ref) else asCondition(ref)) to branch
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
      is SwitchRef -> branch.path("cases").forEach { decider.Case(tasks.getValue(it.asText())) }
      is ConditionRef -> {
        decider.Then(tasks.getValue(branch.path("then").asText()))
        branch.path("else").takeIf { !it.isMissingNode }?.let { decider.Else(tasks.getValue(it.asText())) }
      }
      else -> throw IllegalStateException("Unknown decider: $decider")
    }
  }

  case.path("order_edges").forEach { edge ->
    val node = { id: String -> groups[id] as Deps.Flow? ?: tasks.getValue(id) }
    val downstream = node(edge[1].asText())
    // A third item is the label of the edge.
    val labeled = if (edge.size() > 2) Deps.Flow.label(downstream, edge[2].asText()) else downstream
    node(edge[0].asText()).before(labeled)
  }
  return dag
}

/** Reads the template fields of a TriggerDagRunOperator from a task of test_dags.yaml. */
private fun triggerDagRun(node: JsonNode): TriggerDagRun {
  val trigger = TriggerDagRun(node.path("trigger_dag_id").asText())
  node.fields().forEach { (key, value) ->
    when (key) {
      "trigger_dag_id" -> Unit
      "logical_date", "run_after" -> trigger.config(key, triggerDateTime(value.asText()))
      "conf" -> trigger.config(key, toJsonValue(value) as Map<*, *>)
      "wait_for_completion", "skip_when_already_exists", "reset_dag_run", "fail_when_dag_is_paused", "deferrable" ->
        trigger.config(key, value.asBoolean())
      "poke_interval" -> trigger.config(key, Duration.ofSeconds(value.asLong()))
      "allowed_states", "failed_states" -> trigger.config(key, value.map { it.asText() })
      else -> trigger.config(key, value.asText())
    }
  }
  return trigger
}

/**
 * A trigger task's `logical_date` or `run_after` from the fixture. The fixture spells one the way
 * Python spells a datetime, with a space, so the separator is normalised before parsing.
 */
private fun triggerDateTime(text: String): OffsetDateTime = OffsetDateTime.parse(text.replaceFirst(" ", "T"))

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
    FieldType.STRING_ARRAY, FieldType.DAG_RUN_STATES -> node.map { it.asText() }
    FieldType.JSON_OBJECT -> toJsonValue(node)!!
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
