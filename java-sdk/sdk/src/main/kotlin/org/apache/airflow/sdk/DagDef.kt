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

import org.apache.airflow.sdk.internal.GROUP_ID
import org.apache.airflow.sdk.internal.SchemaFields
import org.apache.airflow.sdk.internal.checkConfigValue
import org.apache.airflow.sdk.internal.validateTaskInput
import kotlin.Throws

/**
 * A collection of tasks with directional dependencies.
 *
 * Create a [DagDef] directly and register [TaskDef]s with [addTask].
 *
 * The [Builder.Dag] annotation should generally be preferred in user code,
 * where the annotation processor generates the wiring for you. Only use this
 * class directly if you need to do low-level plumbing:
 *
 * ```java
 * var dag = new DagDef("java_etl").config("schedule", "@daily");
 * var extract = dag.task("extract", Extract.class).config("retries", 2);
 * extract.before(dag.task("load", Load.class));
 * ```
 *
 * @param id Dag identifier. Must contain only ASCII alphanumeric characters,
 *    dashes, dots, or underscores; must be unique within a [Bundle].
 *
 * @see Builder.Dag
 */
class DagDef(
  val id: String, // TODO: charset check?
) {
  internal val tasks = linkedMapOf<String, TaskDef>()
  internal val dagConfig = linkedMapOf<String, Any>()

  /** Task groups keyed by their full ID, parents before the groups nested in them. */
  internal val groups = linkedMapOf<String, TaskGroupRef>()

  /** Edges with a task group at either end, in the order drawn. */
  internal val groupEdges = linkedSetOf<Pair<Endpoint, Endpoint>>()

  /**
   * Sets one Dag-level configuration value.
   *
   * Keys are Airflow's own Dag setting names (for example `"schedule"`,
   * `"description"`, `"tags"`, `"catchup"`); unknown keys and
   * mismatched value types are rejected on the call, so mistakes surface where
   * the Dag is defined.
   *
   * @param key Airflow Dag setting name.
   * @param value Value matching the key's schema type. Durations take
   *    [java.time.Duration], date-times [java.time.OffsetDateTime] or
   *    [java.time.Instant], string arrays any `Iterable` of `String`.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): DagDef {
    dagConfig[key] = checkConfigValue("Dag", SchemaFields.DAG, key, value)
    return this
  }

  /**
   * Registers a task from its ID and implementation class.
   *
   * @param id Task identifier, unique within this Dag.
   * @param definition Class that implements [Task]. Must have a public no-arg
   *    constructor.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if a task with the same ID is already
   *    registered.
   */
  fun addTask(
    id: String,
    definition: Class<out Task>,
  ): DagDef = addTask(TaskDef(id, definition))

  /**
   * Creates a task, registers it, and hands back its handle, ready to carry
   * configuration and to wire edges with [Deps.Flow.before].
   *
   * ```java
   * var extract = dag.task("extract", Extract.class).config("retries", 2);
   * var load = dag.task("load", Load.class);
   * extract.before(load);
   * ```
   *
   * @param id Task identifier, unique within this Dag.
   * @param definition Class that implements [Task]. Must have a public no-arg
   *    constructor.
   * @return The handle representing this task.
   * @throws IllegalArgumentException if a task with the same ID is already
   *    registered.
   */
  fun <T> task(
    id: String,
    definition: Class<out Task>,
  ): TaskRef<T> {
    val def = TaskDef(id, definition)
    addTask(def)
    return TaskRef(def)
  }

  /**
   * Registers a task with this Dag.
   *
   * A [TaskDef] belongs to at most one [DagDef]; registering the same instance
   * with a second Dag, or twice with the same one, fails. Task IDs must be
   * unique within a Dag. Tasks named as upstreams by [Deps.Flow.before] or
   * [Deps.Flow.after] must be registered with the same Dag by the time it is
   * added to a [Bundle].
   *
   * @param task Task definition to register.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if the task already belongs to a Dag or a
   *    task with the same ID is already registered.
   */
  fun addTask(task: TaskDef): DagDef {
    task.owner?.let { owner ->
      throw IllegalArgumentException("Task '${task.id}' already belongs to Dag '${owner.id}'")
    }
    require(task.id !in groups) { "Dag '$id' already has a task group with ID: ${task.id}" }
    require(tasks.putIfAbsent(task.id, task) == null) {
      "Tasks in Dag have duplicate ID: ${task.id}"
    }
    task.owner = this
    return this
  }

  /**
   * Declares a task group of this Dag.
   *
   * ```java
   * var staging = dag.taskGroup("staging");
   * var stage = staging.task("stage", Stage.class); // task "staging.stage"
   * extract.before(staging);
   * ```
   *
   * @param id Group ID. Must contain only ASCII letters, digits, underscores,
   *    or dashes, and differ from every task and group ID in this Dag.
   * @return The group, to declare tasks in and to wire edges with.
   * @throws IllegalArgumentException if [id] is not a valid group ID, or the
   *    Dag already has a task or task group with that ID.
   */
  fun taskGroup(id: String): TaskGroupRef = addGroup(null, id)

  internal fun addGroup(
    parent: TaskGroupRef?,
    localId: String,
  ): TaskGroupRef {
    require(GROUP_ID.matches(localId)) {
      "Task group ID '$localId' must contain only ASCII letters, digits, underscores, or dashes"
    }
    val groupId = parent?.qualify(localId) ?: localId
    require(groupId !in tasks && groupId !in groups) {
      "Dag '$id' already has a task or task group with ID: $groupId"
    }
    return TaskGroupRef(this, groupId, parent).also {
      groups[groupId] = it
      parent?.children?.add(it)
    }
  }

  /**
   * What this Dag's task-group edges mean in terms of tasks.
   *
   * A group upstream stands for its leaves and a group downstream for its
   * roots. Edges are read in the order they were drawn, each seeing the ones
   * before it, which is how Python resolves a group's endpoints at every
   * `>>`. The result is computed on demand and stored nowhere, so a task
   * added to a group after the Dag was registered still counts.
   */
  internal fun expandGroupEdges(): GroupExpansion {
    val upstreams = mutableMapOf<String, MutableSet<String>>()
    val edges = mutableMapOf<String, MutableGroupEdges>()

    fun edgesOf(groupId: String) = edges.getOrPut(groupId) { MutableGroupEdges() }

    fun upstreamIds(def: TaskDef): Set<String> = def.upstreams.mapTo(linkedSetOf()) { it.id } + upstreams[def.id].orEmpty()

    fun roots(group: TaskGroupRef): List<TaskDef> {
      val members = group.nodes()
      val ids = members.mapTo(mutableSetOf()) { it.id }
      return members.filter { task -> upstreamIds(task).none { it in ids } }
    }

    fun leaves(group: TaskGroupRef): List<TaskDef> {
      val members = group.nodes()
      val ids = members.mapTo(mutableSetOf()) { it.id }
      val fedInside = members.flatMapTo(mutableSetOf()) { task -> upstreamIds(task).filter { it in ids } }
      return members.filter { it.id !in fedInside }
    }

    // Python's find_leaves: the group's own leaves, else whatever already runs
    // before it, else the group it is nested in.
    fun leavesOf(endpoint: Endpoint): List<TaskDef> =
      when (endpoint) {
        is TaskDef -> listOf(endpoint)
        is TaskGroupRef -> {
          var group: TaskGroupRef? = endpoint
          var found: List<TaskDef> = emptyList()
          while (group != null && found.isEmpty()) {
            found = leaves(group).ifEmpty { edgesOf(group.id).upstreamTaskIds.map { tasks.getValue(it) } }
            group = group.parent
          }
          found
        }
      }

    fun rootsOf(endpoint: Endpoint): List<TaskDef> =
      when (endpoint) {
        is TaskDef -> listOf(endpoint)
        is TaskGroupRef -> roots(endpoint)
      }

    for ((upstream, downstream) in groupEdges) {
      val from = leavesOf(upstream).map { it.id }
      rootsOf(downstream).forEach { task -> upstreams.getOrPut(task.id) { linkedSetOf() } += from }
      if (downstream is TaskGroupRef) {
        edgesOf(downstream.id).upstreamTaskIds += from
        if (upstream is TaskGroupRef) edgesOf(downstream.id).upstreamGroupIds += upstream.id
      }
      // When both ends are groups, the upstream records the downstream group
      // only, not its tasks, which is how Python leaves it.
      when {
        upstream is TaskGroupRef && downstream is TaskGroupRef ->
          edgesOf(upstream.id).downstreamGroupIds += downstream.id
        upstream is TaskGroupRef && downstream is TaskDef ->
          edgesOf(upstream.id).downstreamTaskIds += downstream.id
      }
    }
    return GroupExpansion(upstreams, edges)
  }
}

/**
 * The task edges a Dag's task-group edges stand for, and the edges each group
 * records for itself, as [DagDef.expandGroupEdges] worked them out.
 */
internal class GroupExpansion(
  private val upstreams: Map<String, Set<String>>,
  private val edges: Map<String, GroupEdges>,
) {
  /** Every task [def] runs after: the edges it carries, plus the ones a group edge implies. */
  fun upstreamsOf(def: TaskDef): Set<String> = def.upstreams.mapTo(linkedSetOf()) { it.id } + upstreams[def.id].orEmpty()

  /** The edges the group with full ID [groupId] records for itself. */
  fun edgesOf(groupId: String): GroupEdges = edges[groupId] ?: EMPTY_GROUP_EDGES
}

/** One task group's own edges, as Python's `TaskGroup` records them. */
internal open class GroupEdges {
  open val upstreamGroupIds: Set<String> = emptySet()
  open val downstreamGroupIds: Set<String> = emptySet()
  open val upstreamTaskIds: Set<String> = emptySet()
  open val downstreamTaskIds: Set<String> = emptySet()
}

private class MutableGroupEdges : GroupEdges() {
  override val upstreamGroupIds = linkedSetOf<String>()
  override val downstreamGroupIds = linkedSetOf<String>()
  override val upstreamTaskIds = linkedSetOf<String>()
  override val downstreamTaskIds = linkedSetOf<String>()
}

private val EMPTY_GROUP_EDGES = GroupEdges()

/**
 * One task definition: its ID, the class that implements it, its upstream
 * dependencies, and its task-level configuration.
 *
 * Edges are drawn on the handles that [DagDef.task] returns, not here:
 *
 * ```java
 * var dag = new DagDef("java_etl");
 * dag.addTask(new TaskDef("extract", Extract.class).config("retries", 2));
 * ```
 *
 * @param id Task identifier, unique within a [DagDef].
 * @param definition Class that implements [Task]. Must have a public no-arg
 *    constructor.
 * @throws IllegalArgumentException if [definition] is an [InputTask] whose
 *    declared input cannot be bound, so that a mis-declared input fails while
 *    the [Bundle] is built rather than mid-run.
 *
 * @see Builder.Task
 */
class TaskDef(
  val id: String,
  val definition: Class<out Task>,
) : Endpoint {
  init {
    validateTaskInput(definition)
  }

  internal val configValues = linkedMapOf<String, Any>()
  internal val inputs = mutableListOf<Arg<*>>()
  internal val upstreams = linkedSetOf<TaskDef>()
  internal var owner: DagDef? = null

  /**
   * Sets one task-level configuration value.
   *
   * Keys are Airflow's own task setting names (for example `"retries"`,
   * `"queue"`, `"retry_delay"`); unknown keys and mismatched
   * value types are rejected on the call, so mistakes surface where the task is
   * defined.
   *
   * @param key Airflow task setting name, e.g. `"retries"`.
   * @param value Value matching the key's schema type. Durations take
   *    [java.time.Duration], date-times [java.time.OffsetDateTime] or
   *    [java.time.Instant].
   * @return This task definition, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): TaskDef {
    configValues[key] = checkConfigValue("task", SchemaFields.TASK, key, value)
    return this
  }

  /** Records that this task runs after [upstreams], backing [Deps.Flow.before] and [Deps.Flow.after]. */
  internal fun dependsOn(vararg upstreams: TaskDef): TaskDef {
    this.upstreams += upstreams
    return this
  }
}

/**
 * A single unit of work executed by Airflow.
 *
 * Prefer using the [Builder.Task] annotation with [Builder.Dag] to have the
 * annotation processor generate an implementation for you. Only use this
 * interface if you need to do low-level plumbing.
 *
 * Implement this interface to define task logic. Airflow instantiates the class
 * via its no-argument constructor, then calls [execute] once per task-instance
 * run.
 *
 * Implement [InputTask] instead for a task the Python Dag file calls with
 * TaskFlow arguments; the SDK then resolves those arguments and injects them.
 *
 * @see Builder.Dag
 * @see Builder.Task
 * @see InputTask
 */
interface Task {
  /**
   * Executes this task.
   *
   * Any exception thrown marks the task instance as failed. Use [client] to
   * read connections, variables, pull XComs, or to push an XCom for downstream
   * tasks.
   *
   * @param context Runtime context for the current execution workload.
   * @param client Client for Airflow API calls scoped to this exxecution.
   * @throws Exception on failure; the task instance is marked failed.
   */
  @Throws(Exception::class)
  fun execute(
    context: Context,
    client: Client,
  )
}
