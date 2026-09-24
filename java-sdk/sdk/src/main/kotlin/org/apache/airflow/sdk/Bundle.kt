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

import org.apache.airflow.sdk.internal.registrarName

/**
 * All [DagDef]s that this JVM process can execute.
 *
 * Register everything before passing the bundle to [Server.serve]: serving
 * ends registration, so a `register` left below it fails rather than racing
 * the running task.
 *
 * @property dags Dags declared in Java, keyed by [DagDef.id].
 * @throws IllegalArgumentException if any two Dags share the same ID.
 */
class Bundle(
  dags: Iterable<DagDef>,
) {
  /** Dags declared in Java, which own their own tasks. */
  internal val dags = linkedMapOf<String, DagDef>()

  /** Dags the Python file owns, holding the task handlers registered for them. */
  internal val taskHandlers = linkedMapOf<String, DagDef>()

  @Volatile
  private var served = false

  /** Creates an empty bundle to [register] into. */
  constructor() : this(emptyList())

  init {
    dags.forEach { register(it) }
  }

  /**
   * Registers a Dag.
   *
   * @return This bundle, for chaining.
   * @throws IllegalArgumentException if another Dag shares its ID, or task
   *    handlers are already registered against it.
   * @throws IllegalStateException if [Server.serve] has already been called.
   */
  fun register(dag: DagDef): Bundle {
    checkOpen()
    require(dag.id !in taskHandlers) {
      "Dag '${dag.id}' already has registered task handlers; a Dag declared in Java owns its " +
        "own tasks, so one Dag ID cannot have both"
    }
    require(dags.putIfAbsent(dag.id, dag) == null) {
      "Dags in bundle have duplicate ID: ${dag.id}"
    }
    return this
  }

  /**
   * Registers every task handler a class holds, from the ids each
   * [Builder.TaskHandler] names.
   *
   * @param handlerClass A class with [Builder.TaskHandler] methods.
   * @return This bundle, for chaining.
   * @throws IllegalArgumentException if the class has no generated
   *    registrar, because annotation processing did not run over it.
   */
  fun register(handlerClass: Class<*>): Bundle {
    checkOpen()
    val name = registrarName(handlerClass.name)
    val registrar =
      try {
        Class.forName(name, true, handlerClass.classLoader)
      } catch (e: ClassNotFoundException) {
        throw IllegalArgumentException(
          "No generated registrar $name for ${handlerClass.name}; does it declare " +
            "@Builder.TaskHandler methods, and is airflow-sdk-processor on the " +
            "annotationProcessor path?",
          e,
        )
      }
    registrar.getMethod("registerInto", Bundle::class.java).invoke(null, this)
    return this
  }

  /**
   * Registers one task implementation against a Dag the Python file owns, for
   * a task with no annotation to read the ids from.
   *
   * The Dag is created on first use: a stub-backed Dag exists only so the
   * runtime can find the task, and its graph lives in the Python Dag file.
   *
   * @param dagId Dag ID as declared in the Python Dag file.
   * @param taskId Task ID as declared by the `@task.stub` function.
   * @param definition Class that implements [Task].
   * @return This bundle, for chaining.
   * @throws IllegalArgumentException if a Dag declared in Java already holds
   *    that ID.
   * @throws IllegalStateException if [Server.serve] has already been called.
   */
  fun register(
    dagId: String,
    taskId: String,
    definition: Class<out Task>,
  ): Bundle {
    checkOpen()
    require(dagId !in dags) {
      "Dag '$dagId' is declared in Java; attach its tasks with addTask(...) rather than " +
        "registering task handlers for them"
    }
    taskHandlers.getOrPut(dagId) { DagDef(dagId) }.addTask(taskId, definition)
    return this
  }

  /** The task to run for a request, from whichever side registered its Dag. */
  internal fun taskDef(
    dagId: String,
    taskId: String,
  ): TaskDef? = (dags[dagId] ?: taskHandlers[dagId])?.tasks?.get(taskId)

  /**
   * Ends registration, so a `register` left below `serve` is reported as the
   * mistake it is rather than racing the runtime. [Server] calls it when it
   * starts serving, whatever the run turns out to do.
   */
  internal fun finalizeRegistration() {
    served = true
  }

  private fun checkOpen() = check(!served) { "Server.serve has already been called; register everything before serve" }
}

/**
 * Entry point for declaring the [DagDef]s that this bundle contains.
 *
 * Implement this interface to create a Dag bundle to be served by [Server].
 *
 * ```java
 * public class MyBundleBuilder implements BundleBuilder {
 *     @Override
 *     public Iterable<DagDef> getDags() {
 *         return List.of(MyDagBuilder.build());
 *     }
 *
 *     public static void main(String[] args) {
 *         Server.create(args).serve(new MyBundleBuilder().build());
 *     }
 * }
 * ```
 */
interface BundleBuilder {
  /**
   * Returns all [DagDef]s that belong to this bundle.
   *
   * Called once during [build]; Dag IDs must be unique across the returned
   * collection.
   *
   * @throws IllegalArgumentException if any two Dags share the same ID.
   */
  fun getDags(): Iterable<DagDef>

  /**
   * Constructs a [Bundle] from the Dags returned by [getDags].
   *
   * @throws IllegalArgumentException if any two Dags share the same ID.
   */
  fun build(): Bundle = Bundle(getDags())
}
