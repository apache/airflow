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
 * An immutable snapshot of all [DagDef]s that this JVM process can execute.
 *
 * Build a [Bundle] by implementing [BundleBuilder], then pass it to
 * [Server.serve] to start accepting task-execution requests.
 *
 * @property dags All registered Dags keyed by [DagDef.id].
 * @throws IllegalArgumentException if any two Dags share the same ID.
 */
class Bundle(
  dags: Iterable<DagDef>,
) {
  internal val dags = linkedMapOf<String, DagDef>()

  /** Creates an empty bundle to [register] into. */
  constructor() : this(emptyList())

  init {
    dags.forEach { register(it) }
  }

  /**
   * Registers a Dag.
   *
   * @return This bundle, for chaining.
   * @throws IllegalArgumentException if another Dag shares its ID.
   */
  fun register(dag: DagDef): Bundle {
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
    val registrar =
      try {
        Class.forName("${handlerClass.name}Handlers", true, handlerClass.classLoader)
      } catch (e: ClassNotFoundException) {
        throw IllegalArgumentException(
          "No generated registrar for ${handlerClass.name}; does it declare @Builder.TaskHandler " +
            "methods, and is airflow-sdk-processor on the annotationProcessor path?",
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
   */
  fun register(
    dagId: String,
    taskId: String,
    definition: Class<out Task>,
  ): Bundle {
    dags.getOrPut(dagId) { DagDef(dagId) }.addTask(taskId, definition)
    return this
  }
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
