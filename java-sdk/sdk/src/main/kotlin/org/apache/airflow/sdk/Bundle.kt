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
 * An immutable snapshot of all [Dag]s that this JVM process can execute.
 *
 * Build a [Bundle] by implementing [BundleBuilder], then pass it to
 * [Server.serve] to start accepting task-execution requests.
 *
 * @property dags All registered Dags keyed by [Dag.id].
 * @throws IllegalArgumentException if any two Dags share the same ID.
 */
class Bundle(
  dags: Iterable<Dag>,
) {
  internal val dags: Map<String, Dag> = dags.associateByDagId()
}

private fun Iterable<Dag>.associateByDagId(): Map<String, Dag> {
  val dagMap = linkedMapOf<String, Dag>()
  for (dag in this) {
    require(dagMap.putIfAbsent(dag.id, dag) == null) {
      "Dags in bundle have duplicate ID: ${dag.id}"
    }
  }
  return dagMap
}

/**
 * Entry point for declaring the [Dag]s that this bundle contains.
 *
 * Implement this interface to create a Dag bundle to be served by [Server].
 *
 * ```java
 * public class MyBundleBuilder implements BundleBuilder {
 *     @Override
 *     public Iterable<Dag> getDags() {
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
   * Returns all [Dag]s that belong to this bundle.
   *
   * Called once during [build]; Dag IDs must be unique across the returned
   * collection.
   *
   * @throws IllegalArgumentException if any two Dags share the same ID.
   */
  fun getDags(): Iterable<Dag>

  /**
   * Constructs a [Bundle] from the Dags returned by [getDags].
   *
   * @throws IllegalArgumentException if any two Dags share the same ID.
   */
  fun build(): Bundle = Bundle(getDags())
}
