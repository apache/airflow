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
 * <p>Build a [Bundle] by implementing [BundleBuilder], then pass it to [Server.serve]
 * to start accepting task-execution requests.
 *
 * @property version Implementation version read from the JAR manifest
 *   ({@code Implementation-Version}); falls back to {@code "0"} if absent.
 * @property dags All registered Dags keyed by [Dag.id]. Insertion order is preserved.
 */
class Bundle(
  val version: String,
  dags: Iterable<Dag>,
) {
  val dags: Map<String, Dag> = dags.associateByDagId()
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
 * <p>Implement this interface in the class named as {@code Main-Class} in your JAR
 * manifest. The build tooling instantiates it at compile time to record Dag and task
 * IDs in the manifest, enabling inspection without running the full process.
 *
 * <pre>{@code
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
 * }</pre>
 */
interface BundleBuilder {
  /**
   * Returns all [Dag]s that belong to this bundle.
   *
   * <p>Called once during [build]; Dag IDs must be unique across the returned collection.
   */
  fun getDags(): Iterable<Dag>

  /**
   * Constructs a [Bundle] from the Dags returned by [getDags].
   *
   * <p>The bundle version is taken from the JAR's {@code Implementation-Version} manifest
   * attribute, or {@code "0"} if that attribute is absent.
   *
   * @throws IllegalArgumentException if any two Dags share the same ID.
   */
  fun build(): Bundle = Bundle(this::class.java.`package`.implementationVersion ?: "0", getDags())
}
