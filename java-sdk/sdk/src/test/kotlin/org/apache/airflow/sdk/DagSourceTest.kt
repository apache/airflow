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

import com.fasterxml.jackson.databind.ObjectMapper
import com.xenomachina.argparser.SystemExitException
import org.apache.airflow.example.DagSourceFixtures
import org.apache.airflow.sdk.internal.DagSource
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

internal class DagSourceTest {
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  @Test
  @DisplayName("Should record the class that constructed the Dag")
  fun recordsConstructingClass() {
    Assertions.assertEquals(DagSourceFixtures::class.java, DagSourceFixtures().plain().declaringClass)
  }

  @Test
  @DisplayName("Should record the outermost class for nested, lambda and anonymous callers")
  fun recordsOutermostClass() {
    val fixtures = DagSourceFixtures()
    Assertions.assertEquals(DagSourceFixtures::class.java, DagSourceFixtures.Nested().make().declaringClass)
    Assertions.assertEquals(
      DagSourceFixtures::class.java,
      DagSourceFixtures.Nested
        .Deeper()
        .make()
        .declaringClass,
    )
    Assertions.assertEquals(DagSourceFixtures::class.java, fixtures.fromLambda().declaringClass)
    Assertions.assertEquals(DagSourceFixtures::class.java, fixtures.fromAnonymous().declaringClass)
  }

  @Test
  @DisplayName("Should let a generated builder name the annotated class instead of itself")
  fun declaredByOverridesCapturedClass() {
    val dag = DagSource.declaredBy(DagDef("dag"), DagSourceFixtures.Nested::class.java)
    Assertions.assertEquals(DagSourceFixtures::class.java, dag.declaringClass)
  }

  private fun describe(
    dir: File,
    bundle: Bundle,
  ): Map<*, *> {
    val target = File(dir, "out/sources.json")
    Server.create(arrayOf("--describe-sources", target.path)).serve(bundle)
    return ObjectMapper().readValue(target, Map::class.java)
  }

  @Test
  @DisplayName("Should write each Java-declared Dag's declaring class and skip task-handler Dags")
  fun describeSourcesWritesDeclaringClasses(
    @TempDir dir: File,
  ) {
    val fixtures = DagSourceFixtures()
    val bundle =
      Bundle(listOf(fixtures.plain(), DagSourceFixtures.Nested().make()))
        .register("python_owned", "t", NoOp::class.java)

    Assertions.assertEquals(
      mapOf("plain" to DagSourceFixtures::class.java.name, "nested" to DagSourceFixtures::class.java.name),
      describe(dir, bundle),
    )
  }

  @Test
  @DisplayName("Should omit a Dag whose declaring class is unknown")
  fun describeSourcesOmitsUnknownDeclaringClass(
    @TempDir dir: File,
  ) {
    val unknown = DagDef("unknown").also { it.declaringClass = null }
    val bundle = Bundle(listOf(DagSourceFixtures().plain(), unknown))

    Assertions.assertEquals(mapOf("plain" to DagSourceFixtures::class.java.name), describe(dir, bundle))
  }

  @Test
  @DisplayName("Should write an empty object when no Dag is declared in Java")
  fun describeSourcesWithoutJavaDags(
    @TempDir dir: File,
  ) {
    Assertions.assertEquals(emptyMap<Any, Any>(), describe(dir, Bundle()))
  }

  @Test
  @DisplayName("Should use the binary name of the declaring class")
  fun describeSourcesUsesBinaryName(
    @TempDir dir: File,
  ) {
    val dag = DagDef("dag").also { it.declaringClass = DagSourceFixtures.Nested::class.java }
    Assertions.assertEquals(
      mapOf("dag" to "org.apache.airflow.example.DagSourceFixtures\$Nested"),
      describe(dir, Bundle(listOf(dag))),
    )
  }

  @Test
  @DisplayName("Should still require --comm and --logs without --describe-sources")
  fun createStillRequiresAddresses() {
    Assertions.assertThrows(SystemExitException::class.java) { Server.create(arrayOf("--comm", "localhost:1")) }
    Assertions.assertThrows(SystemExitException::class.java) { Server.create(arrayOf("--logs", "localhost:1")) }
    Assertions.assertThrows(SystemExitException::class.java) { Server.create(emptyArray()) }
  }
}
