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
import org.apache.airflow.example.DagSourceFixtures
import org.apache.airflow.sdk.internal.DagSource
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

internal object ServerDependencyProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    Server.create(args)
  }
}

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
    val missingComm = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--logs", "localhost:1"))
    }
    Assertions.assertEquals("Missing required argument: --comm", missingComm.message)

    val missingLogs = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--comm", "localhost:1"))
    }
    Assertions.assertEquals("Missing required argument: --logs", missingLogs.message)

    val missingBoth = Assertions.assertThrows(IllegalArgumentException::class.java) { Server.create(emptyArray()) }
    Assertions.assertEquals("Missing required argument: --comm", missingBoth.message)
  }

  @Test
  @DisplayName("Should reject missing values, malformed addresses and unknown arguments")
  fun createRejectsInvalidArguments() {
    val missingValue = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--comm"))
    }
    Assertions.assertEquals("Missing value for --comm", missingValue.message)

    val malformedAddress = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--comm=localhost:port", "--logs=localhost:1"))
    }
    Assertions.assertEquals("--comm port must be numeric", malformedAddress.message)

    val malformedAddressShape = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--comm=localhost", "--logs=localhost:1"))
    }
    Assertions.assertEquals("--comm must use host:port", malformedAddressShape.message)

    val invalidPort = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--comm=localhost:65536", "--logs=localhost:1"))
    }
    Assertions.assertEquals("--comm port must be between 0 and 65535", invalidPort.message)

    val missingSourcesPath = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--describe-sources="))
    }
    Assertions.assertEquals("Missing value for --describe-sources", missingSourcesPath.message)

    val unknownArgument = Assertions.assertThrows(IllegalArgumentException::class.java) {
      Server.create(arrayOf("--unknown"))
    }
    Assertions.assertEquals("Unknown argument: --unknown", unknownArgument.message)
  }

  @Test
  @DisplayName("Should create a Server when kotlin-argparser is absent from the runtime classpath")
  fun createsWithoutKotlinArgparser() {
    val classpath =
      System.getProperty("java.class.path")
        .split(File.pathSeparator)
        .filterNot { it.contains("kotlin-argparser") || it.contains("xenocom") }
        .joinToString(File.pathSeparator)
    val process =
      ProcessBuilder(
        File(System.getProperty("java.home"), "bin/java").path,
        "-cp",
        classpath,
        ServerDependencyProbe::class.java.name,
        "--comm=127.0.0.1:1",
        "--logs=127.0.0.1:2",
      ).redirectErrorStream(true)
        .start()
    val output = process.inputStream.bufferedReader().use { it.readText() }

    Assertions.assertEquals(0, process.waitFor(), output)
  }
}
