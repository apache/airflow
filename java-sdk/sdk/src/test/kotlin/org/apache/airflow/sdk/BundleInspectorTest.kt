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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

private class NoOp : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

internal class SuspiciousBundleBuilder : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("bad dag").addTask("bad task", NoOp::class.java))
}

internal class ValidBundleBuilder : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("good_dag").addTask("good_task", NoOp::class.java))
}

internal class DuplicateDagBundleBuilder : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("dag"), DagDef("dag"))
}

internal class ThrowingBundleBuilder : BundleBuilder {
  init {
    error("boom")
  }

  override fun getDags(): Iterable<DagDef> = emptyList()
}

internal object SingletonBundleBuilder : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("with space"))
}

internal class PrivateInstanceFieldBundleBuilder : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("with space"))

  companion object {
    @Suppress("unused")
    private val INSTANCE = Any()
  }
}

internal class ScalaLikeBundleBuilder

@Suppress("ktlint:standard:class-naming", "ClassName")
internal class `ScalaLikeBundleBuilder$` : BundleBuilder {
  override fun getDags(): Iterable<DagDef> = listOf(DagDef("with space"))

  companion object {
    @JvmField
    val `MODULE$`: `ScalaLikeBundleBuilder$` = `ScalaLikeBundleBuilder$`()
  }
}

private fun charsetWarningLine(arguments: String) =
  "warning: Dag id must be made of alphanumeric characters, dashes, dots, and underscores; " +
    "the Airflow server will reject it ($arguments)\n"

internal class BundleInspectorTest {
  private fun inspect(className: String): String {
    val output = StringBuilder()
    BundleInspector.inspect(className, output)
    return output.toString()
  }

  @Test
  @DisplayName("prints a warning for every suspicious dag and task id")
  fun shouldWarnOnSuspiciousIds() {
    assertEquals(
      charsetWarningLine("dag_id=\"bad dag\"") +
        "warning: Task id must be made of alphanumeric characters, dashes, dots, and underscores; " +
        "the Airflow server will reject it (dag_id=\"bad dag\", task_id=\"bad task\")\n",
      inspect(SuspiciousBundleBuilder::class.java.name),
    )
  }

  @Test
  @DisplayName("prints nothing for a valid bundle")
  fun shouldStaySilentOnValidBundle() {
    assertEquals("", inspect(ValidBundleBuilder::class.java.name))
  }

  @Test
  @DisplayName("resolves a Kotlin object builder through its INSTANCE field")
  fun shouldResolveKotlinObject() {
    assertEquals(
      charsetWarningLine("dag_id=\"with space\""),
      inspect(SingletonBundleBuilder::class.java.name),
    )
  }

  @Test
  @DisplayName("resolves a Scala-style object builder through MODULE$ on the dollar class")
  fun shouldResolveScalaObject() {
    assertEquals(
      charsetWarningLine("dag_id=\"with space\""),
      inspect(ScalaLikeBundleBuilder::class.java.name),
    )
  }

  @Test
  @DisplayName("a private static INSTANCE field is ignored, not tripped over")
  fun shouldIgnorePrivateInstanceField() {
    assertEquals(
      charsetWarningLine("dag_id=\"with space\""),
      inspect(PrivateInstanceFieldBundleBuilder::class.java.name),
    )
  }

  @Test
  @DisplayName("notes and skips a main class that is not a BundleBuilder")
  fun shouldSkipNonBuilder() {
    assertEquals(
      "note: java.lang.Object is not an instantiable BundleBuilder; skipping the Dag and task ID check\n",
      inspect("java.lang.Object"),
    )
  }

  @Test
  @DisplayName("duplicate dag ids fail the inspection")
  fun shouldFailOnDuplicateDagIds() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        inspect(DuplicateDagBundleBuilder::class.java.name)
      }

    assertEquals("Dags in bundle have duplicate ID: dag", error.message)
  }

  @Test
  @DisplayName("a builder whose constructor throws propagates the real error")
  fun shouldPropagateConstructorFailure() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        inspect(ThrowingBundleBuilder::class.java.name)
      }

    assertEquals("boom", error.message)
  }
}
