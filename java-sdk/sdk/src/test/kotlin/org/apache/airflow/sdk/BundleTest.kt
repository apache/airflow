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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class BundleTest {
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  /** A class of handlers that the processor generated [BundleTest_NestedHandlers] for. */
  class Nested

  @Test
  @DisplayName("Should index dags by dagId")
  fun shouldIndexDagsByDagId() {
    val dag = DagDef("dag")

    val bundle = Bundle(listOf(dag))

    Assertions.assertEquals(mapOf("dag" to dag), bundle.dags)
  }

  @Test
  @DisplayName("Should reject duplicate dag ids")
  fun shouldRejectDuplicateDagIds() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle(listOf(DagDef("dag"), DagDef("dag")))
      }

    Assertions.assertEquals("Dags in bundle have duplicate ID: dag", error.message)
  }

  @Test
  @DisplayName("Should find the registrar generated for a nested handler class")
  fun shouldFindRegistrarOfNestedHandlerClass() {
    val bundle = Bundle().register(Nested::class.java)

    val etl = bundle.dags.getValue("etl")
    Assertions.assertEquals(listOf("etl"), bundle.dags.keys.toList())
    Assertions.assertEquals(listOf("score"), etl.tasks.keys.toList())
  }

  @Test
  @DisplayName("Should name the registrar it looked for when there is none")
  fun shouldNameTheRegistrarItLookedFor() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle().register(NoOp::class.java)
      }

    Assertions.assertTrue(
      error.message!!.startsWith(
        "No generated registrar org.apache.airflow.sdk.BundleTest_NoOpHandlers for ",
      ),
      error.message,
    )
  }
}

/**
 * Stands in for the registrar the annotation processor generates beside
 * [BundleTest.Nested], to pin the name [Bundle.register] looks up.
 */
@Suppress("ktlint:standard:class-naming", "ClassName")
class BundleTest_NestedHandlers {
  companion object {
    @JvmStatic
    fun registerInto(bundle: Bundle) {
      bundle.register("etl", "score", NoOpHandler::class.java)
    }
  }

  class NoOpHandler : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }
}
