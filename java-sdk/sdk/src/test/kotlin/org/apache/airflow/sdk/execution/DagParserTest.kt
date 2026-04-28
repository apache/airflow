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

package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Dag
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class DagParserTest {
  lateinit var parser: DagParser

  @BeforeEach
  fun setUp() {
    parser = DagParser(":memory:", "")
  }

  @Test
  @DisplayName("Should produce serialized dag")
  fun shouldProduceSerializedDag() {
    val bundle = Bundle("0", listOf(Dag("dag")))
    val result = parser.parse(bundle)
    Assertions.assertEquals(
      DagParsingResult(":memory:", "", bundle.dags),
      result,
    )
  }
}
