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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class FrameTest {
  /** Stands in for the Scala collections jackson-module-scala builds for untyped values. */
  private class ForeignCollection(
    val json: String,
  )

  @Test
  @DisplayName("encodeUserValue returns java.util collections when a module replaces untyped deserialization")
  fun encodeUserValueIgnoresModuleDeserializers() {
    val valueMapper =
      ObjectMapper().registerModule(
        SimpleModule().addDeserializer(
          Any::class.java,
          object : StdDeserializer<Any>(Any::class.java) {
            override fun deserialize(
              p: JsonParser,
              ctxt: DeserializationContext,
            ): Any = ForeignCollection(p.readValueAsTree<JsonNode>().toString())
          },
        ),
      )

    val encoded = Frame.encodeUserValue(mapOf("xs" to listOf("a", "b")), valueMapper)

    assertEquals(mapOf("xs" to listOf("a", "b")), encoded)
  }
}
