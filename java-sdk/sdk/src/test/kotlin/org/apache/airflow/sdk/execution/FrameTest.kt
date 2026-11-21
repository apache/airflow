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

import org.apache.airflow.sdk.ArgName
import org.apache.airflow.sdk.execution.comm.SetXCom
import org.apache.airflow.sdk.internal.ArgValues
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.msgpack.core.buffer.ArrayBufferInput

private class PushedStats {
  @JvmField
  @ArgName("Region_Code")
  var region: String? = null
}

internal class FrameTest {
  @Test
  @DisplayName("Should write the pinned name of a pushed object, so that a task can read the object back")
  fun shouldWritePinnedNameOfPushedObject() {
    val pushed = PushedStats().also { it.region = "emea" }

    val buffers = Frame.encodeRequest(1, SetXCom().also { it.value = pushed })
    val body = Frame.decodeRaw(ArrayBufferInput(buffers.single().toByteArray())).rawBody as Map<*, *>

    assertEquals(mapOf("Region_Code" to "emea"), body["value"])
    assertEquals("emea", (ArgValues.decode(body["value"], PushedStats::class.java) as PushedStats).region)
  }
}
