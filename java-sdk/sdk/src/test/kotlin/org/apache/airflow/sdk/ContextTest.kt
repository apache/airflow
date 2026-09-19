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

import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.byteArrayFromHexString
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class ContextTest {
  @Test
  fun fromDecodesRunContextFields() {
    // [2, msg, null] with msg coming from
    // https://github.com/apache/airflow/blob/ac8d947431931921d5186ea99a71e3956458fea9/task-sdk/tests/task_sdk/execution_time/test_comms.py#L72-L110
    val data =
      """
      92 02 88 a4 74 79 70 65 ae 53 74 61 72 74 75 70 44 65 74 61 69 6c 73 a2 74 69 86 a2 69 64 d9 24
      34 64 38 32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63
      65 63 61 62 a7 74 61 73 6b 5f 69 64 a1 61 aa 74 72 79 5f 6e 75 6d 62 65 72 01 a6 72 75 6e 5f 69
      64 a1 62 a6 64 61 67 5f 69 64 a1 63 ae 64 61 67 5f 76 65 72 73 69 6f 6e 5f 69 64 d9 24 34 64 38
      32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63 65 63 61
      62 aa 74 69 5f 63 6f 6e 74 65 78 74 85 a7 64 61 67 5f 72 75 6e 8c a6 64 61 67 5f 69 64 a1 63 a6
      72 75 6e 5f 69 64 a1 62 ac 6c 6f 67 69 63 61 6c 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31
      54 30 31 3a 30 30 3a 30 30 5a b3 64 61 74 61 5f 69 6e 74 65 72 76 61 6c 5f 73 74 61 72 74 b4 32
      30 32 34 2d 31 32 2d 30 31 54 30 30 3a 30 30 3a 30 30 5a b1 64 61 74 61 5f 69 6e 74 65 72 76 61
      6c 5f 65 6e 64 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a aa 73 74 61 72 74
      5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a9 72 75 6e 5f 61
      66 74 65 72 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a8 65 6e 64 5f 64 61
      74 65 c0 a8 72 75 6e 5f 74 79 70 65 a6 6d 61 6e 75 61 6c a5 73 74 61 74 65 a7 73 75 63 63 65 73
      73 a4 63 6f 6e 66 c0 b5 63 6f 6e 73 75 6d 65 64 5f 61 73 73 65 74 5f 65 76 65 6e 74 73 90 a9 6d
      61 78 5f 74 72 69 65 73 00 ac 73 68 6f 75 6c 64 5f 72 65 74 72 79 c2 a9 76 61 72 69 61 62 6c 65
      73 c0 ab 63 6f 6e 6e 65 63 74 69 6f 6e 73 c0 a4 66 69 6c 65 a9 2f 64 65 76 2f 6e 75 6c 6c aa 73
      74 61 72 74 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a ac 64
      61 67 5f 72 65 6c 5f 70 61 74 68 a9 2f 64 65 76 2f 6e 75 6c 6c ab 62 75 6e 64 6c 65 5f 69 6e 66
      6f 82 a4 6e 61 6d 65 a8 61 6e 79 2d 6e 61 6d 65 a7 76 65 72 73 69 6f 6e ab 61 6e 79 2d 76 65 72
      73 69 6f 6e b2 73 65 6e 74 72 79 5f 69 6e 74 65 67 72 61 74 69 6f 6e a0 c0
      """.trimIndent()

    val context = Context.from(CoordinatorComm.decode(byteArrayFromHexString(data)).body as StartupDetails)
    val dr = context.dagRun

    Assertions.assertEquals(OffsetDateTime.parse("2024-12-01T01:00:00Z"), dr.logicalDate)
    Assertions.assertEquals(OffsetDateTime.parse("2024-12-01T00:00:00Z"), dr.dataIntervalStart)
    Assertions.assertEquals(OffsetDateTime.parse("2024-12-01T01:00:00Z"), dr.dataIntervalEnd)
    Assertions.assertEquals(OffsetDateTime.parse("2024-12-01T01:00:00Z"), dr.runAfter)
    Assertions.assertEquals(DagRunType.MANUAL, dr.runType)
    Assertions.assertTrue(dr.conf.isEmpty())
  }

  @Test
  fun fromMapsConfAndToleratesMissingFields() {
    val ti =
      org.apache.airflow.sdk.execution.comm.TaskInstance().apply {
        dagId = "d"
        runId = "r"
        taskId = "t"
        tryNumber = 1
      }
    val commDagRun =
      org.apache.airflow.sdk.execution.comm.DagRun().apply {
        dagId = "d"
        runId = "r"
        conf = mapOf("target_table" to "sales", "dry_run" to true)
      }
    val request =
      StartupDetails().apply {
        this.ti = ti
        tiContext = TIRunContext().apply { dagRun = commDagRun }
      }

    val dr = Context.from(request).dagRun

    Assertions.assertEquals("sales", dr.conf["target_table"])
    Assertions.assertEquals(true, dr.conf["dry_run"])
    Assertions.assertNull(dr.logicalDate)
    Assertions.assertNull(dr.runType)
  }

  @Test
  fun fromToleratesUnparseableAndUnexpectedValues() {
    val ti =
      org.apache.airflow.sdk.execution.comm.TaskInstance().apply {
        dagId = "d"
        runId = "r"
        taskId = "t"
        tryNumber = 1
      }
    val commDagRun =
      org.apache.airflow.sdk.execution.comm.DagRun().apply {
        dagId = "d"
        runId = "r"
        logicalDate = "not-a-date"
        dataIntervalStart = 12345
        conf = "not-a-map"
      }
    val request =
      StartupDetails().apply {
        this.ti = ti
        tiContext = TIRunContext().apply { dagRun = commDagRun }
      }

    val dr = Context.from(request).dagRun

    Assertions.assertNull(dr.logicalDate)
    Assertions.assertNull(dr.dataIntervalStart)
    Assertions.assertTrue(dr.conf.isEmpty())
  }
}
