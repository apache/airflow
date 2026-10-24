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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.byteArrayFromHexString
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicReference

private class EtlConf {
  @JvmField
  var targetTable: String? = null

  @JvmField
  var limit: Int = 100

  @JvmField
  var since: LocalDate? = null

  @JvmField
  var runAt: OffsetDateTime? = null

  @JvmField
  var source: EtlSource? = null

  @JvmField
  var current: AtomicReference<EtlTable>? = null

  @JvmField
  var tables: List<EtlTable>? = null

  @JvmField
  var tableArray: Array<EtlTable>? = null

  @JvmField
  var tablesByName: Map<String, EtlTable>? = null

  @JvmField
  var options: Map<String, Any?>? = null
}

private class EtlSource {
  @JvmField
  var bucketName: String? = null
}

private class EtlTable {
  @JvmField
  var tableName: String? = null
}

private class FoldClash {
  @JvmField
  var targetTable: String? = null

  @JvmField
  var targettable: String? = null
}

private class UrlConf {
  @JvmField
  var baseUrl: String? = null

  fun getBaseURL(): String? = baseUrl
}

private class SetterConf {
  private var stored: String? = null

  fun setTargetTable(value: String?) {
    stored = value
  }

  fun stored(): String? = stored
}

// Jackson binds a Java record through its constructor, but these tests compile
// for Java 11, which has no records. Jackson binds a class with a @JsonCreator
// constructor the same way, so this class stands in for a record.
private class CreatorConf
  @JsonCreator
  constructor(
    @JsonProperty("targetTable") targetTable: String?,
  ) {
    val stored = targetTable
  }

private class GetterOnlyTables {
  private val backingList = mutableListOf<EtlTable>()
  private val backingMap = mutableMapOf<String, EtlTable>()

  fun getTables(): MutableList<EtlTable> = backingList

  fun getTablesByName(): MutableMap<String, EtlTable> = backingMap
}

private fun dagRunWithConf(conf: Map<String, Any?>) = DagRun("d", "r", null, null, null, null, null, conf)

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

  @Test
  fun getConfMatchesKeysIgnoringCaseAndUnderscores() {
    val conf = dagRunWithConf(mapOf("target_table" to "sales", "LIMIT" to 5L)).getConf(EtlConf::class.java)

    Assertions.assertEquals("sales", conf.targetTable)
    Assertions.assertEquals(5, conf.limit)
  }

  @Test
  fun getConfIgnoresKeysWithoutAFieldAndKeepsDefaults() {
    val conf = dagRunWithConf(mapOf("notify_email" to "ops@example.com")).getConf(EtlConf::class.java)

    Assertions.assertNull(conf.targetTable)
    Assertions.assertEquals(100, conf.limit)
  }

  @Test
  fun getConfPrefersAnExactKeyOverKeysThatMatchOnlyIgnoringCase() {
    val conf =
      dagRunWithConf(mapOf("target_table" to "folded", "targetTable" to "exact", "TARGET_TABLE" to "folded"))
        .getConf(EtlConf::class.java)

    Assertions.assertEquals("exact", conf.targetTable)
  }

  @Test
  fun getConfMatchesKeysOfNestedObjects() {
    val conf =
      dagRunWithConf(
        mapOf(
          "source" to mapOf("bucket_name" to "raw"),
          "current" to mapOf("table_name" to "staging"),
          "tables" to listOf(mapOf("table_name" to "orders")),
          "table_array" to listOf(mapOf("table_name" to "items")),
          "tables_by_name" to mapOf("Sales_Key" to mapOf("table_name" to "sales")),
        ),
      ).getConf(EtlConf::class.java)

    Assertions.assertEquals("raw", conf.source?.bucketName)
    Assertions.assertEquals("staging", conf.current?.get()?.tableName)
    Assertions.assertEquals("orders", conf.tables?.single()?.tableName)
    Assertions.assertEquals("items", conf.tableArray?.single()?.tableName)
    Assertions.assertEquals(setOf("Sales_Key"), conf.tablesByName?.keys)
    Assertions.assertEquals("sales", conf.tablesByName?.get("Sales_Key")?.tableName)
  }

  @Test
  fun getConfKeepsTheKeysOfAMapWithUntypedValues() {
    val options = mapOf("Rows" to listOf(mapOf("Key_A" to "a")), "Inner_Map" to mapOf("Key_B" to "b"))

    val conf = dagRunWithConf(mapOf("options" to options)).getConf(EtlConf::class.java)

    Assertions.assertEquals(options, conf.options)
  }

  @Test
  fun getConfReadsIsoDateStringsIntoJavaTimeFields() {
    val conf =
      dagRunWithConf(mapOf("since" to "2024-01-31", "run_at" to "2024-01-31T02:00:00+08:00"))
        .getConf(EtlConf::class.java)

    Assertions.assertEquals(LocalDate.of(2024, 1, 31), conf.since)
    Assertions.assertEquals(OffsetDateTime.parse("2024-01-31T02:00:00+08:00"), conf.runAt)
  }

  @Test
  fun getConfMatchesKeysToSettersAndConstructorParameters() {
    val conf = dagRunWithConf(mapOf("target_table" to "sales"))

    Assertions.assertEquals("sales", conf.getConf(SetterConf::class.java).stored())
    Assertions.assertEquals("sales", conf.getConf(CreatorConf::class.java).stored)
  }

  @Test
  fun getConfSkipsPropertiesThatOnlyHaveAGetter() {
    val conf = dagRunWithConf(mapOf("base_url" to "https://example.com")).getConf(UrlConf::class.java)

    Assertions.assertEquals("https://example.com", conf.baseUrl)
  }

  @Test
  fun getConfFillsCollectionsAndMapsThatOnlyHaveAGetter() {
    val conf =
      dagRunWithConf(
        mapOf(
          "TABLES" to listOf(mapOf("table_name" to "orders")),
          "tables_by_name" to mapOf("Sales_Key" to mapOf("table_name" to "sales")),
        ),
      ).getConf(GetterOnlyTables::class.java)

    Assertions.assertEquals("orders", conf.getTables().single().tableName)
    Assertions.assertEquals("sales", conf.getTablesByName()["Sales_Key"]?.tableName)
  }

  @Test
  fun getConfRejectsTwoKeysThatMatchOneField() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dagRunWithConf(mapOf("target_table" to "a", "TargetTable" to "b")).getConf(EtlConf::class.java)
      }

    Assertions.assertEquals(
      "Dag run conf keys 'target_table' and 'TargetTable' both match field EtlConf.targetTable when case and " +
        "underscores are ignored, so the SDK cannot tell which one to use. Name the field exactly as the key " +
        "it should take.",
      error.message,
    )
  }

  @Test
  fun getConfRejectsNullOnlyForPrimitiveFields() {
    Assertions.assertNull(dagRunWithConf(mapOf("target_table" to null)).getConf(EtlConf::class.java).targetTable)

    listOf("limit", "LIMIT").forEach { key ->
      val error =
        Assertions.assertThrows(IllegalArgumentException::class.java) {
          dagRunWithConf(mapOf(key to null)).getConf(EtlConf::class.java)
        }

      Assertions.assertEquals(
        "Dag run conf key '$key' is null, but field EtlConf.limit has a primitive type that cannot be null. " +
          "Declare a boxed type (e.g. Integer instead of int) to receive null.",
        error.message,
      )
    }
  }

  @Test
  fun getConfRejectsFieldsThatDifferOnlyInCaseOrUnderscores() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dagRunWithConf(emptyMap()).getConf(FoldClash::class.java)
      }

    Assertions.assertEquals(
      "Fields FoldClash.targetTable and FoldClash.targettable differ only in case or underscores, so the SDK " +
        "cannot tell which one a Dag run conf key should go to. Rename one of them.",
      error.message,
    )
  }

  @Test
  fun getConfNamesTheTargetClassWhenAValueDoesNotConvert() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dagRunWithConf(mapOf("limit" to "many")).getConf(EtlConf::class.java)
      }

    val message = error.message.orEmpty()
    Assertions.assertTrue(message.startsWith("Cannot convert the Dag run conf to org.apache.airflow.sdk.EtlConf: ")) {
      message
    }
    Assertions.assertInstanceOf(IllegalArgumentException::class.java, error.cause)
  }
}
