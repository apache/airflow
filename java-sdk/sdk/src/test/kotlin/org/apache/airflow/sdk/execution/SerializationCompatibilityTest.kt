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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.airflow.sdk.Dag
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import java.io.File
import java.time.Duration
import java.time.Instant

/**
 * Reads test_dags.yaml, constructs Dags from the parameters, serialises each
 * one with the Java SDK, and writes the result to serialized_java.json for
 * cross-language comparison with the Python output.
 *
 * Each YAML test-case is turned into a JUnit 5 dynamic test so failures are
 * reported individually.
 *
 * After running:
 *   python validation/serialization/compare.py \
 *       validation/serialization/serialized_python.json \
 *       validation/serialization/serialized_java.json
 */
class SerializationCompatibilityTest {
  companion object {
    private val yamlMapper = ObjectMapper(YAMLFactory())
    private val jsonMapper =
      ObjectMapper().apply {
        enable(SerializationFeature.INDENT_OUTPUT)
        configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      }

    /** Resolve a project-relative path that works from any Gradle working dir. */
    private fun projectFile(relative: String): File {
      // Gradle may run from the repo root or from sdk/
      var dir = File(System.getProperty("user.dir"))
      while (dir.parentFile != null) {
        val candidate = File(dir, relative)
        if (candidate.exists()) return candidate
        dir = dir.parentFile
      }
      // Fallback: try relative to cwd
      return File(relative)
    }
  }

  // -----------------------------------------------------------------------
  // YAML → Dag construction
  // -----------------------------------------------------------------------

  @Suppress("UNCHECKED_CAST")
  private fun constructDag(params: Map<String, Any?>): Dag {
    val dagId = params["dag_id"] as String

    return Dag(
      dagId = dagId,
      description = params["description"] as? String,
      schedule = params["schedule"] as? String,
      startDate = (params["start_date"] as? String)?.let { Instant.parse(it) },
      endDate = (params["end_date"] as? String)?.let { Instant.parse(it) },
      defaultArgs = (params["default_args"] as? Map<String, Any>) ?: emptyMap(),
      maxActiveTasks = (params["max_active_tasks"] as? Number)?.toInt() ?: Dag.DEFAULT_MAX_ACTIVE_TASKS,
      maxActiveRuns = (params["max_active_runs"] as? Number)?.toInt() ?: Dag.DEFAULT_MAX_ACTIVE_RUNS,
      maxConsecutiveFailedDagRuns =
        (params["max_consecutive_failed_dag_runs"] as? Number)?.toInt()
          ?: Dag.DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS,
      dagrunTimeout = (params["dagrun_timeout_seconds"] as? Number)?.let { Duration.ofSeconds(it.toLong()) },
      catchup = params["catchup"] as? Boolean ?: false,
      docMd = params["doc_md"] as? String,
      accessControl =
        (params["access_control"] as? Map<String, Map<String, Any>>)?.mapValues { (_, resources) ->
          resources.mapValues { (_, perms) ->
            when (perms) {
              is List<*> -> perms.filterIsInstance<String>().toSet()
              is Set<*> -> perms.filterIsInstance<String>().toSet()
              else -> setOf(perms.toString())
            }
          }
        },
      isPausedUponCreation = params["is_paused_upon_creation"] as? Boolean,
      tags = (params["tags"] as? List<*>)?.filterIsInstance<String>()?.toSet() ?: emptySet(),
      ownerLinks = (params["owner_links"] as? Map<String, String>) ?: emptyMap(),
      failFast = params["fail_fast"] as? Boolean ?: false,
      dagDisplayName = params["dag_display_name"] as? String,
      renderTemplateAsNativeObj = params["render_template_as_native_obj"] as? Boolean ?: false,
      params = params["params"] as? Map<String, Any>,
    )
  }

  // -----------------------------------------------------------------------
  // Dynamic test generation
  // -----------------------------------------------------------------------

  @Suppress("UNCHECKED_CAST")
  @TestFactory
  fun `serialise all YAML test cases`(): List<DynamicTest> {
    val yamlFile = projectFile("validation/serialization/test_dags.yaml")
    if (!yamlFile.exists()) {
      return listOf(
        DynamicTest.dynamicTest("test_dags.yaml not found — skipping") {
          println("WARNING: ${yamlFile.absolutePath} not found, skipping serialisation tests")
        },
      )
    }

    val root = yamlMapper.readValue(yamlFile, Map::class.java) as Map<String, Any>
    val testCases = root["test_cases"] as List<Map<String, Any>>

    // Accumulate results for JSON output
    val allResults = mutableMapOf<String, Any>()

    val tests =
      testCases.map { case ->
        val name = case["name"] as String
        val params = case["params"] as Map<String, Any?>

        DynamicTest.dynamicTest(name) {
          val dag = constructDag(params)
          val serialized = serializeDag(dag)

          // Basic assertions
          assertNotNull(serialized["dag_id"], "dag_id must be present")
          assertNotNull(serialized["timetable"], "timetable must be present")
          assertNotNull(serialized["tasks"], "tasks must be present")
          assertFalse(
            serialized.containsKey("__error"),
            "serialisation must not produce an error entry",
          )

          allResults[name] = serialized
        }
      }

    // After all dynamic tests, write the combined JSON.
    // We add a final "meta" test that writes the file.
    val writeTest =
      DynamicTest.dynamicTest("_write_serialized_java_json") {
        val outputDir = projectFile("validation/serialization")
        outputDir.mkdirs()
        val outputFile = File(outputDir, "serialized_java.json")
        jsonMapper.writeValue(outputFile, allResults.toSortedMap())
        println("Wrote ${allResults.size} serialised DAGs -> ${outputFile.absolutePath}")
      }

    return tests + writeTest
  }
}
