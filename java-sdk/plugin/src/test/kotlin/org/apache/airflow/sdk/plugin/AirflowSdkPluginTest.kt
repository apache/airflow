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

package org.apache.airflow.sdk.plugin

import groovy.json.JsonSlurper
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.util.jar.JarFile

class AirflowSdkPluginTest {
  private fun main(describeBody: String) =
    """
    package com.example;

    public class Main {
      public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--describe-sources")) {
          $describeBody
        }
      }
    }
    """.trimIndent()

  private val describeDags =
    """
    java.nio.file.Files.write(java.nio.file.Paths.get(args[1]), (
        "{\"orders\":\"com.example.Main\"," +
        "\"reports\":\"com.example.dags.Reports\"," +
        "\"reports_backfill\":\"com.example.dags.Reports\"," +
        "\"ghost\":\"com.example.Ghost\"}").getBytes());
    """.trimIndent()

  private fun project(
    dir: File,
    mainBody: String = describeDags,
    mainClass: String? = "com.example.Main",
  ): File {
    dir.write("settings.gradle", "rootProject.name = 'bundle-test'\n")
    dir.write(
      "build.gradle",
      """
      plugins { id 'org.apache.airflow.sdk' }
      airflowBundle {
        ${mainClass?.let { "mainClass = '$it'" } ?: ""}
        fatJar = false
      }
      sourceSets { main { java.srcDir 'src/extra' } }
      """.trimIndent(),
    )
    dir.write("src/main/java/com/example/Main.java", main(mainBody))
    dir.write("src/extra/com/example/dags/Reports.java", "package com.example.dags; public class Reports {}")
    return dir
  }

  private fun gradle(
    dir: File,
    vararg tasks: String,
  ): BuildResult =
    GradleRunner
      .create()
      .withPluginClasspath()
      .withProjectDir(dir)
      .withArguments(*tasks, "--stacktrace")
      .build()

  private fun entries(jar: File): List<String> =
    JarFile(jar).use {
      it
        .entries()
        .asSequence()
        .map { e -> e.name }
        .toList()
    }

  @Suppress("UNCHECKED_CAST")
  private fun sourcesJson(jar: File): Map<String, Any> =
    JarFile(jar).use {
      JsonSlurper().parse(it.getInputStream(it.getJarEntry("META-INF/airflow/sources.json"))) as Map<String, Any>
    }

  @Test
  fun packsEachDagSourceOnceAndTheEntrypoint(
    @TempDir dir: File,
  ) {
    project(dir)
    val result = gradle(dir, "jar")
    assertEquals(TaskOutcome.SUCCESS, result.task(":packDagSources")!!.outcome)

    val jar = File(dir, "build/libs/bundle-test.jar")
    assertEquals(
      listOf(
        "META-INF/airflow/sources.json",
        "META-INF/airflow/sources/com/example/Main.java",
        "META-INF/airflow/sources/com/example/dags/Reports.java",
      ),
      entries(jar).filter { it.startsWith("META-INF/airflow/") && !it.endsWith("/") }.sorted(),
    )
    assertEquals(
      mapOf(
        "entrypoint_path" to "com/example/Main.java",
        "dag_source_paths" to
          mapOf(
            "orders" to "com/example/Main.java",
            "reports" to "com/example/dags/Reports.java",
            "reports_backfill" to "com/example/dags/Reports.java",
          ),
      ),
      sourcesJson(jar),
    )
    JarFile(jar).use {
      assertEquals("META-INF/airflow/sources.json", it.manifest.mainAttributes.getValue("Airflow-Java-SDK-Sources"))
      assertEquals("com.example.Main", it.manifest.mainAttributes.getValue("Main-Class"))
    }
  }

  @Test
  fun isUpToDateOnSecondRunAndRerunsWhenASourceChanges(
    @TempDir dir: File,
  ) {
    project(dir)
    gradle(dir, "jar")
    assertEquals(TaskOutcome.UP_TO_DATE, gradle(dir, "jar").task(":packDagSources")!!.outcome)

    dir.write("src/extra/com/example/dags/Reports.java", "package com.example.dags; public class Reports { int v; }")
    assertEquals(TaskOutcome.SUCCESS, gradle(dir, "jar").task(":packDagSources")!!.outcome)
    val packed = File(dir, "build/airflow/sources/META-INF/airflow/sources/com/example/dags/Reports.java")
    assertTrue(packed.readText().contains("int v"))
  }

  @Test
  fun worksWithConfigurationCache(
    @TempDir dir: File,
  ) {
    project(dir)
    gradle(dir, "jar", "--configuration-cache")
    val again = gradle(dir, "jar", "--configuration-cache")
    assertTrue(again.output.contains("Reusing configuration cache"))
  }

  @Test
  fun fallsBackToEntrypointOnlyWhenTheDescribeRunFails(
    @TempDir dir: File,
  ) {
    project(dir, mainBody = """throw new IllegalStateException("boom");""")
    val result = gradle(dir, "jar")

    assertEquals(TaskOutcome.SUCCESS, result.task(":jar")!!.outcome)
    assertTrue(result.output.contains("only its entrypoint source is packed"), result.output)
    assertTrue(result.output.contains("boom"), result.output)
    assertEquals(
      mapOf("entrypoint_path" to "com/example/Main.java", "dag_source_paths" to emptyMap<String, String>()),
      sourcesJson(File(dir, "build/libs/bundle-test.jar")),
    )
  }

  @Test
  fun fallsBackToEntrypointOnlyWhenMainWritesNothing(
    @TempDir dir: File,
  ) {
    project(dir, mainBody = "")
    val result = gradle(dir, "jar")

    assertTrue(result.output.contains("does main call Server.serve?"), result.output)
    assertEquals(
      mapOf("entrypoint_path" to "com/example/Main.java", "dag_source_paths" to emptyMap<String, String>()),
      sourcesJson(File(dir, "build/libs/bundle-test.jar")),
    )
  }

  @Test
  fun leavesTheSourcesPayloadOutOfANonBundleJar(
    @TempDir dir: File,
  ) {
    project(dir)
    dir.write(
      "build.gradle",
      File(dir, "build.gradle").readText() + "\njava { withSourcesJar() }\n",
    )

    gradle(dir, "jar", "sourcesJar")

    val sources = File(dir, "build/libs/bundle-test-sources.jar")
    assertFalse(entries(sources).any { it.startsWith("META-INF/airflow") })
    JarFile(sources).use { assertNull(it.manifest.mainAttributes.getValue("Airflow-Java-SDK-Sources")) }
    JarFile(File(dir, "build/libs/bundle-test.jar")).use {
      assertEquals(
        "META-INF/airflow/sources.json",
        it.manifest.mainAttributes.getValue("Airflow-Java-SDK-Sources"),
      )
    }
  }

  @Test
  fun skipsEverythingWithoutMainClass(
    @TempDir dir: File,
  ) {
    project(dir, mainClass = null)
    val result = gradle(dir, "jar", "packDagSources")

    assertEquals(TaskOutcome.SKIPPED, result.task(":packDagSources")!!.outcome)
    val jar = File(dir, "build/libs/bundle-test.jar")
    assertFalse(entries(jar).any { it.startsWith("META-INF/airflow") })
    JarFile(jar).use { assertNull(it.manifest.mainAttributes.getValue("Airflow-Java-SDK-Sources")) }
  }
}
