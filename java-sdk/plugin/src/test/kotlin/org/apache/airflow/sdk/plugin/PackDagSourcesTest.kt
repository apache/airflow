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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class PackDagSourcesTest {
  private fun project(dir: File): Pair<File, File> {
    val src = File(dir, "src")
    val extra = File(dir, "extra")
    src.write("com/example/Main.java", "package com.example; public class Main {}")
    src.write("com/example/Same.java", "package com.example; public class Same { class Inner {} }")
    extra.write("com/example/dags/Reports.java", "package com.example.dags; public class Reports {} class Helper {}")
    src.write("com/example/NoSource.java", "package com.example; public class NoSource {}")
    val classes = File(dir, "classes")
    compileJava(
      src,
      classes,
      "com/example/Main.java",
      "com/example/Same.java",
      "com/example/NoSource.java",
    )
    compileJava(extra, classes, "com/example/dags/Reports.java")
    File(src, "com/example/NoSource.java").delete()
    return src to classes
  }

  @Test
  fun locatesSourceByPackagePathAndSourceFileAttribute(
    @TempDir dir: File,
  ) {
    val (src, classes) = project(dir)
    val locator = SourceLocator(listOf(classes), listOf(src, File(dir, "extra")))

    assertEquals("com/example/Main.java", locator.locate("com.example.Main"))
    assertEquals("com/example/Same.java", locator.locate("com.example.Same\$Inner"))
    assertEquals("com/example/dags/Reports.java", locator.locate("com.example.dags.Reports"))
    assertEquals("com/example/dags/Reports.java", locator.locate("com.example.dags.Helper"))
    assertEquals(File(dir, "extra/com/example/dags/Reports.java"), locator.file("com/example/dags/Reports.java"))
  }

  @Test
  fun locatesNothingForUnknownClassOrMissingSource(
    @TempDir dir: File,
  ) {
    val (src, classes) = project(dir)
    val locator = SourceLocator(listOf(classes), listOf(src))

    assertNull(locator.locate("com.example.Missing"))
    assertNull(locator.locate("com.example.NoSource"))
    assertNull(locator.locate("com.example.dags.Reports"))
  }

  @Test
  fun rendersSourcesJson() {
    assertEquals(
      """{"entrypoint_path":"com/example/Main.java","dag_source_paths":{"orders":"com/example/Main.java"}}""",
      compact(sourcesJson("com/example/Main.java", mapOf("orders" to "com/example/Main.java"))),
    )
  }

  @Test
  fun omitsEntrypointPathWhenUnresolved() {
    assertEquals("""{"dag_source_paths":{}}""", compact(sourcesJson(null, emptyMap())))
  }

  private fun compact(json: String) = (groovy.json.JsonSlurper().parseText(json) as Map<*, *>).let(groovy.json.JsonOutput::toJson)
}
