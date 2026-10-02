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
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class BundleDigestTest {
  @TempDir
  lateinit var tmp: File

  private fun classesDir(
    name: String,
    files: Map<String, String>,
  ): File =
    tmp.resolve(name).also { dir ->
      files.forEach { (path, content) -> dir.resolve(path).apply { parentFile.mkdirs() }.writeText(content) }
    }

  private fun jar(
    name: String,
    entries: List<Pair<String, String>>,
    time: Long = 0L,
  ): File =
    tmp.resolve(name).also { file ->
      file.parentFile.mkdirs()
      ZipOutputStream(file.outputStream()).use { zip ->
        entries.forEach { (path, content) ->
          zip.putNextEntry(ZipEntry(path).apply { this.time = time })
          zip.write(content.toByteArray())
          zip.closeEntry()
        }
      }
    }

  private val classes = mapOf("com/example/Main.class" to "main", "com/example/Handler.class" to "handler")
  private val depEntries = listOf("dep/A.class" to "a", "dep/B.class" to "b")

  private fun digest(
    mainClass: String = "com.example.Main",
    fatJar: Boolean = true,
    outputs: List<File> = listOf(classesDir("classes", classes)),
    classpath: List<File> = listOf(jar("lib/dep.jar", depEntries)),
  ) = BundleDigest.compute(mainClass, fatJar, outputs, classpath)

  @Test
  @DisplayName("Should give the same SHA-256 for the same content")
  fun sameContentSameDigest() {
    val first = digest()
    assertTrue(Regex("[0-9a-f]{64}").matches(first))
    assertEquals(first, digest())
  }

  @Test
  @DisplayName("Should ignore dependency archive timestamps and entry order")
  fun ignoresArchiveMetadata() {
    val first = digest()
    val rezipped = jar("lib/dep.jar", depEntries.reversed(), time = 1_700_000_000_000L)

    assertEquals(first, digest(classpath = listOf(rezipped)))
  }

  @Test
  @DisplayName("Should change when anything the bundle runs changes")
  fun changesWithContent() {
    val first = digest()

    assertNotEquals(first, digest(outputs = listOf(classesDir("changed", classes + ("com/example/Handler.class" to "HANDLER")))))
    assertNotEquals(first, digest(classpath = listOf(jar("lib/dep.jar", depEntries), jar("lib/extra.jar", depEntries))))
    assertNotEquals(first, digest(classpath = listOf(jar("lib/dep.jar", listOf("dep/A.class" to "a", "dep/B.class" to "bb")))))
    assertNotEquals(first, digest(mainClass = "com.example.Other"))
    assertNotEquals(first, digest(fatJar = false))
  }

  @Test
  @DisplayName("Should change when a classpath directory's content changes")
  fun changesWithClasspathDirectoryContent() {
    val included = classesDir("included", classes)
    val first = digest(classpath = listOf(included))

    included.resolve("com/example/Handler.class").writeText("HANDLER")

    assertNotEquals(first, digest(classpath = listOf(included)))
  }

  @Test
  @DisplayName("Should change when a non-zip classpath file's content changes")
  fun changesWithNonZipClasspathFileContent() {
    val native = tmp.resolve("lib/native.so").apply { parentFile.mkdirs() }
    native.writeText("native")
    val first = digest(classpath = listOf(native))

    native.writeText("NATIVE")

    assertNotEquals(first, digest(classpath = listOf(native)))
  }
}
