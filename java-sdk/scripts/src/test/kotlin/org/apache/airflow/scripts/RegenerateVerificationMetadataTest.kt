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

package org.apache.airflow.scripts

import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.copyTo
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import kotlin.io.path.readLines
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Drives the real `scripts/ci/regenerate-verification-metadata.sh` against a throwaway
 * java-sdk tree whose `gradlew` is a stub, so nothing here builds anything or reaches the
 * network. The script resolves its own root relative to its location, which is what makes
 * that substitution work.
 */
class RegenerateVerificationMetadataTest {
  @TempDir
  lateinit var tempDir: Path

  @Test
  fun `drops superseded entries and restores the ASF header`() {
    val sdk = fixture(failingAttempts = 0)

    assertEquals(0, runScript(sdk).exitCode)

    val metadata = metadataOf(sdk)
    assertEquals(listOf("current"), componentNames(metadata), "a superseded entry survived")
    assertEquals("<!--", metadata.readLines()[1], "header should follow the XML declaration")
  }

  @Test
  fun `leaves a single header when Gradle keeps the one it was given`() {
    val sdk = fixture(failingAttempts = 0, gradleKeepsHeader = true)

    assertEquals(0, runScript(sdk).exitCode)

    val headers = metadataOf(sdk).readLines().count { it.startsWith("<!--") }
    assertEquals(1, headers, "header was inserted on top of the one already there")
  }

  @Test
  fun `retries a failing Gradle run and succeeds`() {
    val sdk = fixture(failingAttempts = 2)

    assertEquals(0, runScript(sdk).exitCode)

    assertEquals(3, sdk.resolve("gradlew-calls.log").readLines().size)
    assertEquals(listOf("current"), componentNames(metadataOf(sdk)))
  }

  @Test
  fun `restores the committed file when every attempt fails`() {
    val sdk = fixture(failingAttempts = ALWAYS)
    val committed = metadataOf(sdk).readText()

    assertEquals(1, runScript(sdk).exitCode)

    assertEquals(committed, metadataOf(sdk).readText())
  }

  @Test
  fun `inserts the same header the committed metadata carries`() {
    val sdk = fixture(failingAttempts = 0)

    assertEquals(0, runScript(sdk).exitCode)

    assertEquals(
      headerOf(javaSdkRoot.resolve(METADATA)),
      headerOf(metadataOf(sdk)),
      "the header in the script has drifted from the committed metadata",
    )
  }

  private fun fixture(
    failingAttempts: Int,
    gradleKeepsHeader: Boolean = false,
  ): Path {
    val sdk = tempDir.resolve("java-sdk")
    sdk.resolve("gradle").createDirectories()
    sdk.resolve("scripts/ci").createDirectories()
    javaSdkRoot.resolve(SCRIPT).copyTo(sdk.resolve(SCRIPT))
    metadataOf(sdk).writeText(committedMetadata())
    sdk.resolve("stub-failing-attempts").writeText("$failingAttempts\n")
    if (gradleKeepsHeader) {
      sdk.resolve("stub-keeps-header").createFile()
    }

    val stub = sdk.resolve("gradlew")
    stub.writeText(checkNotNull(javaClass.getResource("/stub-gradlew.sh")).readText())
    check(stub.toFile().setExecutable(true))

    // The script finishes by diffing the metadata against HEAD, so the fixture needs a
    // commit. The developer's own git config is ignored: signing or hooks configured
    // there would fail here.
    git(sdk, "init", "-q")
    git(sdk, "add", "-A")
    git(sdk, "commit", "-q", "-m", "fixture")
    return sdk
  }

  /** A committed file: the real ASF header, and one entry the next run will not resolve. */
  private fun committedMetadata(): String =
    listOf(
      """<?xml version="1.0" encoding="UTF-8"?>""",
      headerOf(javaSdkRoot.resolve(METADATA)),
      """<verification-metadata xmlns="https://schema.gradle.org/dependency-verification">""",
      "   <configuration>",
      "      <verify-metadata>true</verify-metadata>",
      "      <verify-signatures>false</verify-signatures>",
      "   </configuration>",
      "   <components>",
      """      <component group="org.example" name="superseded" version="1.0"/>""",
      """      <component group="org.example" name="current" version="2.0"/>""",
      "   </components>",
      "</verification-metadata>",
    ).joinToString("\n", postfix = "\n")

  private fun componentNames(metadata: Path): List<String> {
    val matches = COMPONENT_NAME.findAll(metadata.readText())
    return matches.map { it.groupValues[1] }.toList()
  }

  private fun headerOf(metadata: Path): String {
    val lines = metadata.readLines()
    return lines
      .subList(
        lines.indexOfFirst { it.startsWith("<!--") },
        lines.indexOfFirst { it.startsWith("-->") } + 1,
      ).joinToString("\n")
  }

  private fun metadataOf(sdk: Path): Path = sdk.resolve(METADATA)

  private fun runScript(sdk: Path): Run {
    val process =
      ProcessBuilder(sdk.resolve(SCRIPT).toString())
        .directory(sdk.toFile())
        .apply { environment()["RETRY_DELAY_SECONDS"] = "0" }
        .redirectErrorStream(true)
        .start()
    val output = process.inputStream.bufferedReader().readText()
    return Run(process.waitFor(), output)
  }

  private fun git(
    dir: Path,
    vararg arguments: String,
  ) {
    val process =
      ProcessBuilder(
        listOf("git", "-C", dir.toString(), "-c", "user.email=tests@example.com", "-c", "user.name=tests") +
          arguments,
      ).apply {
        environment()["GIT_CONFIG_GLOBAL"] = "/dev/null"
        environment()["GIT_CONFIG_SYSTEM"] = "/dev/null"
      }.redirectErrorStream(true)
        .start()
    val output = process.inputStream.bufferedReader().readText()
    check(process.waitFor() == 0) { "git ${arguments.joinToString(" ")} failed: $output" }
  }

  private data class Run(
    val exitCode: Int,
    val output: String,
  )

  private companion object {
    /** More than the script's attempt budget, so every attempt fails. */
    const val ALWAYS = 99
    const val SCRIPT = "scripts/ci/regenerate-verification-metadata.sh"
    const val METADATA = "gradle/verification-metadata.xml"
    val COMPONENT_NAME = Regex("<component [^>]*name=\"([^\"]+)\"")
    val javaSdkRoot: Path = Path.of(System.getProperty("javaSdkRoot"))
  }
}
