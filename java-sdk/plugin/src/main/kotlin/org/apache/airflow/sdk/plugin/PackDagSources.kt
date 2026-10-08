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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.jvm.toolchain.JavaLauncher
import java.io.File
import java.util.concurrent.TimeUnit

internal const val SOURCES_MANIFEST_ATTRIBUTE = "Airflow-Java-SDK-Sources"
internal const val SOURCES_JSON_PATH = "META-INF/airflow/sources.json"
internal const val SOURCES_DIR_PATH = "META-INF/airflow/sources"

/**
 * How long the describe run gets before the build gives up on it and packs
 * the entrypoint's source alone. A `main` that does not reach
 * `Server.create(args)` never answers `--describe-sources`, and one that
 * starts serving instead would otherwise hold the build open.
 */
private const val DESCRIBE_TIMEOUT_SECONDS = 120L
private const val DRAIN_TIMEOUT_MILLIS = 2_000L

/**
 * Finds the source file of a class by its `SourceFile` attribute: the class
 * file's package path plus that name, looked up in each source directory, or
 * else the one source file of that name when the layout does not match the
 * package.
 */
internal class SourceLocator(
  private val classesDirs: Iterable<File>,
  private val sourceDirs: Iterable<File>,
) {
  /** Every source file under the source dirs by file name, walked once and only if a lookup misses. */
  private val pathsByName: Map<String, List<String>> by lazy {
    sourceDirs
      .flatMap { dir -> dir.walkTopDown().filter(File::isFile).map { it.relativeTo(dir).invariantSeparatorsPath } }
      .groupBy { it.substringAfterLast('/') }
  }

  /** Path of the source file relative to its source directory, or `null` if it cannot be found. */
  fun locate(className: String): String? {
    val classFile =
      classesDirs
        .map { File(it, className.replace('.', '/') + ".class") }
        .firstOrNull { it.isFile } ?: return null
    val sourceFile =
      readSourceFile(classFile.readBytes())?.takeIf { it.isNotEmpty() && '/' !in it && '\\' !in it } ?: return null
    val packagePath = className.substringBeforeLast('.', "").replace('.', '/')
    val byPackage = listOfNotNull(packagePath.ifEmpty { null }, sourceFile).joinToString("/")
    if (sourceDirs.any { File(it, byPackage).isFile }) return byPackage
    // Neither javac nor Kotlin requires the package to match the directory, so fall back to the
    // file name when exactly one source tree holds it. An ambiguous name is left unresolved.
    return pathsByName[sourceFile]?.distinct()?.singleOrNull()
  }

  fun file(relativePath: String): File = sourceDirs.map { File(it, relativePath) }.first { it.isFile }
}

/**
 * The `META-INF/airflow/sources.json` body: `entrypoint_path` and each Dag ID
 * in `dag_source_paths`.
 *
 * Every path is relative to the source directory the file was found in, which
 * is also where it sits under `META-INF/airflow/sources/` in the JAR, so a
 * reader resolves one by prefixing that directory.
 */
internal fun sourcesJson(
  entrypointPath: String?,
  dagSourcePaths: Map<String, String>,
): String =
  JsonOutput.prettyPrint(
    JsonOutput.toJson(
      linkedMapOf<String, Any>().apply {
        entrypointPath?.let { put("entrypoint_path", it) }
        put("dag_source_paths", dagSourcePaths)
      },
    ),
  )

/**
 * Runs the bundle's `mainClass` in describe mode to learn which class declared
 * each Dag, then collects those classes' source files, and the entry point's,
 * for the bundle JAR to carry.
 */
@CacheableTask
abstract class PackDagSources : DefaultTask() {
  @get:Input
  @get:Optional
  abstract val mainClass: Property<String>

  @get:Classpath
  abstract val classesDirs: ConfigurableFileCollection

  @get:Classpath
  abstract val runtimeClasspath: ConfigurableFileCollection

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceDirs: ConfigurableFileCollection

  @get:Nested
  @get:Optional
  abstract val launcher: Property<JavaLauncher>

  @get:OutputFile
  abstract val describeFile: RegularFileProperty

  @get:OutputDirectory
  abstract val sourcesDir: DirectoryProperty

  private var describeFailed = false

  init {
    outputs.doNotCacheIf("the Dag describe run failed") { describeFailed }
    // A failed run leaves describeFile missing, which Gradle would otherwise read as unchanged.
    outputs.upToDateWhen { describeFile.get().asFile.isFile }
  }

  @TaskAction
  fun pack() {
    val describe = describeFile.get().asFile
    val root = sourcesDir.get().asFile
    describe.delete()
    root.deleteRecursively()
    describe.parentFile.mkdirs()

    val main = mainClass.get()
    val declaringClasses = describeDags(main, describe)
    val locator = SourceLocator(classesDirs.files, sourceDirs.files)

    val entrypoint = locator.locate(main)
    if (entrypoint == null) {
      logger.warn("No source file found for entrypoint class {}; the Code view will show no source for this JAR", main)
    }
    val dagPaths = linkedMapOf<String, String>()
    declaringClasses.forEach { (dagId, className) ->
      val path = locator.locate(className)
      if (path == null) {
        logger.warn("No source file found for class {} of Dag '{}'; it falls back to the entrypoint", className, dagId)
      } else {
        dagPaths[dagId] = path
      }
    }

    (listOfNotNull(entrypoint) + dagPaths.values).distinct().forEach { path ->
      val target = File(root, "$SOURCES_DIR_PATH/$path")
      target.parentFile.mkdirs()
      locator.file(path).copyTo(target)
    }
    File(root, SOURCES_JSON_PATH).apply {
      parentFile.mkdirs()
      writeText(sourcesJson(entrypoint, dagPaths) + "\n")
    }
  }

  private fun describeDags(
    main: String,
    describe: File,
  ): Map<String, String> {
    var output = ""
    val failure =
      try {
        val java =
          launcher.orNull
            ?.executablePath
            ?.asFile
            ?.absolutePath ?: "java"
        val classpath = (classesDirs + runtimeClasspath).joinToString(File.pathSeparator) { it.absolutePath }
        val process =
          ProcessBuilder(java, "-cp", classpath, main, "--describe-sources", describe.absolutePath)
            .redirectErrorStream(true)
            .start()
        // Drained on its own thread: a main that writes more than the pipe
        // holds would otherwise block before the timeout could fire.
        val drain = Thread { output = process.inputStream.bufferedReader().readText() }
        drain.isDaemon = true
        drain.start()
        val finished = process.waitFor(DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        if (!finished) process.destroyForcibly()
        drain.join(DRAIN_TIMEOUT_MILLIS)
        when {
          !finished -> "it did not finish within $DESCRIBE_TIMEOUT_SECONDS seconds"
          process.exitValue() != 0 -> "exit code ${process.exitValue()}"
          else -> null
        }
      } catch (e: Exception) {
        e.message ?: e.javaClass.simpleName
      }
    val log = output.trim()
    val parsed =
      describe
        .takeIf { it.isFile }
        ?.let {
          @Suppress("UNCHECKED_CAST")
          (runCatching { JsonSlurper().parse(it) }.getOrNull() as? Map<String, Any?>)
        }
    if (parsed != null) {
      if (failure != null) {
        logger.warn(
          "'{}' wrote each Dag's source but {}; its sources are used anyway.{}",
          main,
          failure,
          if (log.isEmpty()) "" else "\n$log",
        )
      }
      return parsed.mapNotNull { (id, cls) -> (cls as? String)?.let { id to it } }.toMap(linkedMapOf())
    }
    describeFailed = true
    val why = failure ?: "it wrote no valid --describe-sources file; does main pass its args to Server.create?"
    logger.warn(
      "Could not read each Dag's source from '{}' ({}); only its entrypoint source is packed.{}",
      main,
      why,
      if (log.isEmpty()) "" else "\n$log",
    )
    return emptyMap()
  }
}
