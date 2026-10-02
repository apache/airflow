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
import org.gradle.process.ExecOperations
import java.io.ByteArrayOutputStream
import java.io.File
import javax.inject.Inject

internal const val SOURCES_MANIFEST_ATTRIBUTE = "Airflow-Java-SDK-Sources"
internal const val SOURCES_JSON_PATH = "META-INF/airflow/sources.json"
internal const val SOURCES_DIR_PATH = "META-INF/airflow/sources"

/**
 * Finds the source file of a class by its `SourceFile` attribute: the class
 * file's package path plus that name, looked up in each source directory.
 */
internal class SourceLocator(
  private val classesDirs: Iterable<File>,
  private val sourceDirs: Iterable<File>,
) {
  /** Path of the source file relative to its source directory, or `null` if it cannot be found. */
  fun locate(className: String): String? {
    val packagePath = className.substringBeforeLast('.', "").replace('.', '/')
    val classFile =
      classesDirs
        .map { File(it, className.replace('.', '/') + ".class") }
        .firstOrNull { it.isFile } ?: return null
    val sourceFile = readSourceFile(classFile.readBytes())?.takeIf { it.isNotEmpty() && '/' !in it && '\\' !in it }
    val relative = listOfNotNull(packagePath.ifEmpty { null }, sourceFile ?: return null).joinToString("/")
    return relative.takeIf { path -> sourceDirs.any { File(it, path).isFile } }
  }

  fun file(relativePath: String): File = sourceDirs.map { File(it, relativePath) }.first { it.isFile }
}

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
  @get:Inject
  abstract val execOperations: ExecOperations

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
    val dagPaths = linkedMapOf<String, String>()
    declaringClasses.forEach { (dagId, className) ->
      val path = locator.locate(className)
      if (path == null) {
        logger.info("No source file found for class {} of Dag '{}'; it falls back to the entrypoint", className, dagId)
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
    val output = ByteArrayOutputStream()
    val failure =
      try {
        val result =
          execOperations.javaexec { spec ->
            launcher.orNull?.let { spec.executable(it.executablePath.asFile.absolutePath) }
            spec.classpath(classesDirs, runtimeClasspath)
            spec.mainClass.set(main)
            spec.args("--describe-sources", describe.absolutePath)
            spec.isIgnoreExitValue = true
            spec.standardOutput = output
            spec.errorOutput = output
          }
        if (result.exitValue != 0) "exit code ${result.exitValue}" else null
      } catch (e: Exception) {
        e.message ?: e.javaClass.simpleName
      }
    if (failure == null && describe.isFile) {
      @Suppress("UNCHECKED_CAST")
      (runCatching { JsonSlurper().parse(describe) }.getOrNull() as? Map<String, Any?>)?.let { parsed ->
        return parsed.mapNotNull { (id, cls) -> (cls as? String)?.let { id to it } }.toMap(linkedMapOf())
      }
    }
    describeFailed = true
    val why = failure ?: "it wrote no valid --describe-sources file; does main call Server.serve?"
    val log = output.toString().trim()
    logger.warn(
      "Could not read each Dag's source from '{}' ({}); only its entrypoint source is packed.{}",
      main,
      why,
      if (log.isEmpty()) "" else "\n$log",
    )
    return emptyMap()
  }
}
