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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.airflow.sdk.execution.containsJars
import org.apache.airflow.sdk.execution.isJarFile
import org.apache.airflow.sdk.execution.jarFiles
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.JarFile

const val METADATA_MANIFEST_KEY = "Airflow-Java-SDK-Metadata"

private val yamlMapper = ObjectMapper(YAMLFactory())

/**
 * A fully resolved bundle: everything needed to start the bundle process.
 */
data class ResolvedBundle(
  val mainClass: String,
  val classpath: String,
)

/**
 * Scans [bundlesDir] for Java DAG bundles by checking JAR manifests for the
 * [METADATA_MANIFEST_KEY] attribute and reading the referenced YAML metadata.
 *
 * Supports two layouts:
 * - **Nested**: each immediate subdirectory of [bundlesDir] is a bundle home.
 * - **Flat**: [bundlesDir] itself contains the bundle JARs.
 *
 * Returns a mapping from dag_id to a [ResolvedBundle] with mainClass and classpath.
 */
fun scanBundles(bundlesDir: Path): Map<String, ResolvedBundle> {
  if (!Files.isDirectory(bundlesDir)) return emptyMap()
  val result = mutableMapOf<String, ResolvedBundle>()

  // Check each immediate subdirectory as a potential bundle home.
  Files.list(bundlesDir).use { paths ->
    paths.filter { Files.isDirectory(it) }.forEach { candidate ->
      collectBundleDags(candidate, result)
    }
  }

  // Also check bundlesDir itself (flat layout).
  collectBundleDags(bundlesDir, result)

  return result
}

private fun collectBundleDags(
  candidate: Path,
  result: MutableMap<String, ResolvedBundle>,
) {
  val bundleHome = normalizeBundleHome(candidate)
  val resolved = resolveBundle(bundleHome) ?: return
  for (dagId in resolved.first) {
    result.putIfAbsent(dagId, resolved.second)
  }
}

/**
 * Inspects JARs in [bundleHome] for [METADATA_MANIFEST_KEY] and Main-Class.
 * Returns (dagIds, ResolvedBundle) or null if no JAR carries the metadata attribute.
 */
private fun resolveBundle(bundleHome: Path): Pair<Set<String>, ResolvedBundle>? {
  val jars = jarFiles(bundleHome)
  if (jars.isEmpty()) return null

  for (jarPath in jars) {
    JarFile(jarPath.toFile()).use { jar ->
      val attrs = jar.manifest?.mainAttributes ?: return@use
      val metadataFile = attrs.getValue(METADATA_MANIFEST_KEY) ?: return@use
      val mainClass = attrs.getValue("Main-Class") ?: return@use
      val entry = jar.getJarEntry(metadataFile) ?: return@use
      val content = jar.getInputStream(entry).bufferedReader().readText()
      val dagIds = parseDagIdsFromYaml(content)
      if (dagIds.isEmpty()) return@use

      val classpath =
        jars
          .map { it.toAbsolutePath().normalize().toString() }
          .joinToString(File.pathSeparator)

      return dagIds to ResolvedBundle(mainClass, classpath)
    }
  }
  return null
}

fun readBundleDagIds(bundleHome: Path): Set<String> {
  for (jarPath in jarFiles(bundleHome)) {
    JarFile(jarPath.toFile()).use { jar ->
      val metadataFile = jar.manifest?.mainAttributes?.getValue(METADATA_MANIFEST_KEY) ?: return@use
      val entry = jar.getJarEntry(metadataFile) ?: return@use
      val content = jar.getInputStream(entry).bufferedReader().readText()
      return parseDagIdsFromYaml(content)
    }
  }
  return emptySet()
}

fun parseDagIdsFromYaml(yaml: String): Set<String> {
  val root = yamlMapper.readTree(yaml)
  val dagsNode = root.get("dags") ?: return emptySet()
  val dagIds = mutableSetOf<String>()
  dagsNode.fieldNames().forEachRemaining { dagIds.add(it) }
  return dagIds
}

private fun normalizeBundleHome(path: Path): Path {
  val normalized = path.toAbsolutePath().normalize()
  if (normalized.isJarFile()) return normalized.parent
  val lib = normalized.resolve("lib")
  return if (containsJars(lib)) lib else normalized
}
