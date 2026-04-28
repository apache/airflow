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

import java.nio.file.Files
import java.nio.file.Path

/** True when [this] points to a regular file whose name ends with `.jar`. */
fun Path.isJarFile(): Boolean = Files.isRegularFile(this) && fileName.toString().endsWith(".jar")

/** Lists JAR files in [directory], sorted by path name. */
fun jarFiles(directory: Path): List<Path> {
  if (!Files.isDirectory(directory)) return emptyList()
  val jars = mutableListOf<Path>()
  Files.list(directory).use { paths ->
    paths
      .filter { it.isJarFile() }
      .sorted()
      .forEach { jars.add(it) }
  }
  return jars
}

/** True when [directory] contains at least one JAR file. */
fun containsJars(directory: Path): Boolean =
  Files.isDirectory(directory) &&
    Files.list(directory).use { paths -> paths.anyMatch { it.isJarFile() } }
