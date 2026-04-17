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
