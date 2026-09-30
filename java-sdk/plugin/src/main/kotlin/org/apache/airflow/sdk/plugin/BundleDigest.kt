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

import java.io.File
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.zip.ZipException
import java.util.zip.ZipFile

/**
 * The `Airflow-Cache-Digest` a bundle JAR carries: a SHA-256 over what the
 * bundle runs, which Airflow compares to tell whether a JAR changed since it
 * last asked it for its task handlers.
 *
 * It hashes content, never archive metadata, so a rebuild that changes
 * nothing gives the same value although the JAR's bytes differ (Gradle keeps
 * file timestamps in archives by default). It covers the main class, the
 * packaging mode, every file of the main source set's output, and every
 * runtime dependency. A dependency JAR contributes its entries' names, sizes
 * and CRC-32 values from the central directory, so no entry is decompressed.
 */
internal object BundleDigest {
  const val ATTRIBUTE = "Airflow-Cache-Digest"

  fun compute(
    mainClass: String?,
    fatJar: Boolean,
    outputs: Iterable<File>,
    classpath: Iterable<File>,
  ): String {
    val writer = Writer()
    writer.write("airflow-cache-digest", 1, mainClass.orEmpty(), if (fatJar) "fat" else "thin")
    writer.writeTree(outputs)
    for (entry in classpath) {
      when {
        entry.isDirectory -> writer.writeTree(listOf(entry))
        entry.isFile -> writer.writeArchive(entry)
      }
    }
    return writer.hex()
  }

  private class Writer {
    private val digest = MessageDigest.getInstance("SHA-256")

    // Each token is length-prefixed, so no two token sequences hash alike.
    fun write(vararg tokens: Any) {
      for (token in tokens) {
        val bytes = token.toString().toByteArray(Charsets.UTF_8)
        digest.update(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(bytes.size).array())
        digest.update(bytes)
      }
    }

    fun writeTree(roots: Iterable<File>) {
      roots
        .filter { it.isDirectory }
        .flatMap { root ->
          root.walkTopDown().filter { it.isFile }.map { root.toPath().relativize(it.toPath()).joinToString("/") to it }
        }.sortedBy { it.first }
        .forEach { (path, file) -> write("file", path, sha256(file)) }
    }

    fun writeArchive(file: File) {
      try {
        ZipFile(file).use { zip ->
          write("archive", file.name)
          zip
            .entries()
            .asSequence()
            .filterNot { it.isDirectory }
            .sortedBy { it.name }
            .forEach { write("entry", it.name, it.size, it.crc) }
        }
      } catch (_: ZipException) {
        write("file", file.name, sha256(file))
      }
    }

    fun hex(): String = digest.digest().joinToString("") { "%02x".format(it) }

    private fun sha256(file: File): String {
      val fileDigest = MessageDigest.getInstance("SHA-256")
      file.inputStream().use { input ->
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
          val read = input.read(buffer)
          if (read < 0) break
          fileDigest.update(buffer, 0, read)
        }
      }
      return fileDigest.digest().joinToString("") { "%02x".format(it) }
    }
  }
}
