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
import javax.tools.ToolProvider

/** Compiles [sources] (paths under [srcDir]) into [classesDir] with the JDK's own compiler. */
internal fun compileJava(
  srcDir: File,
  classesDir: File,
  vararg sources: String,
  options: List<String> = emptyList(),
) {
  classesDir.mkdirs()
  val compiler = checkNotNull(ToolProvider.getSystemJavaCompiler()) { "Tests need a JDK" }
  val args = options + listOf("-d", classesDir.path) + sources.map { File(srcDir, it).path }
  check(compiler.run(null, null, null, *args.toTypedArray()) == 0) { "javac failed for ${sources.toList()}" }
}

internal fun File.write(
  relativePath: String,
  text: String,
): File =
  File(this, relativePath).apply {
    parentFile.mkdirs()
    writeText(text)
  }
