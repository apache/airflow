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

class ClassFilesTest {
  private class Nested

  private fun bytesOf(cls: Class<*>): ByteArray =
    cls.getResourceAsStream("/" + cls.name.replace('.', '/') + ".class")!!.use { it.readBytes() }

  @Test
  fun readsSourceFileOfKotlinClass() {
    assertEquals("ClassFilesTest.kt", readSourceFile(bytesOf(ClassFilesTest::class.java)))
  }

  @Test
  fun readsSourceFileOfNestedClassFromItsOutermostFile() {
    assertEquals("ClassFilesTest.kt", readSourceFile(bytesOf(Nested::class.java)))
  }

  @Test
  fun readsSourceFileOfJdkClassesWithLongAndDoubleConstants() {
    // These carry every constant pool kind, including long, double and invokedynamic entries.
    assertEquals("String.java", readSourceFile(bytesOf(String::class.java)))
    assertEquals("Long.java", readSourceFile(bytesOf(Long::class.javaObjectType)))
    assertEquals("Double.java", readSourceFile(bytesOf(Double::class.javaObjectType)))
    assertEquals("HashMap.java", readSourceFile(bytesOf(java.util.HashMap::class.java)))
  }

  @Test
  fun readsSourceFileOfJavaClassWithSeveralTopLevelTypes(
    @TempDir dir: File,
  ) {
    dir.write("p/Main.java", "package p; public class Main { static final long L = 1L; } class Other {}")
    compileJava(dir, File(dir, "out"), "p/Main.java")

    assertEquals("Main.java", readSourceFile(File(dir, "out/p/Main.class").readBytes()))
    assertEquals("Main.java", readSourceFile(File(dir, "out/p/Other.class").readBytes()))
  }

  @Test
  fun returnsNullWithoutSourceFileAttribute(
    @TempDir dir: File,
  ) {
    dir.write("p/Main.java", "package p; public class Main {}")
    compileJava(dir, File(dir, "out"), "p/Main.java", options = listOf("-g:none"))

    assertNull(readSourceFile(File(dir, "out/p/Main.class").readBytes()))
  }

  @Test
  fun returnsNullForBytesThatAreNotAClassFile() {
    assertNull(readSourceFile(ByteArray(0)))
    assertNull(readSourceFile("not a class file".toByteArray()))
    assertNull(readSourceFile(bytesOf(ClassFilesTest::class.java).copyOf(40)))
  }
}
