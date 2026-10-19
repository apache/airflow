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

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException

private const val CLASS_MAGIC = 0xCAFEBABE.toInt()

/**
 * Reads the `SourceFile` attribute of a class file, which javac, kotlinc and scalac all
 * write, or returns `null` if the class has none or the bytes are not a class file.
 */
internal fun readSourceFile(classFile: ByteArray): String? =
  try {
    DataInputStream(ByteArrayInputStream(classFile)).use(::readSourceFile)
  } catch (_: IOException) {
    null
  }

private fun readSourceFile(input: DataInputStream): String? {
  if (input.readInt() != CLASS_MAGIC) return null
  input.skipBytes(4) // minor and major version

  val utf8 = arrayOfNulls<String>(input.readUnsignedShort())
  var index = 1
  while (index < utf8.size) {
    when (val tag = input.readUnsignedByte()) {
      1 -> utf8[index] = input.readUTF()
      3, 4, 9, 10, 11, 12, 17, 18 -> input.skipBytes(4)
      5, 6 -> {
        input.skipBytes(8)
        index++ // long and double take two slots
      }
      7, 8, 16, 19, 20 -> input.skipBytes(2)
      15 -> input.skipBytes(3)
      else -> throw IOException("Unknown constant pool tag $tag")
    }
    index++
  }

  input.skipBytes(6) // access flags, this class, super class
  input.skipBytes(2 * input.readUnsignedShort()) // interfaces
  repeat(2) { skipMembers(input) } // fields, then methods

  repeat(input.readUnsignedShort()) {
    val name = utf8.getOrNull(input.readUnsignedShort())
    val length = input.readInt()
    if (name == "SourceFile" && length == 2) return utf8.getOrNull(input.readUnsignedShort())
    input.skipBytes(length)
  }
  return null
}

private fun skipMembers(input: DataInputStream) {
  repeat(input.readUnsignedShort()) {
    input.skipBytes(6) // access flags, name, descriptor
    skipAttributes(input)
  }
}

private fun skipAttributes(input: DataInputStream) {
  repeat(input.readUnsignedShort()) {
    input.skipBytes(2)
    input.skipBytes(input.readInt())
  }
}
