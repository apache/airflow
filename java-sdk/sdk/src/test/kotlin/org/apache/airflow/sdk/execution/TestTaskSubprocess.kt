g/*
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

import java.io.InputStream
import java.io.OutputStream
import java.net.Socket

/**
 * Family of minimal subprocesses that simulate a Java task bundle process for integration testing
 * of [Supervisor]. Each object has a `main` method that can be spawned by [Supervisor.run].
 *
 * Protocol: connect to comm + logs sockets (via `--comm` / `--logs` CLI args), read the
 * [StartupDetails] frame from the supervisor, perform a behavior-specific action, then exit.
 */
private fun connectAndProcess(
  args: Array<String>,
  onFrame: (InputStream, OutputStream, IncomingFrame) -> Unit,
) {
  val commAddr = args.first { it.startsWith("--comm=") }.removePrefix("--comm=")
  val logsAddr = args.first { it.startsWith("--logs=") }.removePrefix("--logs=")
  val (commHost, commPort) = commAddr.split(":")
  val (logsHost, logsPort) = logsAddr.split(":")

  val commSocket = Socket(commHost, commPort.toInt())
  val logsSocket = Socket(logsHost, logsPort.toInt())
  try {
    val commIn = commSocket.getInputStream()
    val commOut = commSocket.getOutputStream()
    val frame = TaskSdkFrames.readFrame(commIn, TaskSdkFrames.toTaskTypes)
    onFrame(commIn, commOut, frame)
  } finally {
    commSocket.close()
    logsSocket.close()
  }
}

/** Reads StartupDetails and immediately sends [SucceedTask]. */
object TestSucceedSubprocess {
  @JvmStatic
  fun main(args: Array<String>) =
    connectAndProcess(args) { _, output, frame ->
      TaskSdkFrames.writeRequest(output, frame.id, SucceedTask())
    }
}

/** Reads StartupDetails and sends [TaskState] with state=failed. */
object TestFailSubprocess {
  @JvmStatic
  fun main(args: Array<String>) =
    connectAndProcess(args) { _, output, frame ->
      TaskSdkFrames.writeRequest(output, frame.id, TaskState(state = "failed"))
    }
}

/** Sends a [GetVariable] request, reads the response, then sends [SucceedTask]. */
object TestGetVariableSubprocess {
  @JvmStatic
  fun main(args: Array<String>) =
    connectAndProcess(args) { input, output, frame ->
      TaskSdkFrames.writeRequest(output, 10, GetVariable("test_var"))
      TaskSdkFrames.readFrame(input, TaskSdkFrames.toBundleProcessTypes)
      TaskSdkFrames.writeRequest(output, frame.id, SucceedTask())
    }
}

/** Sends a [GetConnection] request, reads the response, then sends [SucceedTask]. */
object TestGetConnectionSubprocess {
  @JvmStatic
  fun main(args: Array<String>) =
    connectAndProcess(args) { input, output, frame ->
      TaskSdkFrames.writeRequest(output, 10, GetConnection("test_conn"))
      TaskSdkFrames.readFrame(input, TaskSdkFrames.toBundleProcessTypes)
      TaskSdkFrames.writeRequest(output, frame.id, SucceedTask())
    }
}

/** Writes a message to stdout before succeeding, to verify log collection. */
object TestStdoutSubprocess {
  @JvmStatic
  fun main(args: Array<String>) {
    println("stdout-line-1")
    println("stdout-line-2")
    System.err.println("stderr-line-1")
    connectAndProcess(args) { _, output, frame ->
      TaskSdkFrames.writeRequest(output, frame.id, SucceedTask())
    }
  }
}

/** Sends [SucceedTask] but exits with non-zero code — tests exit-code override logic. */
object TestSucceedThenCrashSubprocess {
  @JvmStatic
  fun main(args: Array<String>) {
    connectAndProcess(args) { _, output, frame ->
      TaskSdkFrames.writeRequest(output, frame.id, SucceedTask())
    }
    // Force non-zero exit after the protocol completes cleanly.
    Runtime.getRuntime().halt(42)
  }
}
