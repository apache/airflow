// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.airflow.sdk.conformance

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

// This model exists only to emit the build-time manifest, so it is deliberately `internal`: it is
// not part of the SDK's published surface, and keeping it out means changing the manifest schema
// never becomes a source/binary compatibility concern for SDK users.

/** Support for a single Language SDK conformance dimension. */
@Serializable
internal data class CapabilityEntry(
  val supported: Boolean,
  val since: String? = null,
  val note: String = "",
)

/** The machine-readable capability manifest serialised to java-sdk/generated/lang-sdk/capabilities.json. */
@Serializable
internal data class CapabilityManifest(
  val sdk: String,
  @SerialName("supervisor_schema_version") val supervisorSchemaVersion: String,
  @SerialName("min_airflow_version") val minAirflowVersion: String,
  val states: Map<String, CapabilityEntry>,
  val capabilities: Map<String, CapabilityEntry>,
)

private const val MIN_AIRFLOW_VERSION = "3.3"

// Keep in sync with airflowSupervisorSchemaVersion in java-sdk/gradle.properties.
private const val SUPERVISOR_SCHEMA_VERSION = "2026-06-16"

private fun yes(note: String = ""): CapabilityEntry = CapabilityEntry(supported = true, since = MIN_AIRFLOW_VERSION, note = note)

private fun no(note: String = ""): CapabilityEntry = CapabilityEntry(supported = false, since = null, note = note)

/**
 * Single source of truth for what the Java SDK supports. Update it when the runtime gains or loses a
 * conformance dimension, then regenerate capabilities.json. The normative meaning of each dimension is
 * defined in contributing-docs/30_new_language_sdk.rst.
 *
 * The runtime terminates a task with SucceedTask, RetryTask, or TaskState (failed/removed); it does not
 * yet emit skipped, DeferTask, RescheduleTask, or AwaitInputTask.
 */
internal val Capabilities: CapabilityManifest =
  CapabilityManifest(
    sdk = "java",
    supervisorSchemaVersion = SUPERVISOR_SCHEMA_VERSION,
    minAirflowVersion = MIN_AIRFLOW_VERSION,
    states =
      linkedMapOf(
        "success" to yes(),
        "failed" to yes(),
        "up_for_retry" to yes("RetryTask"),
        "skipped" to no("runtime does not emit TaskState skipped yet"),
        "deferred" to no("runtime does not emit DeferTask yet"),
        "up_for_reschedule" to no("runtime does not emit RescheduleTask yet"),
        "awaiting_input" to no("runtime does not emit AwaitInputTask yet"),
        "removed" to yes(),
      ),
    // Runtime capabilities reflect the task-facing Client surface; native-Dag authoring is not
    // implemented yet, so every native capability is unsupported.
    capabilities =
      linkedMapOf(
        "mixed-lang-stub-target" to yes("@task.stub"),
        "task-logging" to yes("SLF4J + JPL bridged to the task log"),
        "xcom-read-write" to yes(),
        "connection-read" to yes(),
        "variable-read-write" to no("getVariable only; no write over the comm socket yet"),
        "self-contained-bundle" to yes("Airflow metadata embedded in the jar artifact"),
        "task-state-store" to no("no task-facing state-store API yet"),
        "asset-state-store" to no("no task-facing state-store API yet"),
        "asset-event-emit" to no("runtime does not emit asset events yet"),
        "asset-event-read" to no("no task-facing asset-event API yet"),
        "native-dag-authoring" to no("native Dag authoring not implemented yet"),
        "task-args" to no(),
        "dag-params" to no(),
        "taskflow-dependencies" to no(),
        "branching" to no(),
        "dag-test" to no(),
        "task-group" to no(),
        "dynamic-task-mapping" to no(),
        "asset-inlets-outlets" to no(),
        "asset-scheduling" to no(),
        "object-store" to no(),
      ),
  )

/** Print the capability manifest as JSON on stdout; wired to the `:sdk:dumpCapabilities` Gradle task. */
internal fun main() {
  val json =
    Json {
      prettyPrint = true
      encodeDefaults = true
    }
  println(json.encodeToString(Capabilities))
}
