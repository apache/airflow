package org.apache.airflow.sdk.execution

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.execution.api.client.ApiClient
import org.apache.airflow.sdk.execution.api.model.AssetProfile
import org.apache.airflow.sdk.execution.api.model.BundleInfo
import org.apache.airflow.sdk.execution.api.model.TIRunContext
import org.apache.airflow.sdk.execution.api.model.TISuccessStatePayload
import org.apache.airflow.sdk.execution.api.model.TaskInstance
import retrofit2.Call
import java.time.OffsetDateTime
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.system.exitProcess

data class IncomingFrame(
  val id: Int,
  val body: Any?,
)

data class OutgoingFrame(
  val id: Int,
  val body: Any,
)

class ErrorResponse {
  @JsonProperty("error")
  var error: String = "" // TODO: Use enum.

  @JsonProperty("detail")
  var detail: Any? = null
}

class DagFileParseRequest {
  var file: String = ""

  @JsonProperty("bundle_path")
  var bundlePath: String = ""
}

class StartupDetails {
  @JsonProperty("ti")
  lateinit var ti: TaskInstance

  @JsonProperty("dag_rel_path")
  var dagRelPath: String = ""

  @JsonProperty("bundle_info")
  lateinit var bundleInfo: BundleInfo

  @JsonProperty("start_date")
  lateinit var startDate: OffsetDateTime

  @JsonProperty("ti_context")
  lateinit var tiContext: TIRunContext

  @JsonProperty("sentry_integration")
  var sentryIntegration: String = ""
}

class SucceedTask : TISuccessStatePayload {
  constructor(
    endDate: OffsetDateTime = OffsetDateTime.now(),
    taskOutlets: List<AssetProfile> = emptyList(),
    outletEvents: List<Map<String, Any?>> = emptyList(),
  ) {
    endDate(endDate)
    taskOutlets(taskOutlets)
    outletEvents(outletEvents)
  }

  val type = "SucceedTask"
}

@JsonPropertyOrder(value = ["state", "end_date", "type"])
data class TaskState(
  val state: String, // TODO: Use enum (failed, removed, skipped) and custom serialization.
  @get:JsonProperty("end_date") val endDate: OffsetDateTime = OffsetDateTime.now(),
) {
  val type = "TaskState"
}

data class GetConnection(
  @get:JsonProperty("conn_id") val id: String,
) {
  val type = "GetConnection"
}

data class GetVariable(
  val key: String,
) {
  val type = "GetVariable"
}

data class GetXCom(
  val key: String,
  @get:JsonProperty("dag_id") val dagId: String,
  @get:JsonProperty("task_id") val taskId: String,
  @get:JsonProperty("run_id") val runId: String,
  @get:JsonProperty("map_index") val mapIndex: Int? = null,
  @get:JsonProperty("include_prior_dates") val includePriorDates: Boolean = false,
) {
  val type = "GetXCom"
}

data class SetXCom(
  val key: String,
  val value: Any,
  @get:JsonProperty("dag_id") val dagId: String,
  @get:JsonProperty("task_id") val taskId: String,
  @get:JsonProperty("run_id") val runId: String,
  @get:JsonProperty("map_index") val mapIndex: Int,
  @get:JsonProperty("mapped_length") val mappedLength: Int? = null,
) {
  val type = "SetXCom"
}

@OptIn(ExperimentalAtomicApi::class)
class CoordinatorComm(
  private val bundle: Bundle,
  private val reader: ByteReadChannel,
  private val writer: ByteWriteChannel,
) {
  internal companion object {
    private val logger = Logger(CoordinatorComm::class)

    fun encode(outgoing: OutgoingFrame): ByteArray {
      val body =
        when (val message = outgoing.body) {
          is DagParsingResult -> message.serialize()
          else -> message
        }
      return TaskSdkFrames.encodeRequest(outgoing.id, body)
    }

    fun decode(bytes: ByteArray): IncomingFrame = TaskSdkFrames.decode(bytes, TaskSdkFrames.toBundleProcessTypes)
  }

  private val nextId = AtomicInt(0)
  private var shutDownRequested = false

  suspend fun startProcessing() {
    while (!shutDownRequested) {
      processOnce(::handleIncoming)
    }
    logger.debug("Goodbye")
  }

  private suspend fun processOnce(handle: suspend (IncomingFrame) -> Unit) {
    val prefix = reader.readByteArray(4) // First 4 bytes as length.
    if (prefix.size != 4) { // Something is terribly wrong. Let's bail.
      logger.error("Need 4 prefix bytes", mapOf("actual" to prefix.size))
      shutDownRequested = true
      return
    }

    val payloadLength = TaskSdkFrames.parseLengthPrefix(prefix)
    val payload = reader.readByteArray(payloadLength)
    if (payload.size != payloadLength) { // Something is terribly wrong. Let's bail.
      logger.error(
        "Payload length not right",
        mapOf("expect" to payloadLength, "receive" to payload.size),
      )
      shutDownRequested = true
      return
    }
    val frame = decode(payload)
    logger.debug("Handling", mapOf("id" to frame.id))
    handle(frame)
  }

  private suspend fun sendMessage(
    id: Int,
    body: Any,
  ) {
    val data = encode(OutgoingFrame(id, body))
    logger.debug("Sending", mapOf("id" to id, "body" to body))
    writer.writeByteArray(TaskSdkFrames.lengthPrefix(data.size))
    writer.writeByteArray(data)
  }

  suspend fun handleIncoming(frame: IncomingFrame) {
    when (val request = frame.body) {
      null -> {}
      is ErrorResponse -> {
        println("Error!! id=${frame.id} [${request.error}] ${request.detail}") // TODO: Handle error.
        exitProcess(1)
      }
      is DagFileParseRequest -> {
        val body = DagParser(request.file, request.bundlePath).parse(bundle)
        sendMessage(frame.id, body)
        shutDownRequested = true
      }
      is StartupDetails -> {
        sendMessage(frame.id, TaskRunner.run(bundle, request, this))
        shutDownRequested = true
      }
    }
  }

  @Throws(ApiError::class)
  suspend fun communicateImpl(body: Any): Any {
    var frame: IncomingFrame? = null

    suspend fun handle(f: IncomingFrame) {
      frame = f
    }
    sendMessage(nextId.fetchAndAdd(1), body)
    processOnce(::handle)
    if (frame == null) {
      throw ApiError("No response received")
    }
    return frame.body ?: Unit
  }

  @Throws(ApiError::class)
  suspend inline fun <reified T> communicate(request: Any): T {
    when (val response = communicateImpl(request)) {
      is ErrorResponse -> throw ApiError("[${response.error}] ${response.detail}")
      is T -> return response
      else -> throw ApiError("Unexpected response type ${response::class.java}")
    }
  }
}

internal inline fun <reified S, R> ApiClient.communicate(block: S.() -> Call<R>): R {
  val service = createService(S::class.java)
  val response = block(service).execute()
  if (!response.isSuccessful) {
    throw ApiError("[${response.message()}] $response (from $service")
  }
  return response.body() ?: throw ApiError("No body")
}
