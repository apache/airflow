package org.apache.airflow.sdk.execution

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.airflow.sdk.ensureTrailingSlash
import org.apache.airflow.sdk.execution.api.client.ApiClient
import org.apache.airflow.sdk.execution.api.model.TIEnterRunningPayload
import org.apache.airflow.sdk.execution.api.model.TIRunContext
import org.apache.airflow.sdk.execution.api.model.TISuccessStatePayload
import org.apache.airflow.sdk.execution.api.model.TITerminalStatePayload
import org.apache.airflow.sdk.execution.api.model.TerminalStateNonSuccess
import org.apache.airflow.sdk.execution.api.route.TaskInstancesApi
import org.apache.airflow.sdk.execution.api.route.XComsApi
import retrofit2.Call
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.UUID
import org.apache.airflow.sdk.execution.api.model.BundleInfo as ExecutionBundleInfo
import org.apache.airflow.sdk.execution.api.model.TaskInstance as ExecutionTaskInstance
import org.apache.airflow.sdk.execution.api.model.TaskInstanceState as ExecutionTaskInstanceState

data class SupervisorTaskInstance(
  val id: UUID,
  val taskId: String,
  val dagId: String,
  val runId: String,
  val tryNumber: Int,
  val dagVersionId: UUID,
  val mapIndex: Int?,
  val contextCarrier: Map<String, Any?>? = null,
)

data class SupervisorBundleInfo(
  val name: String,
  val version: String?,
)

data class SupervisorRequest(
  val mainClass: String,
  val classpath: String,
  val executionApiBaseUrl: String,
  val token: String,
  val workerName: String,
  val userName: String,
  val dagRelPath: String,
  val bundleInfo: SupervisorBundleInfo,
  val taskInstance: SupervisorTaskInstance,
  val sentryIntegration: String = "",
  val onLogLine: suspend (String) -> Unit = {},
)

data class SupervisorResult(
  val finalState: ExecutionTaskInstanceState,
  val exitCode: Int,
)

/**
 * Retrofit interface for reporting task instance terminal state to the Execution API.
 *
 * Mirrors the Python SDK's `TaskInstanceOperations.succeed()` and `.finish()` methods
 * (see `airflow/sdk/api/client.py`), both of which call `PATCH /task-instances/{id}/state`
 * with [TISuccessStatePayload] or [TITerminalStatePayload] respectively.
 *
 * Why not use the generated [TaskInstancesApi.tiUpdateState]?
 * The OpenAPI code generator flattens the endpoint's `oneOf` discriminated union into a single
 * class [org.apache.airflow.sdk.execution.api.model.TiPatchPayload] whose `StateEnum` only
 * contains `UP_FOR_RETRY`. It cannot represent `"success"`, `"failed"`, or `"skipped"`, and its
 * method signature does not accept [TISuccessStatePayload] or [TITerminalStatePayload].
 * This interface works around that limitation by binding the same endpoint with the correct
 * payload types that the generator *did* produce correctly as standalone classes.
 */
private interface TaskInstanceStateApi {
  @retrofit2.http.Headers("Content-Type:application/json")
  @retrofit2.http.PATCH("task-instances/{task_instance_id}/state")
  fun succeed(
    @retrofit2.http.Path("task_instance_id") id: UUID,
    @retrofit2.http.Body payload: TISuccessStatePayload,
    @retrofit2.http.Header("Airflow-API-Version") version: LocalDate?,
  ): Call<Void>

  @retrofit2.http.Headers("Content-Type:application/json")
  @retrofit2.http.PATCH("task-instances/{task_instance_id}/state")
  fun finish(
    @retrofit2.http.Path("task_instance_id") id: UUID,
    @retrofit2.http.Body payload: TITerminalStatePayload,
    @retrofit2.http.Header("Airflow-API-Version") version: LocalDate?,
  ): Call<Void>
}

object Supervisor {
  private const val CONNECT_TIMEOUT_MS = 15_000
  private val loopback: InetAddress = InetAddress.getByName("127.0.0.1")

  suspend fun run(request: SupervisorRequest): SupervisorResult {
    val execApi = executionApiClient(request.executionApiBaseUrl, request.token)
    val execClient = HttpExecApiClient(execApi)
    val startDate = OffsetDateTime.now()

    return withContext(Dispatchers.IO) {
      coroutineScope {
        ServerSocket(0, 1, loopback).use { commServer ->
          ServerSocket(0, 1, loopback).use { logsServer ->
            commServer.soTimeout = CONNECT_TIMEOUT_MS
            logsServer.soTimeout = CONNECT_TIMEOUT_MS

            val process = startBundleProcess(request.classpath, request.mainClass, commServer.localPort, logsServer.localPort)
            val stdoutPump =
              launch(Dispatchers.IO) {
                streamLines(process.inputStream, request.onLogLine)
              }
            val stderrPump =
              launch(Dispatchers.IO) {
                streamLines(process.errorStream, request.onLogLine)
              }
            try {
              val (commSocket, logsSocket) = acceptConnections(commServer, logsServer)

              commSocket.use { comm ->
                logsSocket.use { logs ->
                  val logPump =
                    launch(Dispatchers.IO) {
                      streamLines(logs.getInputStream(), request.onLogLine)
                    }

                  val taskInstance = request.taskInstance.toExecutionTaskInstance(request.workerName)
                  val tiContext = startTask(execApi, taskInstance, startDate, process, request.workerName, request.userName)

                  TaskSdkFrames.writeRequest(
                    comm.getOutputStream(),
                    0,
                    request.toStartupDetails(taskInstance, tiContext, startDate),
                  )

                  val finalState = serveTaskSdkRequests(comm, execApi, execClient, taskInstance.id)
                  val exitCode = process.waitFor()
                  logPump.join()
                  stdoutPump.join()
                  stderrPump.join()

                  SupervisorResult(
                    finalState = if (exitCode == 0) finalState else ExecutionTaskInstanceState.FAILED,
                    exitCode = exitCode,
                  )
                }
              }
            } catch (e: Exception) {
              process.destroy()
              throw e
            }
          }
        }
      }
    }
  }

  internal suspend fun streamLines(
    input: InputStream,
    onLogLine: suspend (String) -> Unit,
  ) {
    withContext(Dispatchers.IO) {
      input.bufferedReader().useLines { lines ->
        for (line in lines) {
          onLogLine(line)
        }
      }
    }
  }

  private fun serveTaskSdkRequests(
    comm: Socket,
    execApi: ApiClient,
    execClient: HttpExecApiClient,
    taskInstanceId: UUID,
  ): ExecutionTaskInstanceState {
    val input = comm.getInputStream()
    val output = comm.getOutputStream()

    while (true) {
      val frame = TaskSdkFrames.readFrame(input, TaskSdkFrames.toSupervisorTypes)
      when (val message = frame.body ?: return ExecutionTaskInstanceState.FAILED) {
        is GetConnection ->
          reply(frame.id, output) {
            execClient.getConnection(message.id)
          }
        is GetVariable ->
          reply(frame.id, output) {
            execClient.getVariable(message.key)
          }
        is GetXCom ->
          reply(frame.id, output) {
            execClient.getXCom(
              key = message.key,
              dagId = message.dagId,
              taskId = message.taskId,
              runId = message.runId,
              mapIndex = message.mapIndex,
              includePriorDates = message.includePriorDates,
            )
          }
        is SetXCom ->
          reply(frame.id, output) {
            setXCom(execApi, message)
            null
          }
        is SucceedTask -> {
          succeed(execApi, taskInstanceId, message)
          return ExecutionTaskInstanceState.SUCCESS
        }
        is TaskState -> {
          finish(execApi, taskInstanceId, message)
          return ExecutionTaskInstanceState.fromValue(message.state)
        }
        is ErrorResponse -> throw IllegalStateException("[${message.error}] ${message.detail}")
        else -> throw IllegalStateException("Unsupported Task SDK message type ${message::class.java.name}")
      }
    }
  }

  private fun succeed(
    execApi: ApiClient,
    taskInstanceId: UUID,
    message: SucceedTask,
  ) {
    execApi.send<TaskInstanceStateApi> {
      succeed(
        taskInstanceId,
        TISuccessStatePayload()
          .endDate(message.endDate)
          .taskOutlets(message.taskOutlets)
          .outletEvents(message.outletEvents),
        HttpExecApiClient.version,
      )
    }
  }

  private fun finish(
    execApi: ApiClient,
    taskInstanceId: UUID,
    message: TaskState,
  ) {
    execApi.send<TaskInstanceStateApi> {
      finish(
        taskInstanceId,
        TITerminalStatePayload()
          .state(TerminalStateNonSuccess.fromValue(message.state))
          .endDate(message.endDate),
        HttpExecApiClient.version,
      )
    }
  }

  private fun reply(
    requestId: Int,
    output: OutputStream,
    block: () -> Any?,
  ) {
    try {
      TaskSdkFrames.writeResponse(output, requestId, body = block())
    } catch (e: Exception) {
      TaskSdkFrames.writeResponse(
        output,
        requestId,
        error =
          ErrorResponse().also {
            it.error = "generic_error"
            it.detail = mapOf("message" to (e.message ?: e::class.java.name))
          },
      )
    }
  }

  private suspend fun acceptConnections(
    commServer: ServerSocket,
    logsServer: ServerSocket,
  ): Pair<Socket, Socket> =
    coroutineScope {
      val comm = async(Dispatchers.IO) { commServer.accept() }
      val logs = async(Dispatchers.IO) { logsServer.accept() }
      comm.await() to logs.await()
    }

  private fun startBundleProcess(
    classpath: String,
    mainClass: String,
    commPort: Int,
    logsPort: Int,
  ): Process {
    val command =
      listOf(
        "java",
        "-classpath",
        classpath,
        mainClass,
        "--comm=${loopback.hostAddress}:$commPort",
        "--logs=${loopback.hostAddress}:$logsPort",
      )
    return ProcessBuilder(command)
      .redirectOutput(ProcessBuilder.Redirect.PIPE)
      .redirectError(ProcessBuilder.Redirect.PIPE)
      .start()
  }

  private fun executionApiClient(
    baseUrl: String,
    token: String,
  ) = ApiClient("JWTBearer").apply {
    setBearerToken(token)
    adapterBuilder.baseUrl(baseUrl.ensureTrailingSlash())
  }

  private fun setXCom(
    execApi: ApiClient,
    request: SetXCom,
  ) {
    execApi.send<XComsApi> {
      setXcom(
        request.dagId,
        request.runId,
        request.taskId,
        request.key,
        request.mapIndex,
        null,
        HttpExecApiClient.version,
        request.value,
      )
    }
  }

  private fun startTask(
    api: ApiClient,
    taskInstance: ExecutionTaskInstance,
    startDate: OffsetDateTime,
    process: Process,
    workerName: String,
    userName: String,
  ): TIRunContext =
    api.communicate<TaskInstancesApi, TIRunContext> {
      tiRun(
        taskInstance.id,
        TIEnterRunningPayload()
          .hostname(workerName)
          .unixname(userName)
          .pid(process.pid().toInt())
          .startDate(startDate),
        HttpExecApiClient.version,
      )
    }

  private fun SupervisorTaskInstance.toExecutionTaskInstance(workerName: String) =
    ExecutionTaskInstance().also {
      it.id = id
      it.taskId = taskId
      it.dagId = dagId
      it.runId = runId
      it.tryNumber = tryNumber
      it.dagVersionId = dagVersionId
      it.mapIndex = mapIndex
      it.hostname = workerName
      it.contextCarrier = contextCarrier
    }

  private fun SupervisorRequest.toStartupDetails(
    taskInstance: ExecutionTaskInstance,
    tiContext: TIRunContext,
    startDate: OffsetDateTime,
  ) = StartupDetails().also {
    it.ti = taskInstance
    it.dagRelPath = dagRelPath
    it.bundleInfo =
      ExecutionBundleInfo().also { info ->
        info.name = bundleInfo.name
        info.version = bundleInfo.version
      }
    it.tiContext = tiContext
    it.startDate = startDate
    it.sentryIntegration = sentryIntegration
  }
}

private inline fun <reified Q> ApiClient.send(block: Q.() -> Call<*>) {
  val service = createService(Q::class.java)
  val response = block(service).execute()
  if (!response.isSuccessful) {
    throw IllegalStateException("[${response.message()}] $response (from $service)")
  }
}
