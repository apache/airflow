package org.apache.airflow.sdk

import org.apache.airflow.sdk.execution.Client
import org.apache.airflow.sdk.execution.StartupDetails

class Client(
  val details: StartupDetails,
  val impl: Client,
) {
  companion object {
    const val XCOM_RETURN_KEY = "return_value"
  }

  fun getConnection(id: String): Connection =
    with(impl.getConnection(id)) {
      Connection(
        id = connId,
        type = connType,
        host = host,
        schema = schema,
        login = login,
        password = password,
        port = port,
        extra = extra,
      )
    }

  fun getVariable(key: String): Any? = impl.getVariable(key).value

  @JvmOverloads fun getXCom(
    key: String = XCOM_RETURN_KEY,
    dagId: String = details.ti.dagId,
    taskId: String,
    runId: String = details.ti.runId,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): Any? =
    impl
      .getXCom(
        key = key,
        dagId = dagId,
        taskId = taskId,
        runId = runId,
        mapIndex = mapIndex,
        includePriorDates = includePriorDates,
      ).value

  @JvmOverloads fun setXCom(
    key: String = XCOM_RETURN_KEY,
    value: Any,
  ) = impl.setXCom(
    key = key,
    value = value,
    dagId = details.ti.dagId,
    taskId = details.ti.taskId,
    runId = details.ti.runId,
    mapIndex = details.ti.mapIndex ?: -1,
  )
}
