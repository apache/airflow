package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client

object TaskRunner {
  fun run(
    bundle: Bundle,
    request: StartupDetails,
    comm: CoordinatorComm,
  ): Any = run(bundle, request, Client(request, CoordinatorClient(comm)))

  internal fun run(
    bundle: Bundle,
    request: StartupDetails,
    client: Client,
  ): Any {
    val task = bundle.dags[request.ti.dagId]?.tasks[request.ti.taskId] ?: return TaskState("removed")
    val instance = task.getDeclaredConstructor().newInstance()
    return try {
      instance.execute(client)
      SucceedTask()
    } catch (e: Exception) {
      e.printStackTrace()
      TaskState("failed")
    }
  }
}
