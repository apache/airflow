package org.apache.airflow.sdk

import kotlin.Throws

interface Task {
  @Throws(Exception::class)
  fun execute(client: Client)
}
