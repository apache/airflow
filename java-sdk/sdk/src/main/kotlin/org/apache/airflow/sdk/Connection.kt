package org.apache.airflow.sdk

data class Connection(
  val id: String,
  val type: String?,
  val host: String?,
  val schema: String?,
  val login: String?,
  val password: String?,
  val port: Int?,
  val extra: String?,
)
