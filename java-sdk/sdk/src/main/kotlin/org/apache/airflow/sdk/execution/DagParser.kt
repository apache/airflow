package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Dag

data class DagParsingResult(
  val fileloc: String,
  val bundlePath: String,
  val dags: Map<String, Dag>,
)

class DagParser(
  val file: String,
  val bundlePath: String,
) {
  fun parse(bundle: Bundle): DagParsingResult = DagParsingResult(file, bundlePath, bundle.dags)
}
