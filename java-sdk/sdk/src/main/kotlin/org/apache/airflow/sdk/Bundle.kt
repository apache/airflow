package org.apache.airflow.sdk

class Bundle(
  val version: String,
  dags: Iterable<Dag>,
) {
  val dags: Map<String, Dag> = dags.associateByDagId()
}

private fun Iterable<Dag>.associateByDagId(): Map<String, Dag> {
  val dagMap = linkedMapOf<String, Dag>()
  for (dag in this) {
    require(dagMap.putIfAbsent(dag.dagId, dag) == null) {
      "Duplicate dagId in bundle: ${dag.dagId}"
    }
  }
  return dagMap
}
