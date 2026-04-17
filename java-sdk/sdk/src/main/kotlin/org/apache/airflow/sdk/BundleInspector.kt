package org.apache.airflow.sdk

import java.io.File

/**
 * Build-time utility that inspects a [DagBundle] implementation and writes
 * dag_ids and task_ids to a YAML metadata file for inclusion in the JAR.
 *
 * Usage: {@code java -cp <classpath> org.apache.airflow.sdk.BundleInspector <bundleClass> <outputFile>}
 */
object BundleInspector {
  @JvmStatic
  fun main(args: Array<String>) {
    require(args.size == 2) { "Usage: BundleInspector <bundleClassName> <outputYamlFile>" }
    val className = args[0]
    val outputPath = args[1]

    val clazz = Class.forName(className)
    val instance =
      clazz.getDeclaredConstructor().newInstance() as? DagBundle
        ?: error("$className does not implement ${DagBundle::class.qualifiedName}")
    val dags = instance.getDags()

    val outputFile = File(outputPath)
    outputFile.parentFile.mkdirs()
    outputFile.writeText(toYaml(dags))
  }

  internal fun toYaml(dags: List<Dag>): String =
    buildString {
      appendLine("dags:")
      for (dag in dags) {
        appendLine("  ${dag.dagId}:")
        appendLine("    tasks:")
        for (taskId in dag.tasks.keys) {
          appendLine("      - $taskId")
        }
      }
    }
}
