package org.apache.airflow.sdk

/**
 * Interface for declaring DAGs in a bundle.
 *
 * <p>Implement this interface in the class specified as {@code Main-Class} in your JAR manifest.
 * The build system instantiates this class at compile time to extract dag_ids and task_ids
 * into the JAR manifest, enabling inspection of bundled DAGs without running the full process.
 */
interface DagBundle {
  fun getDags(): List<Dag>
}
