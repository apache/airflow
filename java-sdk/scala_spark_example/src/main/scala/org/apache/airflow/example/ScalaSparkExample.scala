/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.example

import org.apache.airflow.sdk.{Bundle, BundleBuilder, Client, Context, Dag, Server, Task}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

/**
 * A Scala + Apache Spark ETL bundle for the Java SDK. Each task runs in its own
 * JVM with a fresh local `SparkSession` and passes scalar results over XCom.
 * See README.md for the overview.
 */

/** Deterministic in-memory sales dataset so downstream assertions are stable. */
private object SalesData {
  // (id, category, amount)
  val rows: Seq[(Int, String, Long)] = Seq(
    (1, "electronics", 100L),
    (2, "books", 200L),
    (3, "electronics", 300L),
    (4, "clothing", 150L),
    (5, "books", 250L),
  )
}

private object SparkEtl {
  val DagId = "scala_spark_example"
  val ExtractTaskId = "spark_extract"
  val TransformTaskId = "spark_transform"
  val LoadTaskId = "spark_load"

  // Loopback driver + no UI so Spark binds no ports and skips hostname lookup.
  def newSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  // 3-arg getXCom(key, dagId, taskId) reads the upstream return value; wire
  // integers arrive boxed.
  def readUpstreamLong(client: Client, context: Context, taskId: String): Long =
    client.getXCom("return_value", context.dagRun.dagId, taskId).asInstanceOf[Number].longValue()

  def pushLong(client: Client, value: Long): Unit =
    client.setXCom("return_value", java.lang.Long.valueOf(value))
}

/** Builds the Spark DataFrame and reports how many records were extracted. */
class SparkExtract extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkExtract])

  override def execute(context: Context, client: Client): Unit = {
    log.info("Starting Scala Spark extract task")
    val spark = SparkEtl.newSession("scala-spark-etl-extract")
    try {
      import spark.implicits._
      val raw = SalesData.rows.toDF("id", "category", "amount")
      val count = raw.count()
      log.info("Extracted {} sales records with Spark", java.lang.Long.valueOf(count))
      SparkEtl.pushLong(client, count)
    } finally {
      spark.stop()
    }
  }
}

/** Aggregates total revenue across the extracted records with Spark. */
class SparkTransform extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkTransform])

  override def execute(context: Context, client: Client): Unit = {
    // Read the upstream count only to demonstrate XCom passing between JVM
    // tasks; the aggregation below recomputes from the source dataset.
    val extractedCount = SparkEtl.readUpstreamLong(client, context, SparkEtl.ExtractTaskId)
    log.info("Transform received {} records from extract", java.lang.Long.valueOf(extractedCount))

    val spark = SparkEtl.newSession("scala-spark-etl-transform")
    try {
      import spark.implicits._
      val raw = SalesData.rows.toDF("id", "category", "amount")
      val total = raw.agg(sum($"amount")).first().getLong(0)
      log.info("Computed total revenue {} with Spark", java.lang.Long.valueOf(total))
      SparkEtl.pushLong(client, total)
    } finally {
      spark.stop()
    }
  }
}

/** "Loads" the aggregated revenue, returning the persisted value. */
class SparkLoad extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkLoad])

  override def execute(context: Context, client: Client): Unit = {
    val total = SparkEtl.readUpstreamLong(client, context, SparkEtl.TransformTaskId)
    log.info("Loading aggregated revenue {}", java.lang.Long.valueOf(total))

    val spark = SparkEtl.newSession("scala-spark-etl-load")
    try {
      import spark.implicits._
      val loaded = Seq(("total_revenue", total)).toDF("metric", "value")
      val value = loaded.first().getLong(1)
      log.info("Load complete; persisted total_revenue={}", java.lang.Long.valueOf(value))
      SparkEtl.pushLong(client, value)
    } finally {
      spark.stop()
    }
  }
}

object ScalaSparkExample {
  def build(): Dag =
    new Dag(SparkEtl.DagId)
      .addTask(SparkEtl.ExtractTaskId, classOf[SparkExtract])
      .addTask(SparkEtl.TransformTaskId, classOf[SparkTransform])
      .addTask(SparkEtl.LoadTaskId, classOf[SparkLoad])
}

/** Bundle entry point served to Airflow's Java coordinator. */
object ScalaSparkBundleBuilder extends BundleBuilder {
  override def getDags(): java.lang.Iterable[Dag] = java.util.List.of(ScalaSparkExample.build())

  def main(args: Array[String]): Unit =
    Server.create(args).serve(new Bundle(getDags()))
}
