# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""

import os
import airflow
from airflow import models
from airflow.gcp.operators.dataproc import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataprocClusterScaleOperator,
    DataProcSparkSqlOperator,
    DataProcSparkOperator,
    DataProcPySparkOperator,
    DataProcPigOperator,
    DataProcHiveOperator,
    DataProcHadoopOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-project")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west-1b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = "gs://{}/{}/".format(BUCKET, OUTPUT_FOLDER)
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)

with models.DAG(
    "example_gcp_dataproc",
    default_args={"start_date": airflow.utils.dates.days_ago(1)},
    schedule_interval=None,
) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        num_workers=2,
        region=REGION,
    )

    scale_cluster = DataprocClusterScaleOperator(
        task_id="scale_cluster",
        num_workers=3,
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
    )

    pig_task = DataProcPigOperator(
        task_id="pig_task",
        query="define sin HiveUDF('sin');",
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    spark_sql_task = DataProcSparkSqlOperator(
        task_id="spark_sql_task",
        query="SHOW DATABASES;",
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    spark_task = DataProcSparkOperator(
        task_id="spark_task",
        main_class="org.apache.spark.examples.SparkPi",
        dataproc_jars="file:///usr/lib/spark/examples/jars/spark-examples.jar",
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataProcPySparkOperator(
        task_id="pyspark_task",
        main=PYSPARK_URI,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    hive_task = DataProcHiveOperator(
        task_id="hive_task",
        query="SHOW DATABASES;",
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    hadoop_task = DataProcHadoopOperator(
        task_id="hadoop_task",
        main_jar="file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        arguments=["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    create_cluster >> scale_cluster
    scale_cluster >> hive_task >> delete_cluster
    scale_cluster >> pig_task >> delete_cluster
    scale_cluster >> spark_sql_task >> delete_cluster
    scale_cluster >> spark_task >> delete_cluster
    scale_cluster >> pyspark_task >> delete_cluster
    scale_cluster >> hadoop_task >> delete_cluster
