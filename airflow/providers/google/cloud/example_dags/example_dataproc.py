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

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-project")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = f"gs://{BUCKET}/{PYSPARK_MAIN}"
SPARKR_MAIN = os.environ.get("SPARKR_MAIN", "hello_world.R")
SPARKR_URI = f"gs://{BUCKET}/{SPARKR_MAIN}"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions
# [START how_to_cloud_dataproc_pig_config]
PIG_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
}
# [END how_to_cloud_dataproc_pig_config]

# [START how_to_cloud_dataproc_sparksql_config]
SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_sparksql_config]

# [START how_to_cloud_dataproc_spark_config]
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
# [END how_to_cloud_dataproc_spark_config]

# [START how_to_cloud_dataproc_pyspark_config]
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}
# [END how_to_cloud_dataproc_pyspark_config]

# [START how_to_cloud_dataproc_sparkr_config]
SPARKR_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_r_job": {"main_r_file_uri": SPARKR_URI},
}
# [END how_to_cloud_dataproc_sparkr_config]

# [START how_to_cloud_dataproc_hive_config]
HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_hive_config]

# [START how_to_cloud_dataproc_hadoop_config]
HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": ["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
    },
}
# [END how_to_cloud_dataproc_hadoop_config]

with models.DAG("example_gcp_dataproc", start_date=days_ago(1), schedule_interval=None) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    # [START how_to_cloud_dataproc_update_cluster_operator]
    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END how_to_cloud_dataproc_update_cluster_operator]

    pig_task = DataprocSubmitJobOperator(
        task_id="pig_task", job=PIG_JOB, location=REGION, project_id=PROJECT_ID
    )
    spark_sql_task = DataprocSubmitJobOperator(
        task_id="spark_sql_task", job=SPARK_SQL_JOB, location=REGION, project_id=PROJECT_ID
    )

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID
    )

    # [START cloud_dataproc_async_submit_sensor]
    spark_task_async = DataprocSubmitJobOperator(
        task_id="spark_task_async", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID, asynchronous=True
    )

    spark_task_async_sensor = DataprocJobSensor(
        task_id='spark_task_async_sensor_task',
        location=REGION,
        project_id=PROJECT_ID,
        dataproc_job_id="{{task_instance.xcom_pull(task_ids='spark_task_async')}}",
        poke_interval=10,
    )
    # [END cloud_dataproc_async_submit_sensor]

    # [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID
    )
    # [END how_to_cloud_dataproc_submit_job_to_cluster_operator]

    sparkr_task = DataprocSubmitJobOperator(
        task_id="sparkr_task", job=SPARKR_JOB, location=REGION, project_id=PROJECT_ID
    )

    hive_task = DataprocSubmitJobOperator(
        task_id="hive_task", job=HIVE_JOB, location=REGION, project_id=PROJECT_ID
    )

    hadoop_task = DataprocSubmitJobOperator(
        task_id="hadoop_task", job=HADOOP_JOB, location=REGION, project_id=PROJECT_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]

    create_cluster >> scale_cluster
    scale_cluster >> hive_task >> delete_cluster
    scale_cluster >> pig_task >> delete_cluster
    scale_cluster >> spark_sql_task >> delete_cluster
    scale_cluster >> spark_task >> delete_cluster
    scale_cluster >> spark_task_async >> spark_task_async_sensor >> delete_cluster
    scale_cluster >> pyspark_task >> delete_cluster
    scale_cluster >> sparkr_task >> delete_cluster
    scale_cluster >> hadoop_task >> delete_cluster
