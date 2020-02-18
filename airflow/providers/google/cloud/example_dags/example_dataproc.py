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
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-project")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west-1b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = "gs://{}/{}/".format(BUCKET, OUTPUT_FOLDER)
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)


# Cluster definition
CLUSTER = {
    "project_id": PROJECT_ID,
    "cluster_name": CLUSTER_NAME,
    "config": {
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
    },
}


# Update options
CLUSTER_UPDATE = {
    "config": {
        "worker_config": {"num_instances": 3},
        "secondary_worker_config": {"num_instances": 3},
    }
}
UPDATE_MASK = {
    "paths": [
        "config.worker_config.num_instances",
        "config.secondary_worker_config.num_instances",
    ]
}

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}


# Jobs definitions
PIG_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
}

SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}

HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": ["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
    },
}

with models.DAG(
    "example_gcp_dataproc",
    default_args={"start_date": days_ago(1)},
    schedule_interval=None,
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster", project_id=PROJECT_ID, cluster=CLUSTER, region=REGION
    )

    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        location=REGION,
    )

    pig_task = DataprocSubmitJobOperator(
        task_id="pig_task", job=PIG_JOB, location=REGION, project_id=PROJECT_ID
    )

    spark_sql_task = DataprocSubmitJobOperator(
        task_id="spark_sql_task",
        job=SPARK_SQL_JOB,
        location=REGION,
        project_id=PROJECT_ID,
    )

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID
    )

    hive_task = DataprocSubmitJobOperator(
        task_id="hive_task", job=HIVE_JOB, location=REGION, project_id=PROJECT_ID
    )

    hadoop_task = DataprocSubmitJobOperator(
        task_id="hadoop_task", job=HADOOP_JOB, location=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
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
