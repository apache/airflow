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
Example Airflow DAG for DataprocSubmitJobOperator with hadoop job.
"""

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc_hadoop"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CLUSTER_NAME = f"dataproc-hadoop-{ENV_ID}"
REGION = "europe-west1"
ZONE = "europe-west1-b"

OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET_NAME}/{OUTPUT_FOLDER}/"

# Cluster definition
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

# Update options
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions
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


with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        region=REGION,
    )

    hadoop_task = DataprocSubmitJobOperator(
        task_id="hadoop_task", job=HADOOP_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    create_bucket >> create_cluster >> scale_cluster >> hadoop_task >> delete_cluster >> delete_bucket

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
