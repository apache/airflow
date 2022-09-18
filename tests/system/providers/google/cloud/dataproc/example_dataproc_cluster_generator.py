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
Example Airflow DAG testing Dataproc
operators for managing a cluster and submitting jobs.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc_cluster_generation"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CLUSTER_NAME = f"dataproc-cluster-gen-{ENV_ID}"
REGION = "europe-west1"
ZONE = "europe-west1-b"
INIT_FILE_SRC = str(Path(__file__).parent / "resources" / "pip-install.sh")

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator
# [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]

INIT_FILE = "pip-install.sh"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[f"gs://{BUCKET_NAME}/{INIT_FILE}"],
    metadata={'PIP_PACKAGES': 'pyyaml requests pandas openpyxl'},
).make()

# [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=INIT_FILE_SRC,
        dst=INIT_FILE,
        bucket=BUCKET_NAME,
    )

    # [START how_to_cloud_dataproc_create_cluster_generate_cluster_config_operator]

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
    )

    # [END how_to_cloud_dataproc_create_cluster_generate_cluster_config_operator]

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

    create_bucket >> upload_file >> create_dataproc_cluster >> [delete_cluster, delete_bucket]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
