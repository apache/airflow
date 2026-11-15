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
"""Example Airflow DAG for a Dataproc High Obtainability (HO) mode cluster.

In High Obtainability mode node groups are described by abstract requirements
(number of instances, core count, and hardware class) instead of concrete machine
types. Dataproc resolves them into a ranked list of widely available machine
families to minimize stockouts. HO clusters are always auto-zoned.

The DAG creates an HO cluster, scales its worker and secondary worker groups
via ``DataprocUpdateClusterOperator``, submits a Spark job, and deletes the
cluster.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_cluster_high_obtainability"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL
REGION = "us-central1"

# [START how_to_cloud_dataproc_create_ho_cluster_config]
# Note: no machine types, disk types or zone are specified. HO clusters are
# cluster-wide: master and primary workers must both use a flexible config.
HO_CLUSTER_CONFIG = {
    "master_flexible_config": {
        "num_instances": 1,
        "core_count": 2,
        "hardware_class": "STANDARD",
        "boot_disk_size_gb": 32,
    },
    "worker_flexible_config": {
        "num_instances": 2,
        "core_count": 2,
        "hardware_class": "STANDARD",
        "boot_disk_size_gb": 32,
    },
    "secondary_worker_flexible_config": {
        "num_instances": 1,
        "core_count": 2,
        "hardware_class": "STANDARD",
        "boot_disk_size_gb": 32,
    },
    "software_config": {
        "image_version": "2.2",
    },
}
# [END how_to_cloud_dataproc_create_ho_cluster_config]

SPARK_PI_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "main_class": "org.apache.spark.examples.SparkPi",
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "args": ["10"],
    },
}

with DAG(
    DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataproc", "high-obtainability"],
) as dag:
    # [START how_to_cloud_dataproc_create_ho_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ho_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=HO_CLUSTER_CONFIG,
    )
    # [END how_to_cloud_dataproc_create_ho_cluster_operator]

    # [START how_to_cloud_dataproc_scale_ho_cluster]
    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_ho_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster={
            "config": {
                "worker_flexible_config": {"num_instances": 3},
                "secondary_worker_flexible_config": {"num_instances": 0},
            }
        },
        update_mask={
            "paths": [
                "config.worker_flexible_config.num_instances",
                "config.secondary_worker_flexible_config.num_instances",
            ]
        },
        graceful_decommission_timeout={"seconds": 600},
    )
    # [END how_to_cloud_dataproc_scale_ho_cluster]

    spark_pi = DataprocSubmitJobOperator(
        task_id="spark_pi",
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_PI_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ho_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> scale_cluster >> spark_pi >> delete_cluster

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
