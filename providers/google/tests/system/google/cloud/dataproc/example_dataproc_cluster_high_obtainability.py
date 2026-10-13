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
(``FlexibleInstanceGroupConfig``: number of instances, core count and hardware
class) instead of concrete machine types. Dataproc resolves them into a ranked
list of widely available machine families to minimize stockouts. HO clusters
are always auto-zoned.

The DAG creates an HO cluster, scales its primary workers through the
authoritative ``config.worker_flexible_config.num_instances`` update path,
scales the secondary workers through the legacy
``config.secondary_worker_config.num_instances`` compatibility path (which the
API keeps accepting for HO clusters), submits a Spark job and deletes the
cluster.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
    FlexibleInstanceGroupConfig,
    flexible_scale_cluster_update,
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
HO_CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    master_flexible_config=FlexibleInstanceGroupConfig(
        num_instances=1, core_count=4, hardware_class="STANDARD"
    ),
    # A plain dict with the same keys is accepted as well.
    worker_flexible_config={
        "num_instances": 2,
        "core_count": 8,
        "hardware_class": "PERFORMANCE",
        "boot_disk_size_gb": 500,
    },
    secondary_worker_flexible_config={
        "num_instances": 1,
        "core_count": 8,
        "hardware_class": "PERFORMANCE",
    },
    image_version="2.2",
).make()
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
    schedule=None,
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

    # [START how_to_cloud_dataproc_scale_ho_cluster_flexible_path]
    # Authoritative HO scaling path: config.worker_flexible_config.num_instances.
    scale_workers_cluster, scale_workers_mask = flexible_scale_cluster_update(num_workers=4)
    scale_up_workers = DataprocUpdateClusterOperator(
        task_id="scale_up_ho_workers",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster=scale_workers_cluster,
        update_mask=scale_workers_mask,
        graceful_decommission_timeout={"seconds": 600},
    )
    # [END how_to_cloud_dataproc_scale_ho_cluster_flexible_path]

    # [START how_to_cloud_dataproc_scale_ho_cluster_legacy_path]
    # Legacy compatibility path: the API maps
    # config.secondary_worker_config.num_instances onto the same resize for HO
    # clusters, so DAGs written for standard clusters keep working. Never combine
    # the legacy and the flexible path for the same node group in one request.
    scale_down_secondary_workers = DataprocUpdateClusterOperator(
        task_id="scale_down_ho_secondary_workers",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster={"config": {"secondary_worker_config": {"num_instances": 0}}},
        update_mask={"paths": ["config.secondary_worker_config.num_instances"]},
        graceful_decommission_timeout={"seconds": 600},
    )
    # [END how_to_cloud_dataproc_scale_ho_cluster_legacy_path]

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

    (
        create_cluster
        >> scale_up_workers
        >> spark_pi
        >> scale_down_secondary_workers
        >> delete_cluster
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
