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
Example Airflow DAG for Google Vertex AI service testing Ray operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from google.cloud.aiplatform.vertex_ray.util import resources
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "The ray provider is optional and requires the `google-cloud-aiplatform` package to be installed. "
    )

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.ray import (
    CreateRayClusterOperator,
    DeleteRayClusterOperator,
    GetRayClusterOperator,
    ListRayClustersOperator,
    UpdateRayClusterOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_ray_operations"
LOCATION = "us-central1"
WORKER_NODE_RESOURCES = resources.Resources(
    node_count=1,
)
WORKER_NODE_RESOURCES_NEW = resources.Resources(
    node_count=2,
)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "ray"],
) as dag:
    # [START how_to_cloud_vertex_ai_create_ray_cluster_operator]
    create_ray_cluster = CreateRayClusterOperator(
        task_id="create_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        worker_node_types=[WORKER_NODE_RESOURCES],
        python_version="3.10",
        ray_version="2.33",
    )
    # [END how_to_cloud_vertex_ai_create_ray_cluster_operator]

    # [START how_to_cloud_vertex_ai_update_ray_cluster_operator]
    update_ray_cluster = UpdateRayClusterOperator(
        task_id="update_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id="{{ task_instance.xcom_pull(task_ids='create_ray_cluster', key='cluster_id') }}",
        worker_node_types=[WORKER_NODE_RESOURCES_NEW],
    )
    # [END how_to_cloud_vertex_ai_update_ray_cluster_operator]

    # [START how_to_cloud_vertex_ai_get_ray_cluster_operator]
    get_ray_cluster = GetRayClusterOperator(
        task_id="get_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id="{{ task_instance.xcom_pull(task_ids='create_ray_cluster', key='cluster_id') }}",
    )
    # [END how_to_cloud_vertex_ai_get_ray_cluster_operator]

    # [START how_to_cloud_vertex_ai_delete_ray_cluster_operator]
    delete_ray_cluster = DeleteRayClusterOperator(
        task_id="delete_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id="{{ task_instance.xcom_pull(task_ids='create_ray_cluster', key='cluster_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_ray_cluster_operator]

    # [START how_to_cloud_vertex_ai_list_ray_clusters_operator]
    list_ray_clusters = ListRayClustersOperator(
        task_id="list_ray_clusters",
        project_id=PROJECT_ID,
        location=LOCATION,
    )
    # [END how_to_cloud_vertex_ai_list_ray_clusters_operator]

    (
        [
            create_ray_cluster >> update_ray_cluster >> get_ray_cluster >> delete_ray_cluster,
            list_ray_clusters,
        ]
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
