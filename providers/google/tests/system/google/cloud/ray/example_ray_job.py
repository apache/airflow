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
Example Airflow DAG for Jobs on Ray operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.ray import (
    RayDeleteJobOperator,
    RayGetJobInfoOperator,
    RayListJobsOperator,
    RayStopJobOperator,
    RaySubmitJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.ray import (
    CreateRayClusterOperator,
    DeleteRayClusterOperator,
    GetRayClusterOperator,
)

try:
    from google.cloud.aiplatform.vertex_ray.util import resources
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "The ray provider is optional and requires the `google-cloud-aiplatform` package to be installed. "
    )
try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "ray_job_operations"
LOCATION = "us-central1"
JOB_ID = f"{DAG_ID}_{ENV_ID}".replace("-", "_")
WORKER_NODE_RESOURCES = resources.Resources(
    node_count=1,
)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["example", "job", "ray"],
) as dag:
    create_ray_cluster = CreateRayClusterOperator(
        task_id="create_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        worker_node_types=[WORKER_NODE_RESOURCES],
        python_version="3.10",
        ray_version="2.33",
    )

    get_ray_cluster = GetRayClusterOperator(
        task_id="get_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=create_ray_cluster.output["cluster_id"],
    )

    # [START how_to_ray_submit_job]
    submit_ray_job = RaySubmitJobOperator(
        task_id="submit_ray_job",
        cluster_address="{{ task_instance.xcom_pull(task_ids='get_ray_cluster')['dashboard_address'] }}",
        entrypoint="echo hi && sleep 105 && echo hi2",
        runtime_env={
            "pip": [
                "ray==2.33.0",
            ],
        },
        get_job_logs=False,
        wait_for_job_done=False,
        submission_id=JOB_ID,
    )
    # [END how_to_ray_submit_job]

    # [START how_to_ray_get_job_info]
    info_ray_job = RayGetJobInfoOperator(
        task_id="info_ray_job",
        cluster_address="{{ task_instance.xcom_pull(task_ids='get_ray_cluster')['dashboard_address'] }}",
        job_id=JOB_ID,
    )
    # [END how_to_ray_get_job_info]

    # [START how_to_ray_list_jobs]
    list_ray_job = RayListJobsOperator(
        task_id="list_ray_job",
        cluster_address="{{ task_instance.xcom_pull(task_ids='get_ray_cluster')['dashboard_address'] }}",
    )
    # [END how_to_ray_list_jobs]

    # [START how_to_ray_stop_job]
    stop_ray_job = RayStopJobOperator(
        task_id="stop_ray_job",
        job_id=JOB_ID,
        cluster_address="{{ task_instance.xcom_pull(task_ids='get_ray_cluster')['dashboard_address'] }}",
    )
    # [END how_to_ray_stop_job]

    # [START how_to_ray_delete_job]
    delete_ray_job = RayDeleteJobOperator(
        task_id="delete_ray_job",
        cluster_address="{{ task_instance.xcom_pull(task_ids='get_ray_cluster')['dashboard_address'] }}",
        job_id=JOB_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_ray_delete_job]

    delete_ray_cluster = DeleteRayClusterOperator(
        task_id="delete_ray_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=create_ray_cluster.output["cluster_id"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        create_ray_cluster
        >> get_ray_cluster
        >> submit_ray_job
        >> info_ray_job
        >> stop_ray_job
        >> list_ray_job
        >> delete_ray_job
        >> delete_ray_cluster
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
