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

from __future__ import annotations

from pathlib import Path

from pendulum import datetime

from airflow.providers.ray.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob
from airflow.sdk import DAG

CONN_ID = "ray_conn"
RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"

with DAG(
    "Setup_Teardown",
    start_date=datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ray", "example"],
):
    setup_cluster = SetupRayCluster(
        task_id="SetupRayCluster", conn_id=CONN_ID, ray_cluster_yaml=str(RAY_SPEC), update_if_exists=False
    )

    submit_ray_job = SubmitRayJob(
        task_id="SubmitRayJob",
        conn_id=CONN_ID,
        entrypoint="python script.py",
        runtime_env={"working_dir": str(FOLDER_PATH)},
        num_cpus=1,
        num_gpus=0,
        memory=0,
        resources={},
        fetch_logs=True,
        wait_for_completion=True,
        job_timeout_seconds=600,
        xcom_task_key="SetupRayCluster.dashboard",
        poll_interval=5,
    )

    delete_cluster = DeleteRayCluster(
        task_id="DeleteRayCluster", conn_id=CONN_ID, ray_cluster_yaml=str(RAY_SPEC)
    )

    # Create ray cluster and submit ray job
    setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
    setup_cluster >> delete_cluster
