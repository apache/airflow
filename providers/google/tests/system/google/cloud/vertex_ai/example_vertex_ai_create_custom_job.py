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


"""Example Airflow DAG for Google Vertex AI service testing create Custom Jobs operator."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomJobOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
REGION = "us-central1"
DAG_ID = "vertex_ai_create_custom_job"

REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
IMAGE_URI = "us-docker.pkg.dev/vertex-ai/training/tf-cpu.2-16.py310:latest"

test_python_code = (
    "import sys; "
    "print('=== VERTEX AI RAW CUSTOM_JOB RUNNING SUCCESSFULLY ==='); "
    "import math; "
    "print(f'Sanity check calculation (pi): {math.pi}'); "
    "print('=== TEST COMPLETED CLEANLY ==='); "
    "sys.exit(0);"
)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "custom_job"],
) as dag:
    create_custom_job = CreateCustomJobOperator(
        task_id="create_custom_job",
        region=REGION,
        project_id=PROJECT_ID,
        custom_job={
            "display_name": f"{DAG_ID}_{ENV_ID}",
            "labels": {
                "vertex_pipelines": "",
                "airflow_dag_id": DAG_ID,
                "airflow_task_id": DAG_ID,
            },
            "job_spec": {
                "scheduling": {"disable_retries": True},
                "worker_pool_specs": [
                    {
                        "machine_spec": {
                            "machine_type": MACHINE_TYPE,
                            "accelerator_type": ACCELERATOR_TYPE,
                            "accelerator_count": ACCELERATOR_COUNT,
                        },
                        "replica_count": REPLICA_COUNT,
                        "container_spec": {
                            "image_uri": IMAGE_URI,
                            "command": ["python3", "-c", test_python_code],
                        },
                    }
                ],
            },
        },
    )

    create_custom_job_def = CreateCustomJobOperator(
        task_id="create_custom_job_def",
        region=REGION,
        project_id=PROJECT_ID,
        custom_job={
            "display_name": f"{DAG_ID}_{ENV_ID}_def",
            "labels": {
                "vertex_pipelines": "",
                "airflow_dag_id": DAG_ID,
                "airflow_task_id": DAG_ID,
            },
            "job_spec": {
                "scheduling": {"disable_retries": True},
                "worker_pool_specs": [
                    {
                        "machine_spec": {
                            "machine_type": MACHINE_TYPE,
                            "accelerator_type": ACCELERATOR_TYPE,
                            "accelerator_count": ACCELERATOR_COUNT,
                        },
                        "replica_count": REPLICA_COUNT,
                        "container_spec": {
                            "image_uri": IMAGE_URI,
                            "command": ["python3", "-c", test_python_code],
                        },
                    }
                ],
            },
        },
        deferrable=True,
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
