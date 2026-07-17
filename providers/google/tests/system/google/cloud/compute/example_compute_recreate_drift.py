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
System test for ComputeEngineInsertInstanceOperator
verifying recreate_if_machine_type_different=True recreates the
correct machine_type instance when machine_type drifts.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "cloud_compute_insert_recreate_if_different"
LOCATION = "us-central1-a"

INSTANCE_NAME = f"airflow-drift-test-{ENV_ID}"
MACHINE_TYPE_A = "n1-standard-1"
MACHINE_TYPE_B = "n1-standard-2"

BASE_BODY = {
    "name": INSTANCE_NAME,
    "disks": [
        {
            "boot": True,
            "auto_delete": True,
            "initialize_params": {
                "disk_size_gb": "10",
                "source_image": "projects/debian-cloud/global/images/family/debian-12",
            },
        }
    ],
    "network_interfaces": [{"network": "global/networks/default"}],
}


def assert_machine_type():
    hook = ComputeEngineHook()
    instance = hook.get_instance(
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=INSTANCE_NAME,
    )

    machine_type = instance.machine_type.split("/")[-1]

    assert machine_type == MACHINE_TYPE_B, f"Expected machine type {MACHINE_TYPE_B}, got {machine_type}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "compute"],
) as dag:
    # Step 1: Create with machine type A.
    create_instance = ComputeEngineInsertInstanceOperator(
        task_id="create_instance",
        project_id=PROJECT_ID,
        zone=LOCATION,
        body={
            **BASE_BODY,
            "machine_type": f"zones/{LOCATION}/machineTypes/{MACHINE_TYPE_A}",
        },
    )

    # Step 2: Re-run with different machine type and recreate recreate_if_machine_type_different=True.
    recreate_instance = ComputeEngineInsertInstanceOperator(
        task_id="recreate_instance",
        project_id=PROJECT_ID,
        zone=LOCATION,
        body={
            **BASE_BODY,
            "machine_type": f"zones/{LOCATION}/machineTypes/{MACHINE_TYPE_B}",
        },
        recreate_if_machine_type_different=True,
    )

    # Step 3: Validate new machine type.
    validate_machine_type = PythonOperator(
        task_id="validate_machine_type",
        python_callable=assert_machine_type,
    )

    # Step 4: Cleanup.
    delete_instance = ComputeEngineDeleteInstanceOperator(
        task_id="delete_instance",
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_instance >> recreate_instance >> validate_machine_type >> delete_instance

    # Everything below this line is required for system tests.
    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
