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
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

# [START howto_operator_gce_args_common]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "cloud_compute_ssh_parallel"
LOCATION = "europe-west1-b"
REGION = "europe-west1"
GCE_INSTANCE_NAME = "inst-ssh-test-parallel"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
GCE_INSTANCE_BODY = {
    "name": GCE_INSTANCE_NAME,
    "machine_type": f"zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "auto_delete": True,
            "device_name": GCE_INSTANCE_NAME,
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{LOCATION}/diskTypes/pd-balanced",
                "source_image": "projects/debian-cloud/global/images/debian-12-bookworm-v20240611",
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": f"regions/{REGION}/subnetworks/default",
        }
    ],
}
# [END howto_operator_gce_args_common]

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "compute-ssh-parallel"],
) as dag:
    # [START howto_operator_gce_insert]
    gce_instance_insert = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task",
        project_id=PROJECT_ID,
        zone=LOCATION,
        body=GCE_INSTANCE_BODY,
    )
    # [END howto_operator_gce_insert]

    # [START howto_execute_command_on_remote_1]
    metadata_without_iap_tunnel = SSHOperator(
        task_id="metadata_without_iap_tunnel",
        ssh_hook=ComputeEngineSSHHook(
            user="username1",
            instance_name=GCE_INSTANCE_NAME,
            zone=LOCATION,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            max_retries=5,
            cmd_timeout=1,
        ),
        command="echo metadata_without_iap_tunnel",
    )
    # [END howto_execute_command_on_remote_1]

    # [START howto_execute_command_on_remote_2]
    metadata_with_iap_tunnel = SSHOperator(
        task_id="metadata_with_iap_tunnel",
        ssh_hook=ComputeEngineSSHHook(
            user="username2",
            instance_name=GCE_INSTANCE_NAME,
            zone=LOCATION,
            use_oslogin=False,
            use_iap_tunnel=True,
            max_retries=5,
            cmd_timeout=1,
        ),
        command="echo metadata_with_iap_tunnel",
    )
    # [END howto_execute_command_on_remote_2]

    # [START howto_operator_gce_delete_no_project_id]
    gce_instance_delete = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task",
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_no_project_id]

    (
        # TEST SETUP
        gce_instance_insert
        # TEST BODY
        >> [metadata_without_iap_tunnel, metadata_with_iap_tunnel]
        # TEST TEARDOWN
        >> gce_instance_delete
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
