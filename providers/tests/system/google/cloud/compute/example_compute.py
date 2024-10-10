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
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineDeleteInstanceTemplateOperator,
    ComputeEngineInsertInstanceFromTemplateOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineInsertInstanceTemplateOperator,
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

# [START howto_operator_gce_args_common]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "cloud_compute"

LOCATION = "europe-west2-b"
REGION = "europe-west2"
GCE_INSTANCE_NAME = "instance-compute-test"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
TEMPLATE_NAME = "instance-template"

INSTANCE_TEMPLATE_BODY = {
    "name": TEMPLATE_NAME,
    "properties": {
        "machine_type": SHORT_MACHINE_TYPE_NAME,
        "disks": [
            {
                "auto_delete": True,
                "boot": True,
                "device_name": TEMPLATE_NAME,
                "initialize_params": {
                    "disk_size_gb": "10",
                    "disk_type": "pd-balanced",
                    "source_image": "projects/debian-cloud/global/images/debian-12-bookworm-v20240611",
                },
            }
        ],
        "network_interfaces": [{"network": "global/networks/default"}],
    },
}
GCE_INSTANCE_BODY = {
    "name": GCE_INSTANCE_NAME,
    "machine_type": f"zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
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
GCE_INSTANCE_FROM_TEMPLATE_BODY = {
    "name": GCE_INSTANCE_NAME,
}
# [END howto_operator_gce_args_common]

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "compute"],
) as dag:
    # [START howto_operator_gce_insert]
    gce_instance_insert = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task",
        project_id=PROJECT_ID,
        zone=LOCATION,
        body=GCE_INSTANCE_BODY,
    )
    # [END howto_operator_gce_insert]

    # Duplicate start for idempotence testing
    # [START howto_operator_gce_insert_no_project_id]
    gce_instance_insert2 = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task_2",
        zone=LOCATION,
        body=GCE_INSTANCE_BODY,
    )
    # [END howto_operator_gce_insert_no_project_id]

    # [START howto_operator_gce_igm_insert_template]
    gce_instance_template_insert = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task",
        project_id=PROJECT_ID,
        body=INSTANCE_TEMPLATE_BODY,
    )
    # [END howto_operator_gce_igm_insert_template]

    # Added to check for idempotence
    # [START howto_operator_gce_igm_insert_template_no_project_id]
    gce_instance_template_insert2 = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task_2",
        body=INSTANCE_TEMPLATE_BODY,
    )
    # [END howto_operator_gce_igm_insert_template_no_project_id]

    # [START howto_operator_gce_insert_from_template]
    gce_instance_insert_from_template = ComputeEngineInsertInstanceFromTemplateOperator(
        task_id="gcp_compute_create_instance_from_template_task",
        project_id=PROJECT_ID,
        zone=LOCATION,
        body=GCE_INSTANCE_FROM_TEMPLATE_BODY,
        source_instance_template=f"global/instanceTemplates/{TEMPLATE_NAME}",
    )
    # [END howto_operator_gce_insert_from_template]

    # Duplicate start for idempotence testing
    # [START howto_operator_gce_insert_from_template_no_project_id]
    gce_instance_insert_from_template2 = ComputeEngineInsertInstanceFromTemplateOperator(
        task_id="gcp_compute_create_instance_from_template_task_2",
        zone=LOCATION,
        body=GCE_INSTANCE_FROM_TEMPLATE_BODY,
        source_instance_template=f"global/instanceTemplates/{TEMPLATE_NAME}",
    )
    # [END howto_operator_gce_insert_from_template_no_project_id]

    # [START howto_operator_gce_start]
    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id="gcp_compute_start_task",
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
    )
    # [END howto_operator_gce_start]

    # Duplicate start for idempotence testing
    # [START howto_operator_gce_start_no_project_id]
    gce_instance_start2 = ComputeEngineStartInstanceOperator(
        task_id="gcp_compute_start_task_2",
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
    )
    # [END howto_operator_gce_start_no_project_id]

    # [START howto_operator_gce_stop]
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id="gcp_compute_stop_task",
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_stop]

    # Duplicate stop for idempotence testing
    # [START howto_operator_gce_stop_no_project_id]
    gce_instance_stop2 = ComputeEngineStopInstanceOperator(
        task_id="gcp_compute_stop_task_2",
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_stop_no_project_id]

    # [START howto_operator_gce_set_machine_type]
    gce_set_machine_type = ComputeEngineSetMachineTypeOperator(
        task_id="gcp_compute_set_machine_type",
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        body={"machineType": f"zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}"},
    )
    # [END howto_operator_gce_set_machine_type]

    # Duplicate set machine type for idempotence testing
    # [START howto_operator_gce_set_machine_type_no_project_id]
    gce_set_machine_type2 = ComputeEngineSetMachineTypeOperator(
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        body={"machineType": f"zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}"},
        task_id="gcp_compute_set_machine_type_2",
    )
    # [END howto_operator_gce_set_machine_type_no_project_id]

    # [START howto_operator_gce_delete_no_project_id]
    gce_instance_delete = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task",
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_no_project_id]

    # [START howto_operator_gce_delete_no_project_id]
    gce_instance_delete2 = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task_2",
        zone=LOCATION,
        resource_id=GCE_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_no_project_id]

    # [START howto_operator_gce_delete_new_template_no_project_id]
    gce_instance_template_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_template_task",
        resource_id=TEMPLATE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_new_template_no_project_id]

    (
        # TEST SETUP
        gce_instance_insert
        >> gce_instance_insert2
        # TEST BODY
        >> gce_instance_delete
        >> gce_instance_template_insert
        >> gce_instance_template_insert2
        >> gce_instance_insert_from_template
        >> gce_instance_insert_from_template2
        >> gce_instance_start
        >> gce_instance_start2
        >> gce_instance_stop
        >> gce_instance_stop2
        >> gce_set_machine_type
        >> gce_set_machine_type2
        # TEST TEARDOWN
        >> gce_instance_delete2
        >> gce_instance_template_delete
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
