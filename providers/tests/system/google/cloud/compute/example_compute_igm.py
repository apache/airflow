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
Example Airflow DAG that:
* creates a copy of existing Instance Template
* updates existing template in Instance Group Manager

"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineDeleteInstanceGroupManagerOperator,
    ComputeEngineDeleteInstanceTemplateOperator,
    ComputeEngineInsertInstanceGroupManagerOperator,
    ComputeEngineInsertInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

LOCATION = "europe-west1-b"
REGION = "europe-west1"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
DAG_ID = "cloud_compute_igm"

# [START howto_operator_compute_template_copy_args]
TEMPLATE_NAME = "instance-template-igm-test"
NEW_TEMPLATE_NAME = "instance-template-test-new"

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

NEW_DESCRIPTION = "Test new description"
INSTANCE_TEMPLATE_BODY_UPDATE = {
    "name": NEW_TEMPLATE_NAME,
    "description": NEW_DESCRIPTION,
    "properties": {"machine_type": "n1-standard-2"},
}
# [END howto_operator_compute_template_copy_args]

# [START howto_operator_compute_igm_update_template_args]
INSTANCE_GROUP_MANAGER_NAME = "instance-group-test"
INSTANCE_GROUP_MANAGER_BODY = {
    "name": INSTANCE_GROUP_MANAGER_NAME,
    "base_instance_name": INSTANCE_GROUP_MANAGER_NAME,
    "instance_template": f"global/instanceTemplates/{TEMPLATE_NAME}",
    "target_size": 1,
}

SOURCE_TEMPLATE_URL = (
    f"https://www.googleapis.com/compute/beta/projects/{PROJECT_ID}/"
    f"global/instanceTemplates/{TEMPLATE_NAME}"
)


DESTINATION_TEMPLATE_URL = (
    f"https://www.googleapis.com/compute/beta/projects/{PROJECT_ID}/"
    f"global/instanceTemplates/{NEW_TEMPLATE_NAME}"
)

UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {"fixed": 1},
    "minReadySec": 1800,
}
# [END howto_operator_compute_igm_update_template_args]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "compute-igm"],
) as dag:
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

    # [START howto_operator_gce_igm_copy_template]
    gce_instance_template_copy = ComputeEngineCopyInstanceTemplateOperator(
        task_id="gcp_compute_igm_copy_template_task",
        project_id=PROJECT_ID,
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    # [END howto_operator_gce_igm_copy_template]

    # Added to check for idempotence
    # [START howto_operator_gce_igm_copy_template_no_project_id]
    gce_instance_template_copy2 = ComputeEngineCopyInstanceTemplateOperator(
        task_id="gcp_compute_igm_copy_template_task_2",
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    # [END howto_operator_gce_igm_copy_template_no_project_id]

    # [START howto_operator_gce_insert_igm]
    gce_igm_insert = ComputeEngineInsertInstanceGroupManagerOperator(
        task_id="gcp_compute_create_group_task",
        zone=LOCATION,
        body=INSTANCE_GROUP_MANAGER_BODY,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_gce_insert_igm]

    # Added to check for idempotence
    # [START howto_operator_gce_insert_igm_no_project_id]
    gce_igm_insert2 = ComputeEngineInsertInstanceGroupManagerOperator(
        task_id="gcp_compute_create_group_task_2",
        zone=LOCATION,
        body=INSTANCE_GROUP_MANAGER_BODY,
    )
    # [END howto_operator_gce_insert_igm_no_project_id]

    # [START howto_operator_gce_igm_update_template]
    gce_instance_group_manager_update_template = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
        task_id="gcp_compute_igm_group_manager_update_template",
        project_id=PROJECT_ID,
        resource_id=INSTANCE_GROUP_MANAGER_NAME,
        zone=LOCATION,
        source_template=SOURCE_TEMPLATE_URL,
        destination_template=DESTINATION_TEMPLATE_URL,
        update_policy=UPDATE_POLICY,
    )
    # [END howto_operator_gce_igm_update_template]

    # Added to check for idempotence (and without UPDATE_POLICY)
    # [START howto_operator_gce_igm_update_template_no_project_id]
    gce_instance_group_manager_update_template2 = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
        task_id="gcp_compute_igm_group_manager_update_template_2",
        resource_id=INSTANCE_GROUP_MANAGER_NAME,
        zone=LOCATION,
        source_template=SOURCE_TEMPLATE_URL,
        destination_template=DESTINATION_TEMPLATE_URL,
    )
    # [END howto_operator_gce_igm_update_template_no_project_id]

    # [START howto_operator_gce_delete_old_template_no_project_id]
    gce_instance_template_old_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_old_template_task",
        resource_id=TEMPLATE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_old_template_no_project_id]

    # [START howto_operator_gce_delete_new_template_no_project_id]
    gce_instance_template_new_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_new_template_task",
        resource_id=NEW_TEMPLATE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_new_template_no_project_id]

    # [START howto_operator_gce_delete_igm_no_project_id]
    gce_igm_delete = ComputeEngineDeleteInstanceGroupManagerOperator(
        task_id="gcp_compute_delete_group_task",
        resource_id=INSTANCE_GROUP_MANAGER_NAME,
        zone=LOCATION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gce_delete_igm_no_project_id]

    (
        # TEST SETUP
        gce_instance_template_insert
        >> gce_instance_template_insert2
        >> gce_instance_template_copy
        >> gce_instance_template_copy2
        # TEST BODY
        >> gce_igm_insert
        >> gce_igm_insert2
        >> gce_instance_group_manager_update_template
        >> gce_instance_group_manager_update_template2
        # TEST TEARDOWN
        >> gce_igm_delete
        >> gce_instance_template_old_delete
        >> gce_instance_template_new_delete
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
