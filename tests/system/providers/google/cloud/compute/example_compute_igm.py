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

import os
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
)

ENV_ID = os.environ.get('SYSTEM_TESTS_ENV_ID')
PROJECT_ID = os.environ.get('SYSTEM_TESTS_GCP_PROJECT')

LOCATION = 'europe-west1-b'
DAG_ID = 'cloud_compute_igm'

# [START howto_operator_compute_template_copy_args]
# todo: add operator create template
TEMPLATE_NAME = 'instance-template-compute-igm-test'
NEW_TEMPLATE_NAME = 'instance-template-test-new'

NEW_DESCRIPTION = 'Test new description'
INSTANCE_TEMPLATE_BODY_UPDATE = {
    "name": NEW_TEMPLATE_NAME,
    "description": NEW_DESCRIPTION,
    "properties": {"machineType": "n1-standard-2"},
}
# [END howto_operator_compute_template_copy_args]

# [START howto_operator_compute_igm_update_template_args]
# todo: requires operator to create instance group manager
INSTANCE_GROUP_MANAGER_NAME = 'instance-group-test'

SOURCE_TEMPLATE_URL = os.environ.get(
    'SOURCE_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    + PROJECT_ID
    + "/global/instanceTemplates/"
    + TEMPLATE_NAME,
    )

DESTINATION_TEMPLATE_URL = os.environ.get(
    'DESTINATION_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    + PROJECT_ID
    + "/global/instanceTemplates/"
    + NEW_TEMPLATE_NAME,
    )

UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {"fixed": 1},
    "minReadySec": 1800,
}
# [END howto_operator_compute_igm_update_template_args]


with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_gce_igm_copy_template]
    gce_instance_template_copy = ComputeEngineCopyInstanceTemplateOperator(
        task_id='gcp_compute_igm_copy_template_task',
        project_id=PROJECT_ID,
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    # [END howto_operator_gce_igm_copy_template]

    # Added to check for idempotence
    # [START howto_operator_gce_igm_copy_template_no_project_id]
    gce_instance_template_copy2 = ComputeEngineCopyInstanceTemplateOperator(
        task_id='gcp_compute_igm_copy_template_task_2',
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    # [END howto_operator_gce_igm_copy_template_no_project_id]

    # [START howto_operator_gce_igm_update_template]
    gce_instance_group_manager_update_template = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
        task_id='gcp_compute_igm_group_manager_update_template',
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
        task_id='gcp_compute_igm_group_manager_update_template_2',
        resource_id=INSTANCE_GROUP_MANAGER_NAME,
        zone=LOCATION,
        source_template=SOURCE_TEMPLATE_URL,
        destination_template=DESTINATION_TEMPLATE_URL,
    )
    # [END howto_operator_gce_igm_update_template_no_project_id]

    chain(
        gce_instance_template_copy,
        gce_instance_template_copy2,
        gce_instance_group_manager_update_template,
        gce_instance_group_manager_update_template2,
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
