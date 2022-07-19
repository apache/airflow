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
Example Airflow DAG that uses IGM-type compute operations:
* copy of Instance Template
* update template in Instance Group Manager

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - the Google Cloud project where the Compute Engine instance exists
* GCE_ZONE - the zone where the Compute Engine instance exists

Variables for copy template operator:
* GCE_TEMPLATE_NAME - name of the template to copy
* GCE_NEW_TEMPLATE_NAME - name of the new template
* GCE_NEW_DESCRIPTION - description added to the template

Variables for update template in Group Manager:

* GCE_INSTANCE_GROUP_MANAGER_NAME - name of the Instance Group Manager
* SOURCE_TEMPLATE_URL - url of the template to replace in the Instance Group Manager
* DESTINATION_TEMPLATE_URL - url of the new template to set in the Instance Group Manager
"""

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCE_ZONE = os.environ.get('GCE_ZONE', 'europe-west1-b')

DAG_ID = 'example_gcp_compute_igm'

# [START howto_operator_compute_template_copy_args]
GCE_TEMPLATE_NAME = os.environ.get('GCE_TEMPLATE_NAME', 'instance-template-test')
GCE_NEW_TEMPLATE_NAME = os.environ.get('GCE_NEW_TEMPLATE_NAME', 'instance-template-test-new')
GCE_NEW_DESCRIPTION = os.environ.get('GCE_NEW_DESCRIPTION', 'Test new description')
GCE_INSTANCE_TEMPLATE_BODY_UPDATE = {
    "name": GCE_NEW_TEMPLATE_NAME,
    "description": GCE_NEW_DESCRIPTION,
    "properties": {"machineType": "n1-standard-2"},
}
# [END howto_operator_compute_template_copy_args]

# [START howto_operator_compute_igm_update_template_args]
GCE_INSTANCE_GROUP_MANAGER_NAME = os.environ.get('GCE_INSTANCE_GROUP_MANAGER_NAME', 'instance-group-test')

SOURCE_TEMPLATE_URL = os.environ.get(
    'SOURCE_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    + GCP_PROJECT_ID
    + "/global/instanceTemplates/instance-template-test",
)

DESTINATION_TEMPLATE_URL = os.environ.get(
    'DESTINATION_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    + GCP_PROJECT_ID
    + "/global/instanceTemplates/"
    + GCE_NEW_TEMPLATE_NAME,
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
    tags=['example', 'igm'],
) as dag:
    # [START howto_operator_gce_igm_copy_template]
    gce_instance_template_copy = ComputeEngineCopyInstanceTemplateOperator(
        project_id=GCP_PROJECT_ID,
        resource_id=GCE_TEMPLATE_NAME,
        body_patch=GCE_INSTANCE_TEMPLATE_BODY_UPDATE,
        task_id='gcp_compute_igm_copy_template_task',
    )
    # [END howto_operator_gce_igm_copy_template]
    # Added to check for idempotence
    # [START howto_operator_gce_igm_copy_template_no_project_id]
    gce_instance_template_copy2 = ComputeEngineCopyInstanceTemplateOperator(
        resource_id=GCE_TEMPLATE_NAME,
        body_patch=GCE_INSTANCE_TEMPLATE_BODY_UPDATE,
        task_id='gcp_compute_igm_copy_template_task_2',
    )
    # [END howto_operator_gce_igm_copy_template_no_project_id]
    # [START howto_operator_gce_igm_update_template]
    gce_instance_group_manager_update_template = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
        project_id=GCP_PROJECT_ID,
        resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
        zone=GCE_ZONE,
        source_template=SOURCE_TEMPLATE_URL,
        destination_template=DESTINATION_TEMPLATE_URL,
        update_policy=UPDATE_POLICY,
        task_id='gcp_compute_igm_group_manager_update_template',
    )
    # [END howto_operator_gce_igm_update_template]
    # Added to check for idempotence (and without UPDATE_POLICY)
    # [START howto_operator_gce_igm_update_template_no_project_id]
    gce_instance_group_manager_update_template2 = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
        resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
        zone=GCE_ZONE,
        source_template=SOURCE_TEMPLATE_URL,
        destination_template=DESTINATION_TEMPLATE_URL,
        task_id='gcp_compute_igm_group_manager_update_template_2',
    )
    # [END howto_operator_gce_igm_update_template_no_project_id]

    (
        # TEST BODY
        gce_instance_template_copy
        >> gce_instance_template_copy2
        >> gce_instance_group_manager_update_template
        >> gce_instance_group_manager_update_template2
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
