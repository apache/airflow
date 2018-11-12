# -*- coding: utf-8 -*-
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

* PROJECT_ID - the Google Cloud Platform project where the Compute Engine instance exists
* ZONE - the zone where the Compute Engine instance exists

Variables for copy template operator:
* TEMPLATE_NAME - name of the template to copy
* NEW_TEMPLATE_NAME - name of the new template
* NEW_DESCRIPTION - description added to the template

Variables for update template in Group Manager:

* INSTANCE_GROUP_MANAGER_NAME - name of the Instance Group Manager
* SOURCE_TEMPLATE_URL - url of the template to replace in the Instance Group Manager
* DESTINATION_TEMPLATE_URL - url of the new template to set in the Instance Group Manager
"""

import os
import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcp_compute_operator import \
    GceInstanceTemplateCopyOperator, GceInstanceGroupManagerUpdateTemplateOperator

# [START howto_operator_compute_igm_common_args]
PROJECT_ID = os.environ.get('PROJECT_ID', 'example-project')
ZONE = os.environ.get('ZONE', 'europe-west1-b')

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}
# [END howto_operator_compute_igm_common_args]

# [START howto_operator_compute_template_copy_args]
TEMPLATE_NAME = os.environ.get('TEMPLATE_NAME', 'instance-template-test')
NEW_TEMPLATE_NAME = os.environ.get('NEW_TEMPLATE_NAME',
                                   'instance-template-test-new')
NEW_DESCRIPTION = os.environ.get('NEW_DESCRIPTION', 'Test new description')
GCE_INSTANCE_TEMPLATE_BODY_UPDATE = {
    "name": NEW_TEMPLATE_NAME,
    "description": NEW_DESCRIPTION,
    "properties": {
        "machineType": "n1-standard-2"
    }
}
# [END howto_operator_compute_template_copy_args]

# [START howto_operator_compute_igm_update_template_args]
INSTANCE_GROUP_MANAGER_NAME = os.environ.get('INSTANCE_GROUP_MANAGER_NAME',
                                             'instance-group-test')

SOURCE_TEMPLATE_URL = os.environ.get(
    'SOURCE_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    "example-project/global/instanceTemplates/instance-template-test")

DESTINATION_TEMPLATE_URL = os.environ.get(
    'DESTINATION_TEMPLATE_URL',
    "https://www.googleapis.com/compute/beta/projects/"
    "example-airflow/global/instanceTemplates/" + NEW_TEMPLATE_NAME)

UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {
        "fixed": 1
    },
    "minReadySec": 1800
}

# [END howto_operator_compute_igm_update_template_args]


with models.DAG(
    'example_gcp_compute_igm',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_gce_igm_copy_template]
    gce_instance_template_copy = GceInstanceTemplateCopyOperator(
        project_id=PROJECT_ID,
        resource_id=TEMPLATE_NAME,
        body_patch=GCE_INSTANCE_TEMPLATE_BODY_UPDATE,
        task_id='gcp_compute_igm_copy_template_task'
    )
    # [END howto_operator_gce_igm_copy_template]
    # Added to check for idempotence
    gce_instance_template_copy2 = GceInstanceTemplateCopyOperator(
        project_id=PROJECT_ID,
        resource_id=TEMPLATE_NAME,
        body_patch=GCE_INSTANCE_TEMPLATE_BODY_UPDATE,
        task_id='gcp_compute_igm_copy_template_task_2'
    )
    # [START howto_operator_gce_igm_update_template]
    gce_instance_group_manager_update_template = \
        GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            resource_id=INSTANCE_GROUP_MANAGER_NAME,
            zone=ZONE,
            source_template=SOURCE_TEMPLATE_URL,
            destination_template=DESTINATION_TEMPLATE_URL,
            update_policy=UPDATE_POLICY,
            task_id='gcp_compute_igm_group_manager_update_template'
        )
    # [END howto_operator_gce_igm_update_template]
    # Added to check for idempotence (and without UPDATE_POLICY)
    gce_instance_group_manager_update_template2 = \
        GceInstanceGroupManagerUpdateTemplateOperator(
            project_id=PROJECT_ID,
            resource_id=INSTANCE_GROUP_MANAGER_NAME,
            zone=ZONE,
            source_template=SOURCE_TEMPLATE_URL,
            destination_template=DESTINATION_TEMPLATE_URL,
            task_id='gcp_compute_igm_group_manager_update_template_2'
        )
    gce_instance_template_copy >> gce_instance_template_copy2 >> \
        gce_instance_group_manager_update_template >> \
        gce_instance_group_manager_update_template2
