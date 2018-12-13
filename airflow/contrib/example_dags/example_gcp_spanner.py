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
Example Airflow DAG that creates, updates and deletes a Cloud Spanner instance.

This DAG relies on the following environment variables
* PROJECT_ID - Google Cloud Platform project for the Cloud Spanner instance.
* INSTANCE_ID - Cloud Spanner instance ID.
* CONFIG_NAME - The name of the instance's configuration. Values are of the form
    projects/<project>/instanceConfigs/<configuration>.
    See also:
        https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs#InstanceConfig
        https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list#google.spanner.admin.instance.v1.InstanceAdmin.ListInstanceConfigs
* NODE_COUNT - Number of nodes allocated to the instance.
* DISPLAY_NAME - The descriptive name for this instance as it appears in UIs.
    Must be unique per project and between 4 and 30 characters in length.
"""

import os

import airflow
from airflow import models
from airflow.contrib.operators.gcp_spanner_operator import \
    CloudSpannerInstanceDeployOperator, CloudSpannerInstanceDeleteOperator

# [START howto_operator_spanner_arguments]
PROJECT_ID = os.environ.get('PROJECT_ID', 'example-project')
INSTANCE_ID = os.environ.get('INSTANCE_ID', 'testinstance')
CONFIG_NAME = os.environ.get('CONFIG_NAME',
                             'projects/example-project/instanceConfigs/eur3')
NODE_COUNT = os.environ.get('NODE_COUNT', '1')
DISPLAY_NAME = os.environ.get('DISPLAY_NAME', 'Test Instance')
# [END howto_operator_spanner_arguments]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_spanner',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:
    # Create
    # [START howto_operator_spanner_deploy]
    spanner_instance_create_task = CloudSpannerInstanceDeployOperator(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        configuration_name=CONFIG_NAME,
        node_count=int(NODE_COUNT),
        display_name=DISPLAY_NAME,
        task_id='spanner_instance_create_task'
    )
    # [END howto_operator_spanner_deploy]

    # Update
    spanner_instance_update_task = CloudSpannerInstanceDeployOperator(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        configuration_name=CONFIG_NAME,
        node_count=int(NODE_COUNT) + 1,
        display_name=DISPLAY_NAME + '_updated',
        task_id='spanner_instance_update_task'
    )

    # [START howto_operator_spanner_delete]
    spanner_instance_delete_task = CloudSpannerInstanceDeleteOperator(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        task_id='spanner_instance_delete_task'
    )
    # [END howto_operator_spanner_delete]

    spanner_instance_create_task >> spanner_instance_update_task \
        >> spanner_instance_delete_task
