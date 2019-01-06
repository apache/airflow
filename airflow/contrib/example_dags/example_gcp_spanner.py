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
Example Airflow DAG that creates, updates, queries and deletes a Cloud Spanner instance.

This DAG relies on the following environment variables
* GCP_PROJECT_ID - Google Cloud Platform project for the Cloud Spanner instance.
* GCP_SPANNER_INSTANCE_ID - Cloud Spanner instance ID.
* GCP_SPANNER_DATABASE_ID - Cloud Spanner database ID.
* GCP_SPANNER_CONFIG_NAME - The name of the instance's configuration. Values are of the
    form projects/<gcp_project>/instanceConfigs/<configuration>.
    See also:
        https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs#InstanceConfig
        https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list#google.spanner.admin.instance.v1.InstanceAdmin.ListInstanceConfigs
* GCP_SPANNER_NODE_COUNT - Number of nodes allocated to the instance.
* GCP_SPANNER_DISPLAY_NAME - The descriptive name for this instance as it appears in UIs.
    Must be unique per project and between 4 and 30 characters in length.
"""

import os

import airflow
from airflow import models
from airflow.contrib.operators.gcp_spanner_operator import \
    CloudSpannerInstanceDeployOperator, CloudSpannerInstanceDatabaseQueryOperator, \
    CloudSpannerInstanceDeleteOperator, \
    CloudSpannerInstanceDatabaseDeployOperator, \
    CloudSpannerInstanceDatabaseUpdateOperator, \
    CloudSpannerInstanceDatabaseDeleteOperator

# [START howto_operator_spanner_arguments]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_SPANNER_INSTANCE_ID = os.environ.get('GCP_SPANNER_INSTANCE_ID', 'testinstance')
GCP_SPANNER_DATABASE_ID = os.environ.get('GCP_SPANNER_DATABASE_ID', 'testdatabase')
GCP_SPANNER_CONFIG_NAME = os.environ.get('GCP_SPANNER_CONFIG_NAME',
                                         'projects/example-project/instanceConfigs/eur3')
GCP_SPANNER_NODE_COUNT = os.environ.get('GCP_SPANNER_NODE_COUNT', '1')
GCP_SPANNER_DISPLAY_NAME = os.environ.get('GCP_SPANNER_DISPLAY_NAME', 'Test Instance')
# OPERATION_ID should be unique per operation
OPERATION_ID = 'unique_operation_id'
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
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=int(GCP_SPANNER_NODE_COUNT),
        display_name=GCP_SPANNER_DISPLAY_NAME,
        task_id='spanner_instance_create_task'
    )
    # [END howto_operator_spanner_deploy]

    # Update
    spanner_instance_update_task = CloudSpannerInstanceDeployOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=int(GCP_SPANNER_NODE_COUNT) + 1,
        display_name=GCP_SPANNER_DISPLAY_NAME + '_updated',
        task_id='spanner_instance_update_task'
    )

    # [START howto_operator_spanner_database_deploy]
    spanner_database_deploy_task = CloudSpannerInstanceDatabaseDeployOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table1 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
            "CREATE TABLE my_table2 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id='spanner_database_deploy_task'
    )
    # [END howto_operator_spanner_database_deploy]

    # [START howto_operator_spanner_database_update]
    spanner_database_update_task = CloudSpannerInstanceDatabaseUpdateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table3 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id='spanner_database_update_task'
    )
    # [END howto_operator_spanner_database_update]

    # [START howto_operator_spanner_database_update_idempotent]
    spanner_database_update_idempotent1_task = CloudSpannerInstanceDatabaseUpdateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id='spanner_database_update_idempotent1_task'
    )
    spanner_database_update_idempotent2_task = CloudSpannerInstanceDatabaseUpdateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id='spanner_database_update_idempotent2_task'
    )
    # [END howto_operator_spanner_database_update_idempotent]

    # [START howto_operator_spanner_query]
    spanner_instance_query_task = CloudSpannerInstanceDatabaseQueryOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id='db1',
        query=["DELETE FROM my_table2 WHERE true"],
        task_id='spanner_instance_query'
    )
    # [END howto_operator_spanner_query]

    spanner_instance_query2_task = CloudSpannerInstanceDatabaseQueryOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id='db1',
        query="example_gcp_spanner.sql",
        task_id='spanner_instance_query2'
    )

    # [START howto_operator_spanner_database_delete]
    spanner_database_delete_task = CloudSpannerInstanceDatabaseDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        task_id='spanner_database_delete_task'
    )
    # [END howto_operator_spanner_database_delete]

    # [START howto_operator_spanner_delete]
    spanner_instance_delete_task = CloudSpannerInstanceDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        task_id='spanner_instance_delete_task'
    )
    # [END howto_operator_spanner_delete]

    spanner_instance_create_task \
        >> spanner_instance_update_task \
        >> spanner_database_deploy_task \
        >> spanner_database_update_task \
        >> spanner_database_update_idempotent1_task \
        >> spanner_database_update_idempotent2_task \
        >> spanner_instance_query_task \
        >> spanner_instance_query2_task \
        >> spanner_database_delete_task \
        >> spanner_instance_delete_task
