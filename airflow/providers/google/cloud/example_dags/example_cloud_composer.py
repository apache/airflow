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

import os
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerCreateEnvironmentOperator,
    CloudComposerDeleteEnvironmentOperator,
    CloudComposerGetEnvironmentOperator,
    CloudComposerListEnvironmentsOperator,
    CloudComposerListImageVersionsOperator,
    CloudComposerUpdateEnvironmentOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "<PROJECT_ID>")
REGION = os.environ.get("GCP_REGION", "<REGION>")

# [START howto_operator_composer_simple_environment]
ENVIRONMENT_ID = os.environ.get("ENVIRONMENT_ID", "ENVIRONMENT_ID>")
ENVIRONMENT = {
    "config": {
        "node_count": 3,
        "software_config": {"image_version": "composer-1.17.7-airflow-2.1.4"},
    }
}
# [END howto_operator_composer_simple_environment]

# [START howto_operator_composer_update_environment]
UPDATED_ENVIRONMENT = {
    "labels": {
        "label1": "testing",
    }
}
UPDATE_MASK = {"paths": ["labels.label1"]}
# [END howto_operator_composer_update_environment]


with models.DAG(
    "composer_dag1",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_composer_image_list]
    image_versions = CloudComposerListImageVersionsOperator(
        task_id="image_versions",
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END howto_operator_composer_image_list]

    # [START howto_operator_create_composer_environment]
    create_env = CloudComposerCreateEnvironmentOperator(
        task_id="create_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        environment=ENVIRONMENT,
    )
    # [END howto_operator_create_composer_environment]

    # [START howto_operator_list_composer_environments]
    list_envs = CloudComposerListEnvironmentsOperator(
        task_id="list_envs", project_id=PROJECT_ID, region=REGION
    )
    # [END howto_operator_list_composer_environments]

    # [START howto_operator_get_composer_environment]
    get_env = CloudComposerGetEnvironmentOperator(
        task_id="get_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
    )
    # [END howto_operator_get_composer_environment]

    # [START howto_operator_update_composer_environment]
    update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="update_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        update_mask=UPDATE_MASK,
        environment=UPDATED_ENVIRONMENT,
    )
    # [END howto_operator_update_composer_environment]

    # [START howto_operator_delete_composer_environment]
    delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="delete_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
    )
    # [END howto_operator_delete_composer_environment]

    chain(image_versions, create_env, list_envs, get_env, update_env, delete_env)


with models.DAG(
    "composer_dag_deferrable1",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as defer_dag:
    # [START howto_operator_create_composer_environment_deferrable_mode]
    defer_create_env = CloudComposerCreateEnvironmentOperator(
        task_id="defer_create_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        environment=ENVIRONMENT,
        deferrable=True,
    )
    # [END howto_operator_create_composer_environment_deferrable_mode]

    # [START howto_operator_update_composer_environment_deferrable_mode]
    defer_update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="defer_update_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        update_mask=UPDATE_MASK,
        environment=UPDATED_ENVIRONMENT,
        deferrable=True,
    )
    # [END howto_operator_update_composer_environment_deferrable_mode]

    # [START howto_operator_delete_composer_environment_deferrable_mode]
    defer_delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="defer_delete_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        deferrable=True,
    )
    # [END howto_operator_delete_composer_environment_deferrable_mode]

    chain(
        defer_create_env,
        defer_update_env,
        defer_delete_env,
    )
