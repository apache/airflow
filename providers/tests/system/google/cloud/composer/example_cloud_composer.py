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
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerCreateEnvironmentOperator,
    CloudComposerDeleteEnvironmentOperator,
    CloudComposerGetEnvironmentOperator,
    CloudComposerListEnvironmentsOperator,
    CloudComposerListImageVersionsOperator,
    CloudComposerRunAirflowCLICommandOperator,
    CloudComposerUpdateEnvironmentOperator,
)
from airflow.providers.google.cloud.sensors.cloud_composer import CloudComposerDAGRunSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

DAG_ID = "composer"
REGION = "us-central1"

# [START howto_operator_composer_simple_environment]

ENVIRONMENT_ID = f"test-{DAG_ID}-{ENV_ID}".replace("_", "-")
ENVIRONMENT_ID_ASYNC = f"test-deferrable-{DAG_ID}-{ENV_ID}".replace("_", "-")

ENVIRONMENT = {
    "config": {
        "software_config": {"image_version": "composer-2.5.0-airflow-2.5.3"},
    }
}
# [END howto_operator_composer_simple_environment]

# [START howto_operator_composer_update_environment]
UPDATED_ENVIRONMENT = {
    "labels": {
        "label": "testing",
    }
}
UPDATE_MASK = {"paths": ["labels.label"]}
# [END howto_operator_composer_update_environment]

COMMAND = "dags list -o json --verbose"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "composer"],
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

    # [START howto_operator_create_composer_environment_deferrable_mode]
    defer_create_env = CloudComposerCreateEnvironmentOperator(
        task_id="defer_create_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        environment=ENVIRONMENT,
        deferrable=True,
    )
    # [END howto_operator_create_composer_environment_deferrable_mode]

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

    # [START howto_operator_update_composer_environment_deferrable_mode]
    defer_update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="defer_update_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        update_mask=UPDATE_MASK,
        environment=UPDATED_ENVIRONMENT,
        deferrable=True,
    )
    # [END howto_operator_update_composer_environment_deferrable_mode]

    # [START howto_operator_run_airflow_cli_command]
    run_airflow_cli_cmd = CloudComposerRunAirflowCLICommandOperator(
        task_id="run_airflow_cli_cmd",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        command=COMMAND,
    )
    # [END howto_operator_run_airflow_cli_command]

    # [START howto_operator_run_airflow_cli_command_deferrable_mode]
    defer_run_airflow_cli_cmd = CloudComposerRunAirflowCLICommandOperator(
        task_id="defer_run_airflow_cli_cmd",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        command=COMMAND,
        deferrable=True,
    )
    # [END howto_operator_run_airflow_cli_command_deferrable_mode]

    # [START howto_sensor_dag_run]
    dag_run_sensor = CloudComposerDAGRunSensor(
        task_id="dag_run_sensor",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
        composer_dag_id="airflow_monitoring",
        allowed_states=["success"],
    )
    # [END howto_sensor_dag_run]

    # [START howto_sensor_dag_run_deferrable_mode]
    defer_dag_run_sensor = CloudComposerDAGRunSensor(
        task_id="defer_dag_run_sensor",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        composer_dag_id="airflow_monitoring",
        allowed_states=["success"],
        deferrable=True,
    )
    # [END howto_sensor_dag_run_deferrable_mode]

    # [START howto_operator_delete_composer_environment]
    delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="delete_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID,
    )
    # [END howto_operator_delete_composer_environment]
    delete_env.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_delete_composer_environment_deferrable_mode]
    defer_delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="defer_delete_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        deferrable=True,
    )
    # [END howto_operator_delete_composer_environment_deferrable_mode]
    defer_delete_env.trigger_rule = TriggerRule.ALL_DONE

    chain(
        image_versions,
        [create_env, defer_create_env],
        list_envs,
        get_env,
        [update_env, defer_update_env],
        [run_airflow_cli_cmd, defer_run_airflow_cli_cmd],
        [dag_run_sensor, defer_dag_run_sensor],
        [delete_env, defer_delete_env],
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
