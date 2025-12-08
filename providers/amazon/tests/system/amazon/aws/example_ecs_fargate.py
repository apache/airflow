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

from datetime import datetime

import boto3

from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_ecs_fargate"

# Externally fetched variables:
SUBNETS_KEY = "SUBNETS"  # At least one public subnet is required.
SECURITY_GROUPS_KEY = "SECURITY_GROUPS"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(SUBNETS_KEY, split_string=True)
    .add_variable(SECURITY_GROUPS_KEY, split_string=True)
    .build()
)


@task
def create_cluster(cluster_name: str) -> None:
    """Creates an ECS cluster."""
    boto3.client("ecs").create_cluster(clusterName=cluster_name)


@task
def register_task_definition(task_name: str, container_name: str) -> str:
    """Creates a Task Definition."""
    response = boto3.client("ecs").register_task_definition(
        family=task_name,
        # CPU and Memory are required for Fargate and are set to the lowest currently allowed values.
        cpu="256",
        memory="512",
        containerDefinitions=[
            {
                "name": container_name,
                "image": "ubuntu",
                "workingDirectory": "/usr/bin",
                "entryPoint": ["sh", "-c"],
                "command": ["ls"],
            }
        ],
        requiresCompatibilities=["FARGATE"],
        networkMode="awsvpc",
    )

    return response["taskDefinition"]["taskDefinitionArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_task_definition(task_definition_arn: str) -> None:
    """Deletes the Task Definition."""
    boto3.client("ecs").deregister_task_definition(taskDefinition=task_definition_arn)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_cluster(cluster_name: str) -> None:
    """Deletes the ECS cluster."""
    boto3.client("ecs").delete_cluster(cluster=cluster_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    cluster_name = f"{env_id}-test-cluster"
    container_name = f"{env_id}-test-container"
    task_definition_name = f"{env_id}-test-definition"

    create_task_definition = register_task_definition(task_definition_name, container_name)

    # [START howto_operator_ecs]
    hello_world = EcsRunTaskOperator(
        task_id="hello_world",
        cluster=cluster_name,
        task_definition=task_definition_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": ["echo", "hello", "world"],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": test_context[SUBNETS_KEY],
                "securityGroups": test_context[SECURITY_GROUPS_KEY],
                "assignPublicIp": "ENABLED",
            },
        },
    )
    # [END howto_operator_ecs]

    # EcsRunTaskOperator waits by default, setting as False to test the Sensor below.
    hello_world.wait_for_completion = False
    # The default is 6 seconds between checks, which is very aggressive, setting to 60s to reduce throttling errors.
    hello_world.waiter_delay = 60

    # [START howto_sensor_ecs_task_state]
    # By default, EcsTaskStateSensor waits until the task has started, but the
    # demo task runs so fast that the sensor misses it.  This sensor instead
    # demonstrates how to wait until the ECS Task has completed by providing
    # the target_state and failure_states parameters.
    await_task_finish = EcsTaskStateSensor(
        task_id="await_task_finish",
        cluster=cluster_name,
        task=hello_world.output["ecs_task_arn"],
        target_state=EcsTaskStates.STOPPED,
        failure_states={EcsTaskStates.NONE},
    )
    # [END howto_sensor_ecs_task_state]

    chain(
        # TEST SETUP
        test_context,
        create_cluster(cluster_name),
        create_task_definition,
        # TEST BODY
        hello_world,
        await_task_finish,
        # TEST TEARDOWN
        delete_task_definition(create_task_definition),
        delete_cluster(cluster_name),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
