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

from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates
from airflow.providers.amazon.aws.operators.ecs import (
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.sensors.ecs import (
    EcsClusterStateSensor,
    EcsTaskDefinitionStateSensor,
)

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

DAG_ID = "example_ecs"

# Externally fetched variables:
EXISTING_CLUSTER_NAME_KEY = "CLUSTER_NAME"
EXISTING_CLUSTER_SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder()
    # NOTE:  Creating a functional ECS Cluster which uses EC2 requires manually creating
    # and configuring a number of resources such as autoscaling groups, networking
    # etc. which is out of scope for this demo and time-consuming for a system test
    # To simplify this demo and make it run in a reasonable length of time as a
    # system test, follow the steps below to create a new cluster on the AWS Console
    # which handles all asset creation and configuration using default values:
    # 1. https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters
    # 2. Select "EC2 Linux + Networking" and hit "Next"
    # 3. Name your cluster in the first field and click Create
    .add_variable(EXISTING_CLUSTER_NAME_KEY)
    .add_variable(EXISTING_CLUSTER_SUBNETS_KEY, split_string=True)
    .build()
)


@task
def get_region():
    return boto3.session.Session().region_name


@task(trigger_rule=TriggerRule.ALL_DONE)
def clean_logs(group_name: str):
    client = boto3.client("logs")
    client.delete_log_group(logGroupName=group_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    existing_cluster_name = test_context[EXISTING_CLUSTER_NAME_KEY]
    existing_cluster_subnets = test_context[EXISTING_CLUSTER_SUBNETS_KEY]

    new_cluster_name = f"{env_id}-cluster"
    container_name = f"{env_id}-container"
    family_name = f"{env_id}-task-definition"
    asg_name = f"{env_id}-asg"

    aws_region = get_region()
    log_group_name = f"/ecs_test/{env_id}"

    # [START howto_operator_ecs_create_cluster]
    create_cluster = EcsCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_operator_ecs_create_cluster]

    # EcsCreateClusterOperator waits by default, setting as False to test the Sensor below.
    create_cluster.wait_for_completion = False

    # [START howto_sensor_ecs_cluster_state]
    await_cluster = EcsClusterStateSensor(
        task_id="await_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_sensor_ecs_cluster_state]

    # [START howto_operator_ecs_register_task_definition]
    register_task = EcsRegisterTaskDefinitionOperator(
        task_id="register_task",
        family=family_name,
        container_definitions=[
            {
                "name": container_name,
                "image": "ubuntu",
                "workingDirectory": "/usr/bin",
                "entryPoint": ["sh", "-c"],
                "command": ["ls"],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": log_group_name,
                        "awslogs-region": aws_region,
                        "awslogs-create-group": "true",
                        "awslogs-stream-prefix": "ecs",
                    },
                },
            },
        ],
        register_task_kwargs={
            "cpu": "256",
            "memory": "512",
            "networkMode": "awsvpc",
        },
    )
    # [END howto_operator_ecs_register_task_definition]

    # [START howto_sensor_ecs_task_definition_state]
    await_task_definition = EcsTaskDefinitionStateSensor(
        task_id="await_task_definition",
        task_definition=register_task.output,
    )
    # [END howto_sensor_ecs_task_definition_state]

    # [START howto_operator_ecs_run_task]
    run_task = EcsRunTaskOperator(
        task_id="run_task",
        reattach=True,
        cluster=existing_cluster_name,
        task_definition=register_task.output,
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": ["echo hello world"],
                },
            ],
        },
        network_configuration={"awsvpcConfiguration": {"subnets": existing_cluster_subnets}},
        # [START howto_awslogs_ecs]
        awslogs_group=log_group_name,
        awslogs_region=aws_region,
        awslogs_stream_prefix=f"ecs/{container_name}",
        # [END howto_awslogs_ecs]
    )
    # [END howto_operator_ecs_run_task]
    # The default is 6 seconds between checks, which is very aggressive, setting to 60s to reduce throttling errors.
    run_task.waiter_delay = 60

    # [START howto_operator_ecs_deregister_task_definition]
    deregister_task = EcsDeregisterTaskDefinitionOperator(
        task_id="deregister_task",
        task_definition=register_task.output,
    )
    # [END howto_operator_ecs_deregister_task_definition]
    deregister_task.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_ecs_delete_cluster]
    delete_cluster = EcsDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_operator_ecs_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    # EcsDeleteClusterOperator waits by default, setting as False to test the Sensor below.
    delete_cluster.wait_for_completion = False

    # [START howto_operator_ecs_delete_cluster]
    await_delete_cluster = EcsClusterStateSensor(
        task_id="await_delete_cluster",
        cluster_name=new_cluster_name,
        target_state=EcsClusterStates.INACTIVE,
    )
    # [END howto_operator_ecs_delete_cluster]

    chain(
        # TEST SETUP
        test_context,
        aws_region,
        # TEST BODY
        create_cluster,
        await_cluster,
        register_task,
        await_task_definition,
        run_task,
        deregister_task,
        delete_cluster,
        await_delete_cluster,
        clean_logs(log_group_name),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
