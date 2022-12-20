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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, FargateProfileStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateFargateProfileOperator,
    EksDeleteClusterOperator,
    EksDeleteFargateProfileOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksFargateProfileStateSensor

# Ignore missing args provided by default_args
# type: ignore[call-arg]
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests.system.providers.amazon.aws.utils.ec2 import (
    create_address_allocation,
    create_nat_gateway,
    create_private_subnets,
    create_route_table,
    delete_nat_gateway,
    delete_route_table,
    delete_subnets,
    get_default_vpc_id,
    remove_address_allocation,
)

DAG_ID = "example_eks_with_fargate_profile"

# Externally fetched variables:
# See https://docs.aws.amazon.com/eks/latest/userguide/service_IAM_role.html
CLUSTER_ROLE_ARN_KEY = "CLUSTER_ROLE_ARN"
# See https://docs.aws.amazon.com/eks/latest/userguide/pod-execution-role.html
FARGATE_POD_ROLE_ARN_KEY = "FARGATE_POD_ROLE_ARN"
PUBLIC_SUBNET_ID_KEY = "PUBLIC_SUBNET_ID"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(CLUSTER_ROLE_ARN_KEY)
    .add_variable(FARGATE_POD_ROLE_ARN_KEY)
    .add_variable(PUBLIC_SUBNET_ID_KEY)
    .build()
)

SELECTORS = [{"namespace": "default"}]

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    cluster_role_arn = test_context[CLUSTER_ROLE_ARN_KEY]
    fargate_pod_role_arn = test_context[FARGATE_POD_ROLE_ARN_KEY]
    public_subnet_id = test_context[PUBLIC_SUBNET_ID_KEY]

    cluster_name = f"{env_id}-cluster"
    fargate_profile_name = f"{env_id}-profile"
    test_name = f"{env_id}_{DAG_ID}"

    vpc_id = get_default_vpc_id()
    allocation = create_address_allocation()
    nat_gateway = create_nat_gateway(allocation_id=allocation, subnet_id=public_subnet_id)
    route_table = create_route_table(vpc_id=vpc_id, nat_gateway_id=nat_gateway, test_name=test_name)
    subnets = create_private_subnets(
        vpc_id=vpc_id, route_table_id=route_table, test_name=test_name, number_to_make=2
    )

    # Create an Amazon EKS Cluster control plane without attaching a compute service.
    create_cluster = EksCreateClusterOperator(
        task_id="create_eks_cluster",
        cluster_name=cluster_name,
        cluster_role_arn=cluster_role_arn,
        resources_vpc_config={
            "subnetIds": subnets,
            "endpointPublicAccess": True,
            "endpointPrivateAccess": False,
        },
        compute=None,
    )

    await_create_cluster = EksClusterStateSensor(
        task_id="wait_for_create_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.ACTIVE,
    )

    # [START howto_operator_eks_create_fargate_profile]
    create_fargate_profile = EksCreateFargateProfileOperator(
        task_id="create_eks_fargate_profile",
        cluster_name=cluster_name,
        pod_execution_role_arn=fargate_pod_role_arn,
        fargate_profile_name=fargate_profile_name,
        selectors=SELECTORS,
    )
    # [END howto_operator_eks_create_fargate_profile]

    # [START howto_sensor_eks_fargate]
    await_create_fargate_profile = EksFargateProfileStateSensor(
        task_id="wait_for_create_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
        target_state=FargateProfileStates.ACTIVE,
    )
    # [END howto_sensor_eks_fargate]

    start_pod = EksPodOperator(
        task_id="run_pod",
        cluster_name=cluster_name,
        pod_name="run_pod",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
        startup_timeout_seconds=200,
    )

    # [START howto_operator_eks_delete_fargate_profile]
    delete_fargate_profile = EksDeleteFargateProfileOperator(
        task_id="delete_eks_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
    )
    # [END howto_operator_eks_delete_fargate_profile]
    delete_fargate_profile.trigger_rule = TriggerRule.ALL_DONE

    await_delete_fargate_profile = EksFargateProfileStateSensor(
        task_id="wait_for_delete_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
        target_state=FargateProfileStates.NONEXISTENT,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_cluster = EksDeleteClusterOperator(
        task_id="delete_eks_cluster",
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    await_delete_cluster = EksClusterStateSensor(
        task_id="wait_for_delete_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.NONEXISTENT,
    )

    chain(
        # TEST SETUP
        test_context,
        nat_gateway,
        route_table,
        subnets,
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_fargate_profile,
        await_create_fargate_profile,
        start_pod,
        delete_fargate_profile,
        await_delete_fargate_profile,
        delete_cluster,
        await_delete_cluster,
        # TEST TEARDOWN
        delete_subnets(subnets),
        delete_route_table(route_table),
        delete_nat_gateway(nat_gateway),
        remove_address_allocation(allocation),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
