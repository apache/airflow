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
from airflow.providers.amazon.aws.sensors.eks import (
    EksClusterStateSensor,
    EksFargateProfileStateSensor,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from providers.tests.system.amazon.aws.utils.k8s import get_describe_pod_operator

DAG_ID = "example_eks_with_fargate_profile"

# Externally fetched variables:
# See https://docs.aws.amazon.com/eks/latest/userguide/service_IAM_role.html
CLUSTER_ROLE_ARN_KEY = "CLUSTER_ROLE_ARN"
# See https://docs.aws.amazon.com/eks/latest/userguide/pod-execution-role.html
FARGATE_POD_ROLE_ARN_KEY = "FARGATE_POD_ROLE_ARN"
SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(CLUSTER_ROLE_ARN_KEY)
    .add_variable(FARGATE_POD_ROLE_ARN_KEY)
    .add_variable(SUBNETS_KEY, split_string=True)
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
    subnets = test_context[SUBNETS_KEY]

    cluster_name = f"{env_id}-cluster"
    fargate_profile_name = f"{env_id}-profile"
    test_name = f"{env_id}_{DAG_ID}"

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
        # Keep the pod alive, so we can describe it in case of trouble. It's deleted with the cluster anyway.
        on_finish_action="keep_pod",
        startup_timeout_seconds=200,
    )

    describe_pod = get_describe_pod_operator(
        cluster_name, pod_name="{{ ti.xcom_pull(key='pod_name', task_ids='run_pod') }}"
    )
    # only describe the pod if the task above failed, to help diagnose
    describe_pod.trigger_rule = TriggerRule.ONE_FAILED

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
        poke_interval=10,
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
        poke_interval=10,
    )

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_fargate_profile,
        await_create_fargate_profile,
        start_pod,
        # TEARDOWN
        describe_pod,
        delete_fargate_profile,  # part of the test AND teardown
        await_delete_fargate_profile,
        delete_cluster,
        await_delete_cluster,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
