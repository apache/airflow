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
from pendulum import duration

from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateNodegroupOperator,
    EksDeleteClusterOperator,
    EksDeleteNodegroupOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor

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
from system.amazon.aws.utils.k8s import get_describe_pod_operator

DAG_ID = "example_eks_with_nodegroups"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).add_variable(SUBNETS_KEY, split_string=True).build()
)


@task
def create_launch_template(template_name: str):
    # This launch template enables IMDSv2.
    boto3.client("ec2").create_launch_template(
        LaunchTemplateName=template_name,
        LaunchTemplateData={
            "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_launch_template(template_name: str):
    boto3.client("ec2").delete_launch_template(LaunchTemplateName=template_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    cluster_name = f"{env_id}-cluster"
    nodegroup_name = f"{env_id}-nodegroup"
    launch_template_name = f"{env_id}-launch-template"

    # [START howto_operator_eks_create_cluster]
    # Create an Amazon EKS Cluster control plane without attaching compute service.
    create_cluster = EksCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=cluster_name,
        cluster_role_arn=test_context[ROLE_ARN_KEY],
        resources_vpc_config={"subnetIds": test_context[SUBNETS_KEY]},
        compute=None,
    )
    # [END howto_operator_eks_create_cluster]

    # [START howto_sensor_eks_cluster]
    await_create_cluster = EksClusterStateSensor(
        task_id="await_create_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.ACTIVE,
    )
    # [END howto_sensor_eks_cluster]

    # [START howto_operator_eks_create_nodegroup]
    create_nodegroup = EksCreateNodegroupOperator(
        task_id="create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        nodegroup_subnets=test_context[SUBNETS_KEY],
        nodegroup_role_arn=test_context[ROLE_ARN_KEY],
    )
    # [END howto_operator_eks_create_nodegroup]
    # The launch template enforces IMDSv2 and is required for internal compliance
    # when running these system tests on AWS infrastructure.  It is not required
    # for the operator to work, so I'm placing it outside the demo snippet.
    create_nodegroup.create_nodegroup_kwargs = {"launchTemplate": {"name": launch_template_name}}

    # [START howto_sensor_eks_nodegroup]
    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="await_create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
    )
    # [END howto_sensor_eks_nodegroup]
    await_create_nodegroup.poke_interval = 10

    # [START howto_operator_eks_pod_operator]
    start_pod = EksPodOperator(
        task_id="start_pod",
        pod_name="test_pod",
        cluster_name=cluster_name,
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        on_finish_action="keep_pod",
    )
    # [END howto_operator_eks_pod_operator]
    # Keep the pod alive, so we can describe it in case of trouble. It's deleted with the cluster anyway.
    start_pod.is_delete_operator_pod = False

    # In this specific situation we want to keep the pod to be able to describe it,
    # it is cleaned anyway with the cluster later on.
    start_pod.is_delete_operator_pod = False

    describe_pod = get_describe_pod_operator(
        cluster_name, pod_name="{{ ti.xcom_pull(key='pod_name', task_ids='run_pod') }}"
    )
    # only describe the pod if the task above failed, to help diagnose
    describe_pod.trigger_rule = TriggerRule.ONE_FAILED

    # Wait for nodegroup to be in stable state before deletion
    await_nodegroup_stable = EksNodegroupStateSensor(
        task_id="await_nodegroup_stable",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
    )

    # [START howto_operator_eks_delete_nodegroup]
    delete_nodegroup = EksDeleteNodegroupOperator(
        task_id="delete_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        retries=4,
        retry_delay=duration(seconds=30),
        retry_exponential_backoff=True,
    )
    # [END howto_operator_eks_delete_nodegroup]
    delete_nodegroup.trigger_rule = TriggerRule.ALL_DONE

    await_delete_nodegroup = EksNodegroupStateSensor(
        task_id="await_delete_nodegroup",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.NONEXISTENT,
    )

    # Wait for cluster to be in stable state before deletion
    await_cluster_stable = EksClusterStateSensor(
        task_id="await_cluster_stable",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        target_state=ClusterStates.ACTIVE,
    )

    # [START howto_operator_eks_delete_cluster]
    delete_cluster = EksDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=cluster_name,
    )
    # [END howto_operator_eks_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    await_delete_cluster = EksClusterStateSensor(
        task_id="await_delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        target_state=ClusterStates.NONEXISTENT,
        poke_interval=10,
    )

    chain(
        # TEST SETUP
        test_context,
        create_launch_template(launch_template_name),
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_nodegroup,
        await_create_nodegroup,
        start_pod,
        # TEST TEARDOWN
        describe_pod,
        await_nodegroup_stable,
        delete_nodegroup,  # part of the test AND teardown
        await_delete_nodegroup,
        await_cluster_stable,
        delete_cluster,  # part of the test AND teardown
        await_delete_cluster,
        delete_launch_template(launch_template_name),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
