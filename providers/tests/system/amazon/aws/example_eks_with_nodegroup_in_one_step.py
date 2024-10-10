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

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksDeleteClusterOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from providers.tests.system.amazon.aws.utils.k8s import get_describe_pod_operator

DAG_ID = "example_eks_with_nodegroup_in_one_step"

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
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    cluster_name = f"{env_id}-cluster"
    nodegroup_name = f"{env_id}-nodegroup"
    launch_template_name = f"{env_id}-launch-template"

    # [START howto_operator_eks_create_cluster_with_nodegroup]
    # Create an Amazon EKS cluster control plane and an EKS nodegroup compute platform in one step.
    create_cluster_and_nodegroup = EksCreateClusterOperator(
        task_id="create_cluster_and_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        cluster_role_arn=test_context[ROLE_ARN_KEY],
        # Opting to use the same ARN for the cluster and the nodegroup here,
        # but a different ARN could be configured and passed if desired.
        nodegroup_role_arn=test_context[ROLE_ARN_KEY],
        resources_vpc_config={"subnetIds": test_context[SUBNETS_KEY]},
        # ``compute='nodegroup'`` is the default, explicitly set here for demo purposes.
        compute="nodegroup",
        # The launch template enforces IMDSv2 and is required for internal
        # compliance when running these system tests on AWS infrastructure.
        create_nodegroup_kwargs={"launchTemplate": {"name": launch_template_name}},
    )
    # [END howto_operator_eks_create_cluster_with_nodegroup]

    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="await_create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
        poke_interval=10,
    )

    start_pod = EksPodOperator(
        task_id="start_pod",
        pod_name="test_pod",
        cluster_name=cluster_name,
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Keep the pod alive, so we can describe it in case of trouble. It's deleted with the cluster anyway.
        on_finish_action="keep_pod",
    )

    describe_pod = get_describe_pod_operator(
        cluster_name, pod_name="{{ ti.xcom_pull(key='pod_name', task_ids='run_pod') }}"
    )
    # only describe the pod if the task above failed, to help diagnose
    describe_pod.trigger_rule = TriggerRule.ONE_FAILED

    # [START howto_operator_eks_force_delete_cluster]
    # An Amazon EKS cluster can not be deleted with attached resources such as nodegroups or Fargate profiles.
    # Setting the `force` to `True` will delete any attached resources before deleting the cluster.
    delete_nodegroup_and_cluster = EksDeleteClusterOperator(
        task_id="delete_nodegroup_and_cluster",
        cluster_name=cluster_name,
        force_delete_compute=True,
    )
    # [END howto_operator_eks_force_delete_cluster]
    delete_nodegroup_and_cluster.trigger_rule = TriggerRule.ALL_DONE

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
        create_cluster_and_nodegroup,
        await_create_nodegroup,
        start_pod,
        # TEST TEARDOWN
        describe_pod,
        delete_nodegroup_and_cluster,
        await_delete_cluster,
        delete_launch_template(launch_template_name),
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
