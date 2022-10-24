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
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateNodegroupOperator,
    EksDeleteClusterOperator,
    EksDeleteNodegroupOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

# Ignore missing args provided by default_args
# type: ignore[call-arg]

DAG_ID = "example_eks_with_nodegroups"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).add_variable(SUBNETS_KEY, split_string=True).build()
)

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

    # [START howto_sensor_eks_nodegroup]
    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="await_create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
    )
    # [END howto_sensor_eks_nodegroup]

    # [START howto_operator_eks_pod_operator]
    start_pod = EksPodOperator(
        task_id="start_pod",
        pod_name="test_pod",
        cluster_name=cluster_name,
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )
    # [END howto_operator_eks_pod_operator]

    # [START howto_operator_eks_delete_nodegroup]
    delete_nodegroup = EksDeleteNodegroupOperator(
        task_id="delete_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
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
    )

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_nodegroup,
        await_create_nodegroup,
        start_pod,
        delete_nodegroup,
        await_delete_nodegroup,
        delete_cluster,
        await_delete_cluster,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
