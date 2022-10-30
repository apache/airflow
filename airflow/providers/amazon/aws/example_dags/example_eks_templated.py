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

# mypy ignore arg types (for templated fields)
# type: ignore[arg-type]


# Example Jinja Template format, substitute your values:
"""
{
    "cluster_name": "templated-cluster",
    "cluster_role_arn": "arn:aws:iam::123456789012:role/role_name",
    "resources_vpc_config": {
        "subnetIds": ["subnet-12345ab", "subnet-67890cd"],
        "endpointPublicAccess": true,
        "endpointPrivateAccess": false
    },
    "nodegroup_name": "templated-nodegroup",
    "nodegroup_subnets": "['subnet-12345ab', 'subnet-67890cd']",
    "nodegroup_role_arn": "arn:aws:iam::123456789012:role/role_name"
}
"""

with DAG(
    dag_id="example_eks_templated",
    start_date=datetime(2021, 1, 1),
    tags=["example", "templated"],
    catchup=False,
    # render_template_as_native_obj=True is what converts the Jinja to Python objects, instead of a string.
    render_template_as_native_obj=True,
) as dag:

    CLUSTER_NAME = "{{ dag_run.conf['cluster_name'] }}"
    NODEGROUP_NAME = "{{ dag_run.conf['nodegroup_name'] }}"

    # Create an Amazon EKS Cluster control plane without attaching a compute service.
    create_cluster = EksCreateClusterOperator(
        task_id="create_eks_cluster",
        cluster_name=CLUSTER_NAME,
        compute=None,
        cluster_role_arn="{{ dag_run.conf['cluster_role_arn'] }}",
        # This only works with render_template_as_native_obj flag (this dag has it set)
        resources_vpc_config="{{ dag_run.conf['resources_vpc_config'] }}",  # type: ignore[arg-type]
    )

    await_create_cluster = EksClusterStateSensor(
        task_id="wait_for_create_cluster",
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.ACTIVE,
    )

    create_nodegroup = EksCreateNodegroupOperator(
        task_id="create_eks_nodegroup",
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        nodegroup_subnets="{{ dag_run.conf['nodegroup_subnets'] }}",
        nodegroup_role_arn="{{ dag_run.conf['nodegroup_role_arn'] }}",
    )

    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="wait_for_create_nodegroup",
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state=NodegroupStates.ACTIVE,
    )

    start_pod = EksPodOperator(
        task_id="run_pod",
        cluster_name=CLUSTER_NAME,
        pod_name="run_pod",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "ls"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )

    delete_nodegroup = EksDeleteNodegroupOperator(
        task_id="delete_eks_nodegroup",
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
    )

    await_delete_nodegroup = EksNodegroupStateSensor(
        task_id="wait_for_delete_nodegroup",
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state=NodegroupStates.NONEXISTENT,
    )

    delete_cluster = EksDeleteClusterOperator(
        task_id="delete_eks_cluster",
        cluster_name=CLUSTER_NAME,
    )

    await_delete_cluster = EksClusterStateSensor(
        task_id="wait_for_delete_cluster",
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.NONEXISTENT,
    )

    (
        create_cluster
        >> await_create_cluster
        >> create_nodegroup
        >> await_create_nodegroup
        >> start_pod
        >> delete_nodegroup
        >> await_delete_nodegroup
        >> delete_cluster
        >> await_delete_cluster
    )
