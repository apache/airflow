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

"""This module contains Amazon EKS operators."""
from datetime import datetime
from time import sleep
from typing import Dict, Iterable, List, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import (
    DEFAULT_CONTEXT_NAME,
    DEFAULT_POD_USERNAME,
    ClusterStates,
    EKSHook,
)
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

CHECK_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 25 * 60
DEFAULT_COMPUTE_TYPE = 'nodegroup'
DEFAULT_CONN_ID = "aws_default"
DEFAULT_NAMESPACE_NAME = 'default'
DEFAULT_NODEGROUP_NAME_SUFFIX = '-nodegroup'
DEFAULT_POD_NAME = 'pod'


class EKSCreateClusterOperator(BaseOperator):
    """
    Creates an Amazon EKS Cluster control plane.

    Optionally, can also create the supporting compute architecture:
    If argument 'compute' is provided with a value of 'nodegroup', will also attempt to create an Amazon
    EKS Managed Nodegroup for the cluster.  See EKSCreateNodegroupOperator documentation for requirements.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSCreateClusterOperator`

    :param cluster_name: The unique name to give to your Amazon EKS Cluster. (templated)
    :type cluster_name: str
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions for the
       Kubernetes control plane to make calls to AWS API operations on your behalf. (templated)
    :type cluster_role_arn: str
    :param resources_vpc_config: The VPC configuration used by the cluster control plane. (templated)
    :type resources_vpc_config: Dict
    :param compute: The type of compute architecture to generate along with the cluster. (templated)
        Defaults to 'nodegroup' to generate an EKS Managed Nodegroup.
    :type compute: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    If compute is assigned the value of ``nodegroup``, the following are required:

    :param nodegroup_name: The unique name to give your EKS Managed Nodegroup. (templated)
    :type nodegroup_name: str
    :param nodegroup_role_arn: The Amazon Resource Name (ARN) of the IAM role to associate
         with the EKS Managed Nodegroup. (templated)
    :type nodegroup_role_arn: str

    """

    template_fields: Iterable[str] = (
        "cluster_name",
        "cluster_role_arn",
        "resources_vpc_config",
        "nodegroup_name",
        "nodegroup_role_arn",
        "compute",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: str,
        resources_vpc_config: Dict,
        nodegroup_name: Optional[str] = None,
        nodegroup_role_arn: Optional[str] = None,
        compute: Optional[str] = DEFAULT_COMPUTE_TYPE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.compute = compute
        if (self.compute == 'nodegroup') and not nodegroup_role_arn:
            raise ValueError("Creating an EKS Managed Nodegroup requires nodegroup_role_arn to be passed in.")
        self.cluster_name = cluster_name
        self.cluster_role_arn = cluster_role_arn
        self.resources_vpc_config = resources_vpc_config
        self.nodegroup_name = nodegroup_name or self.cluster_name + DEFAULT_NODEGROUP_NAME_SUFFIX
        self.nodegroup_role_arn = nodegroup_role_arn
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_cluster(
            name=self.cluster_name,
            roleArn=self.cluster_role_arn,
            resourcesVpcConfig=self.resources_vpc_config,
        )

        if not self.compute:
            return None

        self.log.info("Waiting for EKS Cluster to provision.  This will take some time.")

        countdown = TIMEOUT_SECONDS
        while eks_hook.get_cluster_state(clusterName=self.cluster_name) != ClusterStates.ACTIVE:
            if countdown >= CHECK_INTERVAL_SECONDS:
                countdown -= CHECK_INTERVAL_SECONDS
                self.log.info(
                    "Waiting for cluster to start.  Checking again in %d seconds", CHECK_INTERVAL_SECONDS
                )
                sleep(CHECK_INTERVAL_SECONDS)
            else:
                message = (
                    "Cluster is still inactive after the allocated time limit.  "
                    "Failed cluster will be torn down."
                )
                self.log.error(message)
                # If there is something preventing the cluster for activating, tear it down and abort.
                eks_hook.delete_cluster(name=self.cluster_name)
                raise RuntimeError(message)

        if self.compute == 'nodegroup':
            eks_hook.create_nodegroup(
                clusterName=self.cluster_name,
                nodegroupName=self.nodegroup_name,
                subnets=self.resources_vpc_config.get('subnetIds'),
                nodeRole=self.nodegroup_role_arn,
            )


class EKSCreateNodegroupOperator(BaseOperator):
    """
    Creates am Amazon EKS Managed Nodegroup for an existing Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSCreateNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to create the managed nodegroup in. (templated)
    :type cluster_name: str
    :param nodegroup_name: The unique name to give your managed nodegroup. (templated)
    :type nodegroup_name: str
    :param nodegroup_subnets:
        The subnets to use for the Auto Scaling group that is created for the managed nodegroup. (templated)
    :type nodegroup_subnets: List[str]
    :param nodegroup_role_arn:
        The Amazon Resource Name (ARN) of the IAM role to associate with the managed nodegroup. (templated)
    :type nodegroup_role_arn: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
        :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields: Iterable[str] = (
        "cluster_name",
        "nodegroup_subnets",
        "nodegroup_role_arn",
        "nodegroup_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        nodegroup_subnets: List[str],
        nodegroup_role_arn: str,
        nodegroup_name: Optional[str] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_subnets = nodegroup_subnets
        self.nodegroup_role_arn = nodegroup_role_arn
        self.nodegroup_name = nodegroup_name or cluster_name + datetime.now().strftime("%Y%m%d_%H%M%S")
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_nodegroup(
            clusterName=self.cluster_name,
            nodegroupName=self.nodegroup_name,
            subnets=self.nodegroup_subnets,
            nodeRole=self.nodegroup_role_arn,
        )


class EKSDeleteClusterOperator(BaseOperator):
    """
    Deletes the Amazon EKS Cluster control plane and all nodegroups attached to it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDeleteClusterOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to delete. (templated)
    :type cluster_name: str
    :param force_delete_compute: If True, will delete any attached resources. (templated)
         Defaults to False.
    :type force_delete_compute: bool
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields: Iterable[str] = (
        "cluster_name",
        "force_delete_compute",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        force_delete_compute: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.force_delete_compute = force_delete_compute
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        if self.force_delete_compute:
            nodegroups = eks_hook.list_nodegroups(clusterName=self.cluster_name)
            nodegroup_count = len(nodegroups)
            if nodegroup_count:
                self.log.info(
                    "A cluster can not be deleted with attached nodegroups.  Deleting %d nodegroups.",
                    nodegroup_count,
                )
                for group in nodegroups:
                    eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=group)

                # Scaling up the timeout based on the number of nodegroups that are being processed.
                additional_seconds = 5 * 60
                countdown = TIMEOUT_SECONDS + (nodegroup_count * additional_seconds)
                while eks_hook.list_nodegroups(clusterName=self.cluster_name):
                    if countdown >= CHECK_INTERVAL_SECONDS:
                        countdown -= CHECK_INTERVAL_SECONDS
                        sleep(CHECK_INTERVAL_SECONDS)
                        self.log.info(
                            "Waiting for the remaining %s nodegroups to delete.  "
                            "Checking again in %d seconds.",
                            nodegroup_count,
                            CHECK_INTERVAL_SECONDS,
                        )
                    else:
                        raise RuntimeError(
                            "Nodegroups are still inactive after the allocated time limit.  Aborting."
                        )
            self.log.info("No nodegroups remain, deleting cluster.")

        eks_hook.delete_cluster(name=self.cluster_name)


class EKSDeleteNodegroupOperator(BaseOperator):
    """
    Deletes an Amazon EKS Nodegroup from an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDeleteNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster associated with your nodegroup. (templated)
    :type cluster_name: str
    :param nodegroup_name: The name of the nodegroup to delete. (templated)
    :type nodegroup_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used.  If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields: Iterable[str] = (
        "cluster_name",
        "nodegroup_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=self.nodegroup_name)


class EKSPodOperator(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod on the specified Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSPodOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to execute the task on. (templated)
    :type cluster_name: str
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
       for the Kubernetes control plane to make calls to AWS API operations on your behalf. (templated)
    :type cluster_role_arn: str
    :param in_cluster: If True, look for config inside the cluster; if False look for a local file path.
    :type in_cluster: bool
    :param namespace: The namespace in which to execute the pod. (templated)
    :type namespace: str
    :param pod_context: The security context to use while executing the pod. (templated)
    :type pod_context: str
    :param pod_name: The unique name to give the pod. (templated)
    :type pod_name: str
    :param pod_username: The username to use while executing the pod. (templated)
    :type pod_username: str
    :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
    :param aws_profile: str
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    """

    template_fields: Iterable[str] = {
        "cluster_name",
        "in_cluster",
        "namespace",
        "pod_context",
        "pod_name",
        "pod_username",
        "aws_conn_id",
        "region",
    } | set(KubernetesPodOperator.template_fields)

    def __init__(
        self,
        cluster_name: str,
        # Setting in_cluster to False tells the pod that the config
        # file is stored locally in the worker and not in the cluster.
        in_cluster: bool = False,
        namespace: str = DEFAULT_NAMESPACE_NAME,
        pod_context: str = DEFAULT_CONTEXT_NAME,
        pod_name: str = DEFAULT_POD_NAME,
        pod_username: str = DEFAULT_POD_USERNAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.in_cluster = in_cluster
        self.namespace = namespace
        self.pod_context = pod_context
        self.pod_name = pod_name
        self.pod_username = pod_username
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(
            in_cluster=self.in_cluster,
            namespace=self.namespace,
            name=self.pod_name,
            **kwargs,
        )

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        with eks_hook.generate_config_file(
            eks_cluster_name=self.cluster_name,
            pod_namespace=self.namespace,
            pod_username=self.pod_username,
            pod_context=self.pod_context,
        ) as self.config_file:
            super().execute(context)
