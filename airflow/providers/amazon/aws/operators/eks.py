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
from typing import Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import DEFAULT_CONTEXT_NAME, DEFAULT_POD_USERNAME, EKSHook
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

    :param cluster_name: The unique name to give to your Amazon EKS Cluster.
    :type cluster_name: str
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions for the
       Kubernetes control plane to make calls to AWS API operations on your behalf.
    :type cluster_role_arn: str
    :param resources_vpc_config: The VPC configuration used by the cluster control plane.
    :type resources_vpc_config: Dict
    :param compute: The type of compute architecture to generate along with the cluster.
        Defaults to 'nodegroup' to generate an EKS Managed Nodegroup.
    :type compute: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param region: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    If compute is assigned the value of ``nodegroup``, the following are required:

    :param nodegroup_name: The unique name to give your EKS Managed Nodegroup.
    :type nodegroup_name: str
    :param nodegroup_role_arn: The Amazon Resource Name (ARN) of the IAM role to associate
         with the EKS Managed Nodegroup.
    :type nodegroup_role_arn: str

    """

    template_fields = (
        "cluster_name",
        "cluster_role_arn",
        "resources_vpc_config",
        # "nodegroup_name",
        # "nodegroup_role_arn",
        "compute",
        # "aws_conn_id",
        # "region",
    )

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: str,
        resources_vpc_config: Dict,
        nodegroup_name: Optional[str] = None,
        nodegroup_role_arn: Optional[str] = None,
        compute: Optional[str] = DEFAULT_COMPUTE_TYPE,
        aws_conn_id: Optional[str] = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.cluster_role_arn = cluster_role_arn
        self.resources_vpc_config = resources_vpc_config
        self.compute = compute
        self.aws_conn_id = aws_conn_id
        self.region = region

        if self.compute == 'nodegroup':
            self.nodegroup_name = nodegroup_name or self.cluster_name + DEFAULT_NODEGROUP_NAME_SUFFIX
            if nodegroup_role_arn:
                self.nodegroup_role_arn = nodegroup_role_arn
            else:
                message = "Creating an EKS Managed Nodegroup requires nodegroup_role_arn to be passed in."
                self.log.error(message)
                raise AttributeError(message)

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

        if self.compute is not None:
            self.log.info("Waiting for EKS Cluster to provision.  This will take some time.")

            countdown = TIMEOUT_SECONDS
            while eks_hook.get_cluster_state(clusterName=self.cluster_name) != "ACTIVE":
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

    :param cluster_name: The name of the Amazon EKS Cluster to create the managed nodegroup in.
    :type cluster_name: str
    :param nodegroup_name: The unique name to give your managed nodegroup.
    :type nodegroup_name: str
    :param nodegroup_subnets:
        The subnets to use for the Auto Scaling group that is created for the managed nodegroup.
    :type nodegroup_subnets: List[str]
    :param nodegroup_role_arn:
        The Amazon Resource Name (ARN) of the IAM role to associate with the managed nodegroup.
    :type nodegroup_role_arn: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
        :param region: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields = (
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
        nodegroup_name: Optional[str],
        aws_conn_id: Optional[str] = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.nodegroup_subnets = nodegroup_subnets
        self.nodegroup_role_arn = nodegroup_role_arn
        self.nodegroup_name = nodegroup_name or cluster_name + datetime.now().strftime("%Y%m%d_%H%M%S")
        self.aws_conn_id = aws_conn_id
        self.region = region

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

    :param cluster_name: The name of the Amazon EKS Cluster to delete.
    :type cluster_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
        :param region: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields = (
        "cluster_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        aws_conn_id: Optional[str] = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.aws_conn_id = aws_conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        nodegroups = eks_hook.list_nodegroups(clusterName=self.cluster_name)
        nodegroup_count = len(nodegroups)
        if nodegroup_count > 0:
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
                        "Waiting for the remaining %s nodegroups to delete.  Checking again in %d seconds.",
                        nodegroup_count,
                        CHECK_INTERVAL_SECONDS,
                    )
                else:
                    message = "Nodegroups are still inactive after the allocated time limit.  Aborting."
                    self.log.error(message)
                    raise RuntimeError(message)

        self.log.info("No nodegroups remain, deleting cluster.")
        eks_hook.delete_cluster(name=self.cluster_name)


class EKSDeleteNodegroupOperator(BaseOperator):
    """
    Deletes an Amazon EKS Nodegroup from an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDeleteNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster that is associated with your nodegroup.
    :type cluster_name: str
    :param nodegroup_name: The name of the nodegroup to delete.
    :type nodegroup_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used.  If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param region: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str

    """

    template_fields = (
        "cluster_name",
        "nodegroup_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        aws_conn_id: Optional[str] = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.aws_conn_id = aws_conn_id
        self.region = region

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

    :param cluster_name: The name of the Amazon EKS Cluster to execute the task on.
    :type cluster_name: str
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
       for the Kubernetes control plane to make calls to AWS API operations on your behalf.
    :type cluster_role_arn: str
    :param in_cluster: If True, look for config inside the cluster; if False look for a local file path.
    :type in_cluster: bool
    :param namespace: The namespace in which to execute the pod.
    :type namespace: str
    :param pod_context: The security context to use while executing the pod.
    :type pod_context: str
    :param pod_name: The unique name to give the pod.
    :type pod_name: str
    :param pod_username: The username to use while executing the pod.
    :type pod_username: str
    :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
    :param aws_profile: str
    :param region: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    """

    template_fields = (
        "role_arn",
        "cluster_name",
        "pod_name",
        "pod_username",
        "pod_context",
        "region",
        "aws_conn_id",
        "namespace",
    )

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: Optional[str] = None,
        # Setting in_cluster to False tells the pod that the config
        # file is stored locally in the worker and not in the cluster.
        in_cluster: Optional[bool] = False,
        namespace: Optional[str] = DEFAULT_NAMESPACE_NAME,
        pod_context: Optional[str] = DEFAULT_CONTEXT_NAME,
        pod_name: Optional[str] = DEFAULT_POD_NAME,
        pod_username: Optional[str] = DEFAULT_POD_USERNAME,
        region: Optional[str] = None,
        aws_conn_id: Optional[str] = DEFAULT_CONN_ID,
        **kwargs,
    ) -> None:
        super().__init__(
            in_cluster=in_cluster,
            namespace=namespace,
            name=pod_name,
            **kwargs,
        )
        self.role_arn = cluster_role_arn
        self.cluster_name = cluster_name
        self.pod_name = pod_name
        self.pod_username = pod_username
        self.pod_context = pod_context
        self.region = region
        self.aws_conn_id = aws_conn_id

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
