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

# pylint: disable=invalid-name
"""This module contains Amazon EKS operators."""
import json
import os
from datetime import datetime
from time import sleep
from typing import Dict, List, Optional

from boto3 import Session

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import (
    DEFAULT_CONTEXT_NAME,
    DEFAULT_KUBE_CONFIG_PATH,
    DEFAULT_NAMESPACE_NAME,
    DEFAULT_POD_USERNAME,
    EKSHook,
    generate_config_file,
)
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

CHECK_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 25 * 60
CONN_ID = "eks"
DEFAULT_COMPUTE_TYPE = 'nodegroup'
DEFAULT_NODEGROUP_NAME_SUFFIX = '-nodegroup'
DEFAULT_POD_NAME = 'pod'
KUBE_CONFIG_ENV_VAR = 'KUBECONFIG'


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

    If compute is assigned the value of ``nodegroup``, the following are required:

    :param nodegroup_name: The unique name to give your EKS Managed Nodegroup.
    :type nodegroup_name: str
    :param nodegroup_role_arn: The Amazon Resource Name (ARN) of the IAM role to associate
         with the EKS Managed Nodegroup.
    :type nodegroup_role_arn: str

    """

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: str,
        resources_vpc_config: Dict,
        nodegroup_name: Optional[str] = None,
        nodegroup_role_arn: Optional[str] = None,
        compute: Optional[str] = DEFAULT_COMPUTE_TYPE,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.clusterRoleArn = cluster_role_arn
        self.resourcesVpcConfig = resources_vpc_config
        self.compute = compute
        self.conn_id = conn_id
        self.region = region

        if self.compute == 'nodegroup':
            self.nodegroupName = nodegroup_name or self.clusterName + DEFAULT_NODEGROUP_NAME_SUFFIX
            if nodegroup_role_arn:
                self.nodegroupRoleArn = nodegroup_role_arn
            else:
                message = "Creating an EKS Managed Nodegroup requires nodegroup_role_arn to be passed in."
                self.log.error(message)
                raise AttributeError(message)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        eks_hook.create_cluster(
            name=self.clusterName,
            roleArn=self.clusterRoleArn,
            resourcesVpcConfig=self.resourcesVpcConfig,
        )

        if self.compute is not None:
            self.log.info("Waiting for EKS Cluster to provision.  This will take some time.")

            countdown = TIMEOUT_SECONDS
            while eks_hook.get_cluster_state(clusterName=self.clusterName) != "ACTIVE":
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
                    eks_hook.delete_cluster(name=self.clusterName)
                    raise RuntimeError(message)

        if self.compute == 'nodegroup':
            eks_hook.create_nodegroup(
                clusterName=self.clusterName,
                nodegroupName=self.nodegroupName,
                subnets=self.resourcesVpcConfig.get('subnetIds'),
                nodeRole=self.nodegroupRoleArn,
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

    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_subnets: List[str],
        nodegroup_role_arn: str,
        nodegroup_name: Optional[str],
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.nodegroupSubnets = nodegroup_subnets
        self.nodegroupRoleArn = nodegroup_role_arn
        self.nodegroupName = nodegroup_name or cluster_name + datetime.now().strftime("%Y%m%d_%H%M%S")
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        return eks_hook.create_nodegroup(
            clusterName=self.clusterName,
            nodegroupName=self.nodegroupName,
            subnets=self.nodegroupSubnets,
            nodeRole=self.nodegroupRoleArn,
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

    """

    def __init__(
        self, cluster_name: str, conn_id: Optional[str] = CONN_ID, region: Optional[str] = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        nodegroups = eks_hook.list_nodegroups(clusterName=self.clusterName).get('nodegroups')
        nodegroup_count = len(nodegroups)
        if nodegroup_count > 0:
            self.log.info(
                "A cluster can not be deleted with attached nodegroups.  Deleting %d nodegroups.",
                nodegroup_count,
            )
            for group in nodegroups:
                eks_hook.delete_nodegroup(clusterName=self.clusterName, nodegroupName=group)

            # Scaling up the timeout based on the number of nodegroups that are being processed.
            additional_seconds = 5 * 60
            countdown = TIMEOUT_SECONDS + (nodegroup_count * additional_seconds)
            while eks_hook.list_nodegroups(clusterName=self.clusterName).get('nodegroups'):
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
        return eks_hook.delete_cluster(name=self.clusterName)


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
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str

    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.nodegroupName = nodegroup_name
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        return eks_hook.delete_nodegroup(clusterName=self.clusterName, nodegroupName=self.nodegroupName)


class EKSDescribeAllClustersOperator(BaseOperator):
    """
    Describes all Amazon EKS Clusters in your AWS account.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        response = eks_hook.list_clusters(verbose=self.verbose)
        cluster_list = response.get('clusters')
        next_token = response.get('nextToken')

        result = []
        for cluster in cluster_list:
            full_describe = json.loads(eks_hook.describe_cluster(name=cluster))
            cluster_details = json.dumps(full_describe.get('cluster'))
            result.append(cluster_details)

        if self.verbose is True:
            self.log.info("\n\t".join(["Cluster Details:"] + result))

        return {'nextToken': next_token, 'clusters': result}


class EKSDescribeAllNodegroupsOperator(BaseOperator):
    """
    Describes all Amazon EKS Nodegroups associated with the specified EKS Cluster.

    :param cluster_name: The name of the Amazon EKS Cluster to check..
    :type cluster_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        cluster_name: str,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        response = eks_hook.list_nodegroups(clusterName=self.clusterName, verbose=self.verbose)
        nodegroup_list = response.get('nodegroups')
        next_token = response.get('nextToken')

        result = []
        for nodegroup in nodegroup_list:
            full_describe = json.loads(
                eks_hook.describe_nodegroup(clusterName=self.clusterName, nodegroupName=nodegroup)
            )
            nodegroup_details = json.dumps(full_describe.get('nodegroup'))
            result.append(nodegroup_details)

        if self.verbose is True:
            self.log.info("\n\t".join(["Nodegroup Details:"] + result))

        return {'nextToken': next_token, 'nodegroups': result}


class EKSDescribeClusterOperator(BaseOperator):
    """
    Returns descriptive information about an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDescribeClusterOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to describe.
    :type cluster_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        cluster_name: str,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        response = eks_hook.describe_cluster(name=self.clusterName, verbose=self.verbose)
        response_json = json.loads(response)
        # Extract the cluster data, drop the request metadata
        cluster_data = response_json.get('cluster')
        return json.dumps(cluster_data)


class EKSDescribeNodegroupOperator(BaseOperator):
    """
    Returns descriptive information about the Amazon EKS Nodegroup.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDescribeNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster associated with the nodegroup.
    :type cluster_name: str
    :param nodegroup_name: The name of the Amazon EKS Nodegroup to describe.
    :type nodegroup_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.nodegroupName = nodegroup_name
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        response = eks_hook.describe_nodegroup(
            clusterName=self.clusterName, nodegroupName=self.nodegroupName, verbose=self.verbose
        )
        response_json = json.loads(response)
        # Extract the nodegroup data, drop the request metadata
        nodegroup_data = response_json.get('nodegroup')
        return json.dumps(nodegroup_data)


class EKSListClustersOperator(BaseOperator):
    """
    Lists the Amazon EKS Clusters in your AWS account with optional pagination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSListClustersOperator`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        return eks_hook.list_clusters(verbose=self.verbose)


class EKSListNodegroupsOperator(BaseOperator):
    """
    Lists the Amazon EKS Nodegroups associated with the specified EKS Cluster with optional pagination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSListNodegroupsOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to check..
    :type cluster_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    :param verbose: Provides additional logging if set to True.  Defaults to False.
    :type verbose: bool

    """

    def __init__(
        self,
        cluster_name: str,
        verbose: Optional[bool] = False,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clusterName = cluster_name
        self.verbose = verbose
        self.conn_id = conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        return eks_hook.list_nodegroups(clusterName=self.clusterName, verbose=self.verbose)


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
    :param kube_config_file_path: Path to save the generated kube_config file to.
    :type kube_config_file_path: str
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
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str

    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        cluster_name: str,
        cluster_role_arn: Optional[str] = None,
        # A default path will be used if none is provided.
        kube_config_file_path: Optional[str] = os.environ.get(KUBE_CONFIG_ENV_VAR, DEFAULT_KUBE_CONFIG_PATH),
        # Setting in_cluster to False tells the pod that the config
        # file is stored locally in the worker and not in the cluster.
        in_cluster: Optional[bool] = False,
        namespace: Optional[str] = DEFAULT_NAMESPACE_NAME,
        pod_context: Optional[str] = DEFAULT_CONTEXT_NAME,
        pod_name: Optional[str] = DEFAULT_POD_NAME,
        pod_username: Optional[str] = DEFAULT_POD_USERNAME,
        aws_profile: Optional[str] = None,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            in_cluster=in_cluster,
            namespace=namespace,
            config_file=kube_config_file_path,
            name=pod_name,
            **kwargs,
        )
        self.aws_profile = os.getenv('AWS_PROFILE', None) if aws_profile is None else aws_profile
        self.roleArn = cluster_role_arn
        self.clusterName = cluster_name
        self.config_file = kube_config_file_path
        self.pod_username = pod_username
        self.pod_context = pod_context
        self.region = region

    def execute(self, context):
        generate_config_file(
            kube_config_file_location=self.config_file,
            eks_cluster_name=self.clusterName,
            eks_namespace_name=self.namespace,
            pod_username=self.pod_username,
            pod_context=self.pod_context,
            aws_region=self.region,
            aws_profile=self.aws_profile,
        )

        return super().execute(context)
