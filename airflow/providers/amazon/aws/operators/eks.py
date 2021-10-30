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
import warnings
from time import sleep
from typing import Dict, Iterable, List, Optional

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, EKSHook, FargateProfileStates
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

CHECK_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 25 * 60
DEFAULT_COMPUTE_TYPE = 'nodegroup'
DEFAULT_CONN_ID = 'aws_default'
DEFAULT_FARGATE_PROFILE_NAME = 'profile'
DEFAULT_NAMESPACE_NAME = 'default'
DEFAULT_NODEGROUP_NAME = 'nodegroup'
DEFAULT_POD_NAME = 'pod'

ABORT_MSG = "{compute} are still active after the allocated time limit.  Aborting."
CAN_NOT_DELETE_MSG = "A cluster can not be deleted with attached {compute}.  Deleting {count} {compute}."
MISSING_ARN_MSG = "Creating an {compute} requires {requirement} to be passed in."
SUCCESS_MSG = "No {compute} remain, deleting cluster."

SUPPORTED_COMPUTE_VALUES = frozenset({'nodegroup', 'fargate'})
NODEGROUP_FULL_NAME = 'Amazon EKS managed node groups'
FARGATE_FULL_NAME = 'AWS Fargate profiles'


class EKSCreateClusterOperator(BaseOperator):
    """
    Creates an Amazon EKS Cluster control plane.

    Optionally, can also create the supporting compute architecture:

     - If argument 'compute' is provided with a value of 'nodegroup', will also
         attempt to create an Amazon EKS Managed Nodegroup for the cluster.
         See :class:`~airflow.providers.amazon.aws.operators.EKSCreateNodegroupOperator`
         documentation for requirements.

    -  If argument 'compute' is provided with a value of 'fargate', will also attempt to create an AWS
         Fargate profile for the cluster.
         See :class:`~airflow.providers.amazon.aws.operators.EKSCreateFargateProfileOperator`
         documentation for requirements.


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

    If compute is assigned the value of 'nodegroup', the following are required:

    :param nodegroup_name: The unique name to give your Amazon EKS managed node group. (templated)
    :type nodegroup_name: str
    :param nodegroup_role_arn: The Amazon Resource Name (ARN) of the IAM role to associate with the
         Amazon EKS managed node group. (templated)
    :type nodegroup_role_arn: str

    If compute is assigned the value of 'fargate', the following are required:

    :param fargate_profile_name: The unique name to give your AWS Fargate profile. (templated)
    :type fargate_profile_name: str
    :param fargate_pod_execution_role_arn: The Amazon Resource Name (ARN) of the pod execution role to
         use for pods that match the selectors in the AWS Fargate profile. (templated)
    :type podExecutionRoleArn: str
    :param selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
    :type selectors: List

    """

    template_fields: Iterable[str] = (
        "cluster_name",
        "cluster_role_arn",
        "resources_vpc_config",
        "compute",
        "nodegroup_name",
        "nodegroup_role_arn",
        "fargate_profile_name",
        "fargate_pod_execution_role_arn",
        "fargate_selectors",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: str,
        resources_vpc_config: Dict,
        compute: Optional[str] = DEFAULT_COMPUTE_TYPE,
        nodegroup_name: Optional[str] = DEFAULT_NODEGROUP_NAME,
        nodegroup_role_arn: Optional[str] = None,
        fargate_profile_name: Optional[str] = DEFAULT_FARGATE_PROFILE_NAME,
        fargate_pod_execution_role_arn: Optional[str] = None,
        fargate_selectors: Optional[List] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        if compute:
            if compute not in SUPPORTED_COMPUTE_VALUES:
                raise ValueError("Provided compute type is not supported.")
            elif (compute == 'nodegroup') and not nodegroup_role_arn:
                raise ValueError(
                    MISSING_ARN_MSG.format(compute=NODEGROUP_FULL_NAME, requirement='nodegroup_role_arn')
                )
            elif (compute == 'fargate') and not fargate_pod_execution_role_arn:
                raise ValueError(
                    MISSING_ARN_MSG.format(
                        compute=FARGATE_FULL_NAME, requirement='fargate_pod_execution_role_arn'
                    )
                )

        self.compute = compute
        self.cluster_name = cluster_name
        self.cluster_role_arn = cluster_role_arn
        self.resources_vpc_config = resources_vpc_config
        self.nodegroup_name = nodegroup_name
        self.nodegroup_role_arn = nodegroup_role_arn
        self.fargate_profile_name = fargate_profile_name
        self.fargate_pod_execution_role_arn = fargate_pod_execution_role_arn
        self.fargate_selectors = fargate_selectors or [{"namespace": DEFAULT_NAMESPACE_NAME}]
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
        elif self.compute == 'fargate':
            eks_hook.create_fargate_profile(
                clusterName=self.cluster_name,
                fargateProfileName=self.fargate_profile_name,
                podExecutionRoleArn=self.fargate_pod_execution_role_arn,
                selectors=self.fargate_selectors,
            )


class EKSCreateNodegroupOperator(BaseOperator):
    """
    Creates an Amazon EKS managed node group for an existing Amazon EKS Cluster.

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
        nodegroup_name: Optional[str] = DEFAULT_NODEGROUP_NAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_subnets = nodegroup_subnets
        self.nodegroup_role_arn = nodegroup_role_arn
        self.nodegroup_name = nodegroup_name
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


class EKSCreateFargateProfileOperator(BaseOperator):
    """
    Creates an AWS Fargate profile for an Amazon EKS cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSCreateFargateProfileOperator`

    :param cluster_name: The name of the Amazon EKS cluster to apply the AWS Fargate profile to. (templated)
    :type cluster_name: str
    :param pod_execution_role_arn: The Amazon Resource Name (ARN) of the pod execution role to
         use for pods that match the selectors in the AWS Fargate profile. (templated)
    :type pod_execution_role_arn: str
    :param selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
    :type selectors: List
    :param fargate_profile_name: The unique name to give your AWS Fargate profile. (templated)
    :type fargate_profile_name: str

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
        "pod_execution_role_arn",
        "selectors",
        "fargate_profile_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        pod_execution_role_arn: str,
        selectors: List,
        fargate_profile_name: Optional[str] = DEFAULT_FARGATE_PROFILE_NAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.pod_execution_role_arn = pod_execution_role_arn
        self.selectors = selectors
        self.fargate_profile_name = fargate_profile_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_fargate_profile(
            clusterName=self.cluster_name,
            fargateProfileName=self.fargate_profile_name,
            podExecutionRoleArn=self.pod_execution_role_arn,
            selectors=self.selectors,
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
            self.delete_any_nodegroups(eks_hook)
            self.delete_any_fargate_profiles(eks_hook)

        eks_hook.delete_cluster(name=self.cluster_name)

    def delete_any_nodegroups(self, eks_hook) -> None:
        """
        Deletes all Amazon EKS managed node groups for a provided Amazon EKS Cluster.

        Amazon EKS managed node groups can be deleted in parallel, so we can send all
        of the delete commands in bulk and move on once the count of nodegroups is zero.
        """
        nodegroups = eks_hook.list_nodegroups(clusterName=self.cluster_name)
        if nodegroups:
            self.log.info(CAN_NOT_DELETE_MSG.format(compute=NODEGROUP_FULL_NAME, count=len(nodegroups)))
            for group in nodegroups:
                eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=group)

            # Scaling up the timeout based on the number of nodegroups that are being processed.
            additional_seconds = 5 * 60
            countdown = TIMEOUT_SECONDS + (len(nodegroups) * additional_seconds)
            while eks_hook.list_nodegroups(clusterName=self.cluster_name):
                if countdown >= CHECK_INTERVAL_SECONDS:
                    countdown -= CHECK_INTERVAL_SECONDS
                    sleep(CHECK_INTERVAL_SECONDS)
                    self.log.info(
                        "Waiting for the remaining %s nodegroups to delete.  "
                        "Checking again in %d seconds.",
                        len(nodegroups),
                        CHECK_INTERVAL_SECONDS,
                    )
                else:
                    raise RuntimeError(ABORT_MSG.format(compute=NODEGROUP_FULL_NAME))
        self.log.info(SUCCESS_MSG.format(compute=NODEGROUP_FULL_NAME))

    def delete_any_fargate_profiles(self, eks_hook) -> None:
        """
        Deletes all EKS Fargate profiles for a provided Amazon EKS Cluster.

        EKS Fargate profiles must be deleted one at a time, so we must wait
        for one to be deleted before sending the next delete command.
        """
        fargate_profiles = eks_hook.list_fargate_profiles(clusterName=self.cluster_name)
        if fargate_profiles:
            self.log.info(CAN_NOT_DELETE_MSG.format(compute=FARGATE_FULL_NAME, count=len(fargate_profiles)))
            for profile in fargate_profiles:
                # The API will return a (cluster) ResourceInUseException if you try
                # to delete Fargate profiles in parallel the way we can with nodegroups,
                # so each must be deleted sequentially
                eks_hook.delete_fargate_profile(clusterName=self.cluster_name, fargateProfileName=profile)

                countdown = TIMEOUT_SECONDS
                while (
                    eks_hook.get_fargate_profile_state(
                        clusterName=self.cluster_name, fargateProfileName=profile
                    )
                    != FargateProfileStates.NONEXISTENT
                ):
                    if countdown >= CHECK_INTERVAL_SECONDS:
                        countdown -= CHECK_INTERVAL_SECONDS
                        sleep(CHECK_INTERVAL_SECONDS)
                        self.log.info(
                            "Waiting for the AWS Fargate profile %s to delete.  "
                            "Checking again in %d seconds.",
                            profile,
                            CHECK_INTERVAL_SECONDS,
                        )
                    else:
                        raise RuntimeError(ABORT_MSG.format(compute=FARGATE_FULL_NAME))
        self.log.info(SUCCESS_MSG.format(compute=FARGATE_FULL_NAME))


class EKSDeleteNodegroupOperator(BaseOperator):
    """
    Deletes an Amazon EKS managed node group from an Amazon EKS Cluster.

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


class EKSDeleteFargateProfileOperator(BaseOperator):
    """
    Deletes an AWS Fargate profile from an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EKSDeleteFargateProfileOperator`

    :param cluster_name: The name of the Amazon EKS cluster associated with your Fargate profile. (templated)
    :type cluster_name: str
    :param fargate_profile_name: The name of the AWS Fargate profile to delete. (templated)
    :type fargate_profile_name: str
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
        "fargate_profile_name",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.aws_conn_id = aws_conn_id
        self.region = region

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_fargate_profile(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )


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
    :param pod_name: The unique name to give the pod. (templated)
    :type pod_name: str
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
        "pod_name",
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
        pod_context: str = None,
        pod_name: str = None,
        pod_username: str = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None:
        if pod_name is None:
            warnings.warn(
                "Default value of pod name is deprecated. "
                "We recommend that you pass pod name explicitly. ",
                DeprecationWarning,
                stacklevel=2,
            )
            pod_name = DEFAULT_POD_NAME

        self.cluster_name = cluster_name
        self.in_cluster = in_cluster
        self.namespace = namespace
        self.pod_name = pod_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(
            in_cluster=self.in_cluster,
            namespace=self.namespace,
            name=self.pod_name,
            **kwargs,
        )
        if pod_username:
            warnings.warn(
                "This pod_username parameter is deprecated, because changing the value does not make any "
                "visible changes to the user.",
                DeprecationWarning,
                stacklevel=2,
            )
        if pod_context:
            warnings.warn(
                "This pod_context parameter is deprecated, because changing the value does not make any "
                "visible changes to the user.",
                DeprecationWarning,
                stacklevel=2,
            )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the EKSPodOperator.
        if self.config_file:
            raise AirflowException("The config_file is not an allowed parameter for the EKSPodOperator.")

    def execute(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        with eks_hook.generate_config_file(
            eks_cluster_name=self.cluster_name, pod_namespace=self.namespace
        ) as self.config_file:
            return super().execute(context)
