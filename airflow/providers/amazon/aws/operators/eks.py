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
from __future__ import annotations

import warnings
from ast import literal_eval
from typing import TYPE_CHECKING, Any, List, Sequence, cast

from botocore.exceptions import ClientError, WaiterError

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


CHECK_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 25 * 60
DEFAULT_COMPUTE_TYPE = "nodegroup"
DEFAULT_CONN_ID = "aws_default"
DEFAULT_FARGATE_PROFILE_NAME = "profile"
DEFAULT_NAMESPACE_NAME = "default"
DEFAULT_NODEGROUP_NAME = "nodegroup"

CAN_NOT_DELETE_MSG = "A cluster can not be deleted with attached {compute}.  Deleting {count} {compute}."
MISSING_ARN_MSG = "Creating an {compute} requires {requirement} to be passed in."
SUCCESS_MSG = "No {compute} remain, deleting cluster."

SUPPORTED_COMPUTE_VALUES = frozenset({"nodegroup", "fargate"})
NODEGROUP_FULL_NAME = "Amazon EKS managed node groups"
FARGATE_FULL_NAME = "AWS Fargate profiles"


class EksCreateClusterOperator(BaseOperator):
    """
    Creates an Amazon EKS Cluster control plane.

    Optionally, can also create the supporting compute architecture:

     - If argument 'compute' is provided with a value of 'nodegroup', will also
         attempt to create an Amazon EKS Managed Nodegroup for the cluster.
         See :class:`~airflow.providers.amazon.aws.operators.EksCreateNodegroupOperator`
         documentation for requirements.

    -  If argument 'compute' is provided with a value of 'fargate', will also attempt to create an AWS
         Fargate profile for the cluster.
         See :class:`~airflow.providers.amazon.aws.operators.EksCreateFargateProfileOperator`
         documentation for requirements.


    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksCreateClusterOperator`

    :param cluster_name: The unique name to give to your Amazon EKS Cluster. (templated)
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions for the
         Kubernetes control plane to make calls to AWS API operations on your behalf. (templated)
    :param resources_vpc_config: The VPC configuration used by the cluster control plane. (templated)
    :param compute: The type of compute architecture to generate along with the cluster. (templated)
         Defaults to 'nodegroup' to generate an EKS Managed Nodegroup.
    :param create_cluster_kwargs: Optional parameters to pass to the CreateCluster API (templated)
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
         If this is None or empty then the default boto3 behaviour is used.

    If compute is assigned the value of 'nodegroup':

    :param nodegroup_name: *REQUIRED* The unique name to give your Amazon EKS managed node group. (templated)
    :param nodegroup_role_arn: *REQUIRED* The Amazon Resource Name (ARN) of the IAM role to associate with
         the Amazon EKS managed node group. (templated)
    :param create_nodegroup_kwargs: Optional parameters to pass to the CreateNodegroup API (templated)


    If compute is assigned the value of 'fargate':

    :param fargate_profile_name: *REQUIRED* The unique name to give your AWS Fargate profile. (templated)
    :param fargate_pod_execution_role_arn: *REQUIRED* The Amazon Resource Name (ARN) of the pod execution
         role to use for pods that match the selectors in the AWS Fargate profile. (templated)
    :param fargate_selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
    :param create_fargate_profile_kwargs: Optional parameters to pass to the CreateFargateProfile API
         (templated)

    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "cluster_role_arn",
        "resources_vpc_config",
        "create_cluster_kwargs",
        "compute",
        "nodegroup_name",
        "nodegroup_role_arn",
        "create_nodegroup_kwargs",
        "fargate_profile_name",
        "fargate_pod_execution_role_arn",
        "fargate_selectors",
        "create_fargate_profile_kwargs",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        cluster_role_arn: str,
        resources_vpc_config: dict[str, Any],
        compute: str | None = DEFAULT_COMPUTE_TYPE,
        create_cluster_kwargs: dict | None = None,
        nodegroup_name: str = DEFAULT_NODEGROUP_NAME,
        nodegroup_role_arn: str | None = None,
        create_nodegroup_kwargs: dict | None = None,
        fargate_profile_name: str = DEFAULT_FARGATE_PROFILE_NAME,
        fargate_pod_execution_role_arn: str | None = None,
        fargate_selectors: list | None = None,
        create_fargate_profile_kwargs: dict | None = None,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        self.compute = compute
        self.cluster_name = cluster_name
        self.cluster_role_arn = cluster_role_arn
        self.resources_vpc_config = resources_vpc_config
        self.create_cluster_kwargs = create_cluster_kwargs or {}
        self.nodegroup_name = nodegroup_name
        self.nodegroup_role_arn = nodegroup_role_arn
        self.create_nodegroup_kwargs = create_nodegroup_kwargs or {}
        self.fargate_profile_name = fargate_profile_name
        self.fargate_pod_execution_role_arn = fargate_pod_execution_role_arn
        self.fargate_selectors = fargate_selectors or [{"namespace": DEFAULT_NAMESPACE_NAME}]
        self.create_fargate_profile_kwargs = create_fargate_profile_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context: Context):
        if self.compute:
            if self.compute not in SUPPORTED_COMPUTE_VALUES:
                raise ValueError("Provided compute type is not supported.")
            elif (self.compute == "nodegroup") and not self.nodegroup_role_arn:
                raise ValueError(
                    MISSING_ARN_MSG.format(compute=NODEGROUP_FULL_NAME, requirement="nodegroup_role_arn")
                )
            elif (self.compute == "fargate") and not self.fargate_pod_execution_role_arn:
                raise ValueError(
                    MISSING_ARN_MSG.format(
                        compute=FARGATE_FULL_NAME, requirement="fargate_pod_execution_role_arn"
                    )
                )

        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_cluster(
            name=self.cluster_name,
            roleArn=self.cluster_role_arn,
            resourcesVpcConfig=self.resources_vpc_config,
            **self.create_cluster_kwargs,
        )

        # Short circuit early if we don't need to wait to attach compute
        # and the caller hasn't requested to wait for the cluster either.
        if not self.compute and not self.wait_for_completion:
            return None

        self.log.info("Waiting for EKS Cluster to provision.  This will take some time.")
        client = eks_hook.conn

        try:
            client.get_waiter("cluster_active").wait(name=self.cluster_name)
        except (ClientError, WaiterError) as e:
            self.log.error("Cluster failed to start and will be torn down.\n %s", e)
            eks_hook.delete_cluster(name=self.cluster_name)
            client.get_waiter("cluster_deleted").wait(name=self.cluster_name)
            raise

        if self.compute == "nodegroup":
            eks_hook.create_nodegroup(
                clusterName=self.cluster_name,
                nodegroupName=self.nodegroup_name,
                subnets=cast(List[str], self.resources_vpc_config.get("subnetIds")),
                nodeRole=self.nodegroup_role_arn,
                **self.create_nodegroup_kwargs,
            )
            if self.wait_for_completion:
                self.log.info("Waiting for nodegroup to provision.  This will take some time.")
                client.get_waiter("nodegroup_active").wait(
                    clusterName=self.cluster_name,
                    nodegroupName=self.nodegroup_name,
                )
        elif self.compute == "fargate":
            eks_hook.create_fargate_profile(
                clusterName=self.cluster_name,
                fargateProfileName=self.fargate_profile_name,
                podExecutionRoleArn=self.fargate_pod_execution_role_arn,
                selectors=self.fargate_selectors,
                **self.create_fargate_profile_kwargs,
            )
            if self.wait_for_completion:
                self.log.info("Waiting for Fargate profile to provision.  This will take some time.")
                client.get_waiter("fargate_profile_active").wait(
                    clusterName=self.cluster_name,
                    fargateProfileName=self.fargate_profile_name,
                )


class EksCreateNodegroupOperator(BaseOperator):
    """
    Creates an Amazon EKS managed node group for an existing Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksCreateNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to create the managed nodegroup in. (templated)
    :param nodegroup_name: The unique name to give your managed nodegroup. (templated)
    :param nodegroup_subnets:
         The subnets to use for the Auto Scaling group that is created for the managed nodegroup. (templated)
    :param nodegroup_role_arn:
         The Amazon Resource Name (ARN) of the IAM role to associate with the managed nodegroup. (templated)
    :param create_nodegroup_kwargs: Optional parameters to pass to the Create Nodegroup API (templated)
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.

    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "nodegroup_subnets",
        "nodegroup_role_arn",
        "nodegroup_name",
        "create_nodegroup_kwargs",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        nodegroup_subnets: list[str] | str,
        nodegroup_role_arn: str,
        nodegroup_name: str = DEFAULT_NODEGROUP_NAME,
        create_nodegroup_kwargs: dict | None = None,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_role_arn = nodegroup_role_arn
        self.nodegroup_name = nodegroup_name
        self.create_nodegroup_kwargs = create_nodegroup_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.nodegroup_subnets = nodegroup_subnets
        super().__init__(**kwargs)

    def execute(self, context: Context):
        if isinstance(self.nodegroup_subnets, str):
            nodegroup_subnets_list: list[str] = []
            if self.nodegroup_subnets != "":
                try:
                    nodegroup_subnets_list = cast(List, literal_eval(self.nodegroup_subnets))
                except ValueError:
                    self.log.warning(
                        "The nodegroup_subnets should be List or string representing "
                        "Python list and is %s. Defaulting to []",
                        self.nodegroup_subnets,
                    )
            self.nodegroup_subnets = nodegroup_subnets_list

        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        eks_hook.create_nodegroup(
            clusterName=self.cluster_name,
            nodegroupName=self.nodegroup_name,
            subnets=self.nodegroup_subnets,
            nodeRole=self.nodegroup_role_arn,
            **self.create_nodegroup_kwargs,
        )

        if self.wait_for_completion:
            self.log.info("Waiting for nodegroup to provision.  This will take some time.")
            eks_hook.conn.get_waiter("nodegroup_active").wait(
                clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
            )


class EksCreateFargateProfileOperator(BaseOperator):
    """
    Creates an AWS Fargate profile for an Amazon EKS cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksCreateFargateProfileOperator`

    :param cluster_name: The name of the Amazon EKS cluster to apply the AWS Fargate profile to. (templated)
    :param pod_execution_role_arn: The Amazon Resource Name (ARN) of the pod execution role to
         use for pods that match the selectors in the AWS Fargate profile. (templated)
    :param selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
    :param fargate_profile_name: The unique name to give your AWS Fargate profile. (templated)
    :param create_fargate_profile_kwargs: Optional parameters to pass to the CreateFargate Profile API
     (templated)
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)

    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "pod_execution_role_arn",
        "selectors",
        "fargate_profile_name",
        "create_fargate_profile_kwargs",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        pod_execution_role_arn: str,
        selectors: list,
        fargate_profile_name: str | None = DEFAULT_FARGATE_PROFILE_NAME,
        create_fargate_profile_kwargs: dict | None = None,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.pod_execution_role_arn = pod_execution_role_arn
        self.selectors = selectors
        self.fargate_profile_name = fargate_profile_name
        self.create_fargate_profile_kwargs = create_fargate_profile_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.create_fargate_profile(
            clusterName=self.cluster_name,
            fargateProfileName=self.fargate_profile_name,
            podExecutionRoleArn=self.pod_execution_role_arn,
            selectors=self.selectors,
            **self.create_fargate_profile_kwargs,
        )

        if self.wait_for_completion:
            self.log.info("Waiting for Fargate profile to provision.  This will take some time.")
            eks_hook.conn.get_waiter("fargate_profile_active").wait(
                clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
            )


class EksDeleteClusterOperator(BaseOperator):
    """
    Deletes the Amazon EKS Cluster control plane and all nodegroups attached to it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksDeleteClusterOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to delete. (templated)
    :param force_delete_compute: If True, will delete any attached resources. (templated)
         Defaults to False.
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.

    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "force_delete_compute",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        force_delete_compute: bool = False,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.force_delete_compute = force_delete_compute
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        if self.force_delete_compute:
            self.delete_any_nodegroups(eks_hook)
            self.delete_any_fargate_profiles(eks_hook)

        eks_hook.delete_cluster(name=self.cluster_name)

        if self.wait_for_completion:
            self.log.info("Waiting for cluster to delete.  This will take some time.")
            eks_hook.conn.get_waiter("cluster_deleted").wait(name=self.cluster_name)

    def delete_any_nodegroups(self, eks_hook) -> None:
        """
        Deletes all Amazon EKS managed node groups for a provided Amazon EKS Cluster.

        Amazon EKS managed node groups can be deleted in parallel, so we can send all
        delete commands in bulk and move on once the count of nodegroups is zero.
        """
        nodegroups = eks_hook.list_nodegroups(clusterName=self.cluster_name)
        if nodegroups:
            self.log.info(CAN_NOT_DELETE_MSG.format(compute=NODEGROUP_FULL_NAME, count=len(nodegroups)))
            for group in nodegroups:
                eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=group)
            # Note this is a custom waiter so we're using hook.get_waiter(), not hook.conn.get_waiter().
            self.log.info("Waiting for all nodegroups to delete.  This will take some time.")
            eks_hook.get_waiter("all_nodegroups_deleted").wait(clusterName=self.cluster_name)
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
            self.log.info("Waiting for Fargate profiles to delete.  This will take some time.")
            for profile in fargate_profiles:
                # The API will return a (cluster) ResourceInUseException if you try
                # to delete Fargate profiles in parallel the way we can with nodegroups,
                # so each must be deleted sequentially
                eks_hook.delete_fargate_profile(clusterName=self.cluster_name, fargateProfileName=profile)
                eks_hook.conn.get_waiter("fargate_profile_deleted").wait(
                    clusterName=self.cluster_name, fargateProfileName=profile
                )
        self.log.info(SUCCESS_MSG.format(compute=FARGATE_FULL_NAME))


class EksDeleteNodegroupOperator(BaseOperator):
    """
    Deletes an Amazon EKS managed node group from an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksDeleteNodegroupOperator`

    :param cluster_name: The name of the Amazon EKS Cluster associated with your nodegroup. (templated)
    :param nodegroup_name: The name of the nodegroup to delete. (templated)
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used.  If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.

    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "nodegroup_name",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=self.nodegroup_name)
        if self.wait_for_completion:
            self.log.info("Waiting for nodegroup to delete.  This will take some time.")
            eks_hook.conn.get_waiter("nodegroup_deleted").wait(
                clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
            )


class EksDeleteFargateProfileOperator(BaseOperator):
    """
    Deletes an AWS Fargate profile from an Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksDeleteFargateProfileOperator`

    :param cluster_name: The name of the Amazon EKS cluster associated with your Fargate profile. (templated)
    :param fargate_profile_name: The name of the AWS Fargate profile to delete. (templated)
    :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used.  If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "fargate_profile_name",
        "wait_for_completion",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_fargate_profile(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )
        if self.wait_for_completion:
            self.log.info("Waiting for Fargate profile to delete.  This will take some time.")
            eks_hook.conn.get_waiter("fargate_profile_deleted").wait(
                clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
            )


class EksPodOperator(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod on the specified Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksPodOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to execute the task on. (templated)
    :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
         for the Kubernetes control plane to make calls to AWS API operations on your behalf. (templated)
    :param in_cluster: If True, look for config inside the cluster; if False look for a local file path.
    :param namespace: The namespace in which to execute the pod. (templated)
    :param pod_name: The unique name to give the pod. (templated)
    :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
    :param aws_profile: str
    :param region: Which AWS region the connection should use. (templated)
         If this is None or empty then the default boto3 behaviour is used.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True, delete the
        pod; if False, leave the pod.  Current default is False, but this will be
        changed in the next major release of this provider.

    """

    template_fields: Sequence[str] = tuple(
        {
            "cluster_name",
            "in_cluster",
            "namespace",
            "pod_name",
            "aws_conn_id",
            "region",
        }
        | set(KubernetesPodOperator.template_fields)
    )

    def __init__(
        self,
        cluster_name: str,
        # Setting in_cluster to False tells the pod that the config
        # file is stored locally in the worker and not in the cluster.
        in_cluster: bool = False,
        namespace: str = DEFAULT_NAMESPACE_NAME,
        pod_context: str | None = None,
        pod_name: str | None = None,
        pod_username: str | None = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        is_delete_operator_pod: bool | None = None,
        **kwargs,
    ) -> None:
        if is_delete_operator_pod is None:
            warnings.warn(
                f"You have not set parameter `is_delete_operator_pod` in class {self.__class__.__name__}. "
                "Currently the default for this parameter is `False` but in a future release the default "
                "will be changed to `True`. To ensure pods are not deleted in the future you will need to "
                "set `is_delete_operator_pod=False` explicitly.",
                DeprecationWarning,
                stacklevel=2,
            )
            is_delete_operator_pod = False

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
            is_delete_operator_pod=is_delete_operator_pod,
            **kwargs,
        )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the EksPodOperator.
        if self.config_file:
            raise AirflowException("The config_file is not an allowed parameter for the EksPodOperator.")

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        with eks_hook.generate_config_file(
            eks_cluster_name=self.cluster_name, pod_namespace=self.namespace
        ) as self.config_file:
            return super().execute(context)
