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

import logging
import warnings
from ast import literal_eval
from datetime import timedelta
from typing import TYPE_CHECKING, Any, List, Sequence, cast

from botocore.exceptions import ClientError, WaiterError

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateClusterTrigger,
    EksCreateFargateProfileTrigger,
    EksCreateNodegroupTrigger,
    EksDeleteClusterTrigger,
    EksDeleteFargateProfileTrigger,
    EksDeleteNodegroupTrigger,
)
from airflow.providers.amazon.aws.utils.waiter_with_logging import wait
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction

try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:
    # preserve backward compatibility for older versions of cncf.kubernetes provider
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


def _create_compute(
    compute: str | None,
    cluster_name: str,
    aws_conn_id: str,
    region: str | None,
    waiter_delay: int,
    waiter_max_attempts: int,
    wait_for_completion: bool = False,
    nodegroup_name: str | None = None,
    nodegroup_role_arn: str | None = None,
    create_nodegroup_kwargs: dict | None = None,
    fargate_profile_name: str | None = None,
    fargate_pod_execution_role_arn: str | None = None,
    fargate_selectors: list | None = None,
    create_fargate_profile_kwargs: dict | None = None,
    subnets: list[str] | None = None,
):
    log = logging.getLogger(__name__)
    eks_hook = EksHook(aws_conn_id=aws_conn_id, region_name=region)
    if compute == "nodegroup" and nodegroup_name:
        # this is to satisfy mypy
        subnets = subnets or []
        create_nodegroup_kwargs = create_nodegroup_kwargs or {}

        eks_hook.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=nodegroup_name,
            subnets=subnets,
            nodeRole=nodegroup_role_arn,
            **create_nodegroup_kwargs,
        )
        if wait_for_completion:
            log.info("Waiting for nodegroup to provision.  This will take some time.")
            wait(
                waiter=eks_hook.conn.get_waiter("nodegroup_active"),
                waiter_delay=waiter_delay,
                waiter_max_attempts=waiter_max_attempts,
                args={"clusterName": cluster_name, "nodegroupName": nodegroup_name},
                failure_message="Nodegroup creation failed",
                status_message="Nodegroup status is",
                status_args=["nodegroup.status"],
            )
    elif compute == "fargate" and fargate_profile_name:
        # this is to satisfy mypy
        create_fargate_profile_kwargs = create_fargate_profile_kwargs or {}
        fargate_selectors = fargate_selectors or []

        eks_hook.create_fargate_profile(
            clusterName=cluster_name,
            fargateProfileName=fargate_profile_name,
            podExecutionRoleArn=fargate_pod_execution_role_arn,
            selectors=fargate_selectors,
            **create_fargate_profile_kwargs,
        )
        if wait_for_completion:
            log.info("Waiting for Fargate profile to provision.  This will take some time.")
            wait(
                waiter=eks_hook.conn.get_waiter("fargate_profile_active"),
                waiter_delay=waiter_delay,
                waiter_max_attempts=waiter_max_attempts,
                args={"clusterName": cluster_name, "fargateProfileName": fargate_profile_name},
                failure_message="Fargate profile creation failed",
                status_message="Fargate profile status is",
                status_args=["fargateProfile.status"],
            )


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check cluster state
    :param waiter_max_attempts: The maximum number of attempts to check cluster state
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)

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
        resources_vpc_config: dict,
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
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        waiter_delay: int = 30,
        waiter_max_attempts: int = 40,
        **kwargs,
    ) -> None:
        self.compute = compute
        self.cluster_name = cluster_name
        self.cluster_role_arn = cluster_role_arn
        self.resources_vpc_config = resources_vpc_config
        self.create_cluster_kwargs = create_cluster_kwargs or {}
        self.nodegroup_role_arn = nodegroup_role_arn
        self.fargate_pod_execution_role_arn = fargate_pod_execution_role_arn
        self.create_fargate_profile_kwargs = create_fargate_profile_kwargs or {}
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.nodegroup_name = nodegroup_name
        self.create_nodegroup_kwargs = create_nodegroup_kwargs or {}
        self.fargate_selectors = fargate_selectors or [{"namespace": DEFAULT_NAMESPACE_NAME}]
        self.fargate_profile_name = fargate_profile_name
        self.deferrable = deferrable
        super().__init__(
            **kwargs,
        )

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
        self.eks_hook = EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region)
        self.eks_hook.create_cluster(
            name=self.cluster_name,
            roleArn=self.cluster_role_arn,
            resourcesVpcConfig=self.resources_vpc_config,
            **self.create_cluster_kwargs,
        )

        # Short circuit early if we don't need to wait to attach compute
        # and the caller hasn't requested to wait for the cluster either.
        if not any([self.compute, self.wait_for_completion, self.deferrable]):
            return None

        self.log.info("Waiting for EKS Cluster to provision. This will take some time.")
        client = self.eks_hook.conn

        if self.deferrable:
            self.defer(
                trigger=EksCreateClusterTrigger(
                    cluster_name=self.cluster_name,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="deferrable_create_cluster_next",
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
            )

        try:
            client.get_waiter("cluster_active").wait(
                name=self.cluster_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )
        except (ClientError, WaiterError) as e:
            self.log.error("Cluster failed to start and will be torn down.\n %s", e)
            self.eks_hook.delete_cluster(name=self.cluster_name)
            client.get_waiter("cluster_deleted").wait(
                name=self.cluster_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )
            raise
        _create_compute(
            compute=self.compute,
            cluster_name=self.cluster_name,
            aws_conn_id=self.aws_conn_id,
            region=self.region,
            wait_for_completion=self.wait_for_completion,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
            nodegroup_name=self.nodegroup_name,
            nodegroup_role_arn=self.nodegroup_role_arn,
            create_nodegroup_kwargs=self.create_nodegroup_kwargs,
            fargate_profile_name=self.fargate_profile_name,
            fargate_pod_execution_role_arn=self.fargate_pod_execution_role_arn,
            fargate_selectors=self.fargate_selectors,
            create_fargate_profile_kwargs=self.create_fargate_profile_kwargs,
            subnets=cast(List[str], self.resources_vpc_config.get("subnetIds")),
        )

    def deferrable_create_cluster_next(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event is None:
            self.log.error("Trigger error: event is None")
            raise AirflowException("Trigger error: event is None")
        elif event["status"] == "failed":
            self.log.error("Cluster failed to start and will be torn down.")
            self.eks_hook.delete_cluster(name=self.cluster_name)
            self.defer(
                trigger=EksDeleteClusterTrigger(
                    cluster_name=self.cluster_name,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region,
                    force_delete_compute=False,
                ),
                method_name="execute_failed",
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
            )
        elif event["status"] == "success":
            self.log.info("Cluster is ready to provision compute.")
            _create_compute(
                compute=self.compute,
                cluster_name=self.cluster_name,
                aws_conn_id=self.aws_conn_id,
                region=self.region,
                wait_for_completion=self.wait_for_completion,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                nodegroup_name=self.nodegroup_name,
                nodegroup_role_arn=self.nodegroup_role_arn,
                create_nodegroup_kwargs=self.create_nodegroup_kwargs,
                fargate_profile_name=self.fargate_profile_name,
                fargate_pod_execution_role_arn=self.fargate_pod_execution_role_arn,
                fargate_selectors=self.fargate_selectors,
                create_fargate_profile_kwargs=self.create_fargate_profile_kwargs,
                subnets=cast(List[str], self.resources_vpc_config.get("subnetIds")),
            )
            if self.compute == "fargate":
                self.defer(
                    trigger=EksCreateFargateProfileTrigger(
                        cluster_name=self.cluster_name,
                        fargate_profile_name=self.fargate_profile_name,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                        region=self.region,
                    ),
                    method_name="execute_complete",
                    timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                )
            else:
                self.defer(
                    trigger=EksCreateNodegroupTrigger(
                        nodegroup_name=self.nodegroup_name,
                        cluster_name=self.cluster_name,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                    ),
                    method_name="execute_complete",
                    timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
                )

    def execute_failed(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event is None:
            self.log.info("Trigger error: event is None")
            raise AirflowException("Trigger error: event is None")
        elif event["status"] == "delteted":
            self.log.info("Cluster deleted")
            raise event["exception"]

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        resource = "fargate profile" if self.compute == "fargate" else self.compute
        if event is None:
            self.log.info("Trigger error: event is None")
            raise AirflowException("Trigger error: event is None")
        elif event["status"] != "success":
            raise AirflowException(f"Error creating {resource}: {event}")

        self.log.info("%s created successfully", resource)


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check nodegroup state
    :param waiter_max_attempts: The maximum number of attempts to check nodegroup state
    :param deferrable: If True, the operator will wait asynchronously for the nodegroup to be created.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)

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
        waiter_delay: int = 30,
        waiter_max_attempts: int = 80,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        self.nodegroup_subnets = nodegroup_subnets
        self.compute = "nodegroup"
        self.cluster_name = cluster_name
        self.nodegroup_role_arn = nodegroup_role_arn
        self.nodegroup_name = nodegroup_name
        self.create_nodegroup_kwargs = create_nodegroup_kwargs or {}
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable
        super().__init__(**kwargs)

    def execute(self, context: Context):
        self.log.info(self.task_id)
        if isinstance(self.nodegroup_subnets, str):
            nodegroup_subnets_list: list[str] = []
            if self.nodegroup_subnets != "":
                try:
                    nodegroup_subnets_list = cast(list, literal_eval(self.nodegroup_subnets))
                except ValueError:
                    self.log.warning(
                        "The nodegroup_subnets should be List or string representing "
                        "Python list and is %s. Defaulting to []",
                        self.nodegroup_subnets,
                    )
            self.nodegroup_subnets = nodegroup_subnets_list

        _create_compute(
            compute=self.compute,
            cluster_name=self.cluster_name,
            aws_conn_id=self.aws_conn_id,
            region=self.region,
            wait_for_completion=self.wait_for_completion,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
            nodegroup_name=self.nodegroup_name,
            nodegroup_role_arn=self.nodegroup_role_arn,
            create_nodegroup_kwargs=self.create_nodegroup_kwargs,
            subnets=self.nodegroup_subnets,
        )

        if self.deferrable:
            self.defer(
                trigger=EksCreateNodegroupTrigger(
                    cluster_name=self.cluster_name,
                    nodegroup_name=self.nodegroup_name,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay + 60),
            )

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error creating nodegroup: {event}")
        return


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check profile status
    :param waiter_max_attempts: The maximum number of attempts to check the status of the profile.
    :param deferrable: If True, the operator will wait asynchronously for the profile to be created.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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
        fargate_profile_name: str = DEFAULT_FARGATE_PROFILE_NAME,
        create_fargate_profile_kwargs: dict | None = None,
        wait_for_completion: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        waiter_delay: int = 10,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.selectors = selectors
        self.pod_execution_role_arn = pod_execution_role_arn
        self.fargate_profile_name = fargate_profile_name
        self.create_fargate_profile_kwargs = create_fargate_profile_kwargs or {}
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable
        self.compute = "fargate"
        super().__init__(
            **kwargs,
        )

    def execute(self, context: Context):
        _create_compute(
            compute=self.compute,
            cluster_name=self.cluster_name,
            aws_conn_id=self.aws_conn_id,
            region=self.region,
            wait_for_completion=self.wait_for_completion,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
            fargate_profile_name=self.fargate_profile_name,
            fargate_pod_execution_role_arn=self.pod_execution_role_arn,
            fargate_selectors=self.selectors,
            create_fargate_profile_kwargs=self.create_fargate_profile_kwargs,
        )
        if self.deferrable:
            self.defer(
                trigger=EksCreateFargateProfileTrigger(
                    cluster_name=self.cluster_name,
                    fargate_profile_name=self.fargate_profile_name,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    region=self.region,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=(self.waiter_max_attempts * self.waiter_delay + 60)),
            )

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error creating Fargate profile: {event}")
        else:
            self.log.info("Fargate profile created successfully")
        return


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check cluster state
    :param waiter_max_attempts: The maximum number of attempts to check cluster state
    :param deferrable: If True, the operator will wait asynchronously for the cluster to be deleted.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)

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
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        waiter_delay: int = 30,
        waiter_max_attempts: int = 40,
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.force_delete_compute = force_delete_compute
        self.wait_for_completion = False if deferrable else wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        super().__init__(**kwargs)

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        if self.deferrable:
            self.defer(
                trigger=EksDeleteClusterTrigger(
                    cluster_name=self.cluster_name,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region,
                    force_delete_compute=self.force_delete_compute,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.waiter_delay * self.waiter_max_attempts),
            )
        elif self.force_delete_compute:
            self.delete_any_nodegroups(eks_hook)
            self.delete_any_fargate_profiles(eks_hook)

        eks_hook.delete_cluster(name=self.cluster_name)

        if self.wait_for_completion:
            self.log.info("Waiting for cluster to delete.  This will take some time.")
            eks_hook.conn.get_waiter("cluster_deleted").wait(name=self.cluster_name)

    def delete_any_nodegroups(self, eks_hook) -> None:
        """
        Delete all Amazon EKS managed node groups for a provided Amazon EKS Cluster.

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
        Delete all EKS Fargate profiles for a provided Amazon EKS Cluster.

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

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event is None:
            self.log.error("Trigger error. Event is None")
            raise AirflowException("Trigger error. Event is None")
        elif event["status"] == "success":
            self.log.info("Cluster deleted successfully.")


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check nodegroup state
    :param waiter_max_attempts: The maximum number of attempts to check nodegroup state
    :param deferrable: If True, the operator will wait asynchronously for the nodegroup to be deleted.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)

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
        waiter_delay: int = 30,
        waiter_max_attempts: int = 40,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable
        super().__init__(**kwargs)

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=self.nodegroup_name)
        if self.deferrable:
            self.defer(
                trigger=EksDeleteNodegroupTrigger(
                    cluster_name=self.cluster_name,
                    nodegroup_name=self.nodegroup_name,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay + 60),
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for nodegroup to delete.  This will take some time.")
            eks_hook.conn.get_waiter("nodegroup_deleted").wait(
                clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
            )

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error deleting nodegroup: {event}")
        return


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
    :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check profile status
    :param waiter_max_attempts: The maximum number of attempts to check the status of the profile.
    :param deferrable: If True, the operator will wait asynchronously for the profile to be deleted.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        eks_hook.delete_fargate_profile(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )
        if self.deferrable:
            self.defer(
                trigger=EksDeleteFargateProfileTrigger(
                    cluster_name=self.cluster_name,
                    fargate_profile_name=self.fargate_profile_name,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    region=self.region,
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=(self.waiter_max_attempts * self.waiter_delay + 60)),
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for Fargate profile to delete.  This will take some time.")
            eks_hook.conn.get_waiter("fargate_profile_deleted").wait(
                clusterName=self.cluster_name,
                fargateProfileName=self.fargate_profile_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error deleting Fargate profile: {event}")
        else:
            self.log.info("Fargate profile deleted successfully")
        return


class EksPodOperator(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod on the specified Amazon EKS Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EksPodOperator`

    :param cluster_name: The name of the Amazon EKS Cluster to execute the task on. (templated)
    :param in_cluster: If True, look for config inside the cluster; if False look for a local file path.
    :param namespace: The namespace in which to execute the pod. (templated)
    :param pod_name: The unique name to give the pod. (templated)
    :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
    :param region: Which AWS region the connection should use. (templated)
         If this is None or empty then the default boto3 behaviour is used.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
        If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
        only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
        Current default is `keep_pod`, but this will be changed in the next major release of this provider.
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True, delete the
        pod; if False, leave the pod. Current default is False, but this will be
        changed in the next major release of this provider.
        Deprecated - use `on_finish_action` instead.

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
        on_finish_action: str | None = None,
        is_delete_operator_pod: bool | None = None,
        **kwargs,
    ) -> None:
        if is_delete_operator_pod is not None:
            warnings.warn(
                "`is_delete_operator_pod` parameter is deprecated, please use `on_finish_action`",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["on_finish_action"] = (
                OnFinishAction.DELETE_POD if is_delete_operator_pod else OnFinishAction.KEEP_POD
            )
        else:
            if on_finish_action is not None:
                kwargs["on_finish_action"] = OnFinishAction(on_finish_action)
            else:
                warnings.warn(
                    f"You have not set parameter `on_finish_action` in class {self.__class__.__name__}. "
                    "Currently the default for this parameter is `keep_pod` but in a future release"
                    " the default will be changed to `delete_pod`. To ensure pods are not deleted in"
                    " the future you will need to set `on_finish_action=keep_pod` explicitly.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
                kwargs["on_finish_action"] = OnFinishAction.KEEP_POD

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
