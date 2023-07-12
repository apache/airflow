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

import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger


class EksCreateFargateProfileTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously wait for the fargate profile to be created.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region: str | None = None,
        region_name: str | None = None,
    ):
        if region is not None:
            warnings.warn(
                "please use region_name param instead of region",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            region_name = region

        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "fargate_profile_name": fargate_profile_name},
            waiter_name="fargate_profile_active",
            waiter_args={"clusterName": cluster_name, "fargateProfileName": fargate_profile_name},
            failure_message="Failure while creating Fargate profile",
            status_message="Fargate profile not created yet",
            status_queries=["fargateProfile.status"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksDeleteFargateProfileTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously wait for the fargate profile to be deleted.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region: str | None = None,
        region_name: str | None = None,
    ):
        if region is not None:
            warnings.warn(
                "please use region_name param instead of region",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            region_name = region

        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "fargate_profile_name": fargate_profile_name},
            waiter_name="fargate_profile_deleted",
            waiter_args={"clusterName": cluster_name, "fargateProfileName": fargate_profile_name},
            failure_message="Failure while deleting Fargate profile",
            status_message="Fargate profile not deleted yet",
            status_queries=["fargateProfile.status"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksCreateNodegroupTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksCreateNodegroupOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    nodegroup to be in the state specified by the waiter.

    :param waiter_name: Name of the waiter to use, for instance 'nodegroup_active' or 'nodegroup_deleted'
    :param cluster_name: The name of the EKS cluster associated with the node group.
    :param nodegroup_name: The name of the nodegroup to check.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region_name: str | None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "nodegroup_name": nodegroup_name},
            waiter_name="nodegroup_active",
            waiter_args={"clusterName": cluster_name, "nodegroupName": nodegroup_name},
            failure_message="Error creating nodegroup",
            status_message="Nodegroup status is",
            status_queries=["nodegroup.status", "nodegroup.health.issues"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksDeleteNodegroupTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksDeleteNodegroupOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    nodegroup to be in the state specified by the waiter.

    :param waiter_name: Name of the waiter to use, for instance 'nodegroup_active' or 'nodegroup_deleted'
    :param cluster_name: The name of the EKS cluster associated with the node group.
    :param nodegroup_name: The name of the nodegroup to check.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region_name: str | None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "nodegroup_name": nodegroup_name},
            waiter_name="nodegroup_deleted",
            waiter_args={"clusterName": cluster_name, "nodegroupName": nodegroup_name},
            failure_message="Error deleting nodegroup",
            status_message="Nodegroup status is",
            status_queries=["nodegroup.status", "nodegroup.health.issues"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
