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

import asyncio
from typing import Any

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EksCreateFargateProfileTrigger(BaseTrigger):
    """
    Trigger for EksCreateFargateProfileOperator.
    The trigger will asynchronously wait for the fargate profile to be created.

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
    ):
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_name": self.cluster_name,
                "fargate_profile_name": self.fargate_profile_name,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
            },
        )

    async def run(self):
        self.hook = EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("fargate_profile_active")
            while attempt < int(self.waiter_max_attempts):
                attempt += 1
                try:
                    await waiter.wait(
                        clusterName=self.cluster_name,
                        fargateProfileName=self.fargate_profile_name,
                        WaiterConfig={"Delay": int(self.waiter_delay), "MaxAttempts": 1},
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"Create Fargate Profile failed: {error}")
                    self.log.info(
                        "Status of fargate profile is %s", error.last_response["fargateProfile"]["status"]
                    )
                    await asyncio.sleep(int(self.waiter_delay))
        if attempt >= int(self.waiter_max_attempts):
            raise AirflowException(
                f"Create Fargate Profile failed - max attempts reached: {self.waiter_max_attempts}"
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Fargate Profile Created"})


class EksDeleteFargateProfileTrigger(BaseTrigger):
    """
    Trigger for EksDeleteFargateProfileOperator.
    The trigger will asynchronously wait for the fargate profile to be deleted.

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
    ):
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_name": self.cluster_name,
                "fargate_profile_name": self.fargate_profile_name,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
            },
        )

    async def run(self):
        self.hook = EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("fargate_profile_deleted")
            while attempt < int(self.waiter_max_attempts):
                attempt += 1
                try:
                    await waiter.wait(
                        clusterName=self.cluster_name,
                        fargateProfileName=self.fargate_profile_name,
                        WaiterConfig={"Delay": int(self.waiter_delay), "MaxAttempts": 1},
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"Delete Fargate Profile failed: {error}")
                    self.log.info(
                        "Status of fargate profile is %s", error.last_response["fargateProfile"]["status"]
                    )
                    await asyncio.sleep(int(self.waiter_delay))
        if attempt >= int(self.waiter_max_attempts):
            raise AirflowException(
                f"Delete Fargate Profile failed - max attempts reached: {self.waiter_max_attempts}"
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Fargate Profile Deleted"})


class EksNodegroupTrigger(BaseTrigger):
    """
    Trigger for EksCreateNodegroupOperator and EksDeleteNodegroupOperator.

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
        waiter_name: str,
        cluster_name: str,
        nodegroup_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region: str | None,
    ):
        self.waiter_name = waiter_name
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.aws_conn_id = aws_conn_id
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "waiter_name": self.waiter_name,
                "cluster_name": self.cluster_name,
                "nodegroup_name": self.nodegroup_name,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
            },
        )

    async def run(self):
        self.hook = EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region)
        async with self.hook.async_conn as client:
            waiter = client.get_waiter(self.waiter_name)
            await async_wait(
                waiter=waiter,
                waiter_max_attempts=int(self.waiter_max_attempts),
                waiter_delay=int(self.waiter_delay),
                args={"clusterName": self.cluster_name, "nodegroupName": self.nodegroup_name},
                failure_message="Error checking nodegroup",
                status_message="Nodegroup status is",
                status_args=["nodegroup.status"],
            )

        yield TriggerEvent(
            {
                "status": "success",
                "cluster_name": self.cluster_name,
                "nodegroup_name": self.nodegroup_name,
            }
        )
