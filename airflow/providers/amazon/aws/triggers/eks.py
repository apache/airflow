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
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EksCreateFargateProfileTrigger(BaseTrigger):
    """
    Trigger for EksCreateFargateProfileOperator.
    The trigger will asynchronously wait for the fargate profile to be created.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_name": self.cluster_name,
                "fargate_profile_name": self.fargate_profile_name,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self):
        self.hook = EksHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("fargate_profile_active")
            while attempt < int(self.max_attempts):
                attempt += 1
                try:
                    await waiter.wait(
                        clusterName=self.cluster_name,
                        fargateProfileName=self.fargate_profile_name,
                        WaiterConfig={"Delay": int(self.poll_interval), "MaxAttempts": 1},
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"Create Fargate Profile failed: {error}")
                    self.log.info(
                        "Status of fargate profile is %s", error.last_response["fargateProfile"]["status"]
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            raise AirflowException(
                f"Create Fargate Profile failed - max attempts reached: {self.max_attempts}"
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Fargate Profile Created"})


class EksDeleteFargateProfileTrigger(BaseTrigger):
    """
    Trigger for EksDeleteFargateProfileOperator.
    The trigger will asynchronously wait for the fargate profile to be deleted.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_name": self.cluster_name,
                "fargate_profile_name": self.fargate_profile_name,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self):
        self.hook = EksHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("fargate_profile_deleted")
            while attempt < int(self.max_attempts):
                attempt += 1
                try:
                    await waiter.wait(
                        clusterName=self.cluster_name,
                        fargateProfileName=self.fargate_profile_name,
                        WaiterConfig={"Delay": int(self.poll_interval), "MaxAttempts": 1},
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"Delete Fargate Profile failed: {error}")
                    self.log.info(
                        "Status of fargate profile is %s", error.last_response["fargateProfile"]["status"]
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            raise AirflowException(
                f"Delete Fargate Profile failed - max attempts reached: {self.max_attempts}"
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Fargate Profile Deleted"})
