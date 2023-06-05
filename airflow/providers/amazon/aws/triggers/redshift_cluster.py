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
from functools import cached_property
from typing import Any, AsyncIterator

from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RedshiftCreateClusterTrigger(BaseTrigger):
    """
    Trigger for RedshiftCreateClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempt: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterTrigger",
            {
                "cluster_identifier": str(self.cluster_identifier),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    @cached_property
    def hook(self) -> RedshiftHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        async with self.hook.async_conn as client:
            await client.get_waiter("cluster_available").wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": int(self.poll_interval),
                    "MaxAttempts": int(self.max_attempt),
                },
            )
        yield TriggerEvent({"status": "success", "message": "Cluster Created"})


class RedshiftPauseClusterTrigger(BaseTrigger):
    """
    Trigger for RedshiftPauseClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `paused` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftPauseClusterTrigger",
            {
                "cluster_identifier": self.cluster_identifier,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> RedshiftHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = self.hook.get_waiter("cluster_paused", deferrable=True, client=client)
            while attempt < int(self.max_attempts):
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterIdentifier=self.cluster_identifier,
                        WaiterConfig={
                            "Delay": int(self.poll_interval),
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent({"status": "failure", "message": f"Pause Cluster Failed: {error}"})
                        break
                    self.log.info(
                        "Status of cluster is %s", error.last_response["Clusters"][0]["ClusterStatus"]
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            yield TriggerEvent(
                {"status": "failure", "message": "Pause Cluster Failed - max attempts reached."}
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Cluster paused"})


class RedshiftCreateClusterSnapshotTrigger(BaseTrigger):
    """
    Trigger for RedshiftCreateClusterSnapshotOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster snapshot to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterSnapshotTrigger",
            {
                "cluster_identifier": self.cluster_identifier,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> RedshiftHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("snapshot_available")
            while attempt < int(self.max_attempts):
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterIdentifier=self.cluster_identifier,
                        WaiterConfig={
                            "Delay": int(self.poll_interval),
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent(
                            {"status": "failure", "message": f"Create Cluster Snapshot Failed: {error}"}
                        )
                        break
                    self.log.info(
                        "Status of cluster snapshot is %s", error.last_response["Snapshots"][0]["Status"]
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            yield TriggerEvent(
                {
                    "status": "failure",
                    "message": "Create Cluster Snapshot Cluster Failed - max attempts reached.",
                }
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Cluster Snapshot Created"})


class RedshiftResumeClusterTrigger(BaseTrigger):
    """
    Trigger for RedshiftResumeClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftResumeClusterTrigger",
            {
                "cluster_identifier": self.cluster_identifier,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> RedshiftHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = self.hook.get_waiter("cluster_resumed", deferrable=True, client=client)
            while attempt < int(self.max_attempts):
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterIdentifier=self.cluster_identifier,
                        WaiterConfig={
                            "Delay": int(self.poll_interval),
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent(
                            {"status": "failure", "message": f"Resume Cluster Failed: {error}"}
                        )
                        break
                    self.log.info(
                        "Status of cluster is %s", error.last_response["Clusters"][0]["ClusterStatus"]
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            yield TriggerEvent(
                {"status": "failure", "message": "Resume Cluster Failed - max attempts reached."}
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Cluster resumed"})


class RedshiftDeleteClusterTrigger(BaseTrigger):
    """
    Trigger for RedshiftDeleteClusterOperator.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    """

    def __init__(
        self,
        cluster_identifier: str,
        max_attempts: int = 30,
        aws_conn_id: str = "aws_default",
        poll_interval: int = 30,
    ):
        super().__init__()
        self.cluster_identifier = cluster_identifier
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftDeleteClusterTrigger",
            {
                "cluster_identifier": self.cluster_identifier,
                "max_attempts": self.max_attempts,
                "aws_conn_id": self.aws_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self):
        return RedshiftHook(aws_conn_id=self.aws_conn_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = client.get_waiter("cluster_deleted")
            while attempt < self.max_attempts:
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterIdentifier=self.cluster_identifier,
                        WaiterConfig={
                            "Delay": self.poll_interval,
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent(
                            {"status": "failure", "message": f"Delete Cluster Failed: {error}"}
                        )
                        break
                    self.log.info(
                        "Cluster status is %s. Retrying attempt %s/%s",
                        error.last_response["Clusters"][0]["ClusterStatus"],
                        attempt,
                        self.max_attempts,
                    )
                    await asyncio.sleep(int(self.poll_interval))

        if attempt >= self.max_attempts:
            yield TriggerEvent(
                {"status": "failure", "message": "Delete Cluster Failed - max attempts reached."}
            )
        else:
            yield TriggerEvent({"status": "success", "message": "Cluster deleted."})
