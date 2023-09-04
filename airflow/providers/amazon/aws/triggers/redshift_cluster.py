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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class RedshiftCreateClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for RedshiftCreateClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int | None = None,
        max_attempt: int | None = None,
        aws_conn_id: str = "aws_default",
        waiter_delay: int = 15,
        waiter_max_attempts: int = 999999,
    ):
        if poll_interval is not None or max_attempt is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempt.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempt or waiter_max_attempts
        super().__init__(
            serialized_fields={"cluster_identifier": cluster_identifier},
            waiter_name="cluster_available",
            waiter_args={"ClusterIdentifier": cluster_identifier},
            failure_message="Error while creating the redshift cluster",
            status_message="Redshift cluster creation in progress",
            status_queries=["Clusters[].ClusterStatus"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)


class RedshiftPauseClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for RedshiftPauseClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `paused` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int | None = None,
        max_attempts: int | None = None,
        aws_conn_id: str = "aws_default",
        waiter_delay: int = 15,
        waiter_max_attempts: int = 999999,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempt.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"cluster_identifier": cluster_identifier},
            waiter_name="cluster_paused",
            waiter_args={"ClusterIdentifier": cluster_identifier},
            failure_message="Error while pausing the redshift cluster",
            status_message="Redshift cluster pausing in progress",
            status_queries=["Clusters[].ClusterStatus"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)


class RedshiftCreateClusterSnapshotTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for RedshiftCreateClusterSnapshotOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster snapshot to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int | None = None,
        max_attempts: int | None = None,
        aws_conn_id: str = "aws_default",
        waiter_delay: int = 15,
        waiter_max_attempts: int = 999999,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempt.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"cluster_identifier": cluster_identifier},
            waiter_name="snapshot_available",
            waiter_args={"ClusterIdentifier": cluster_identifier},
            failure_message="Create Cluster Snapshot Failed",
            status_message="Redshift Cluster Snapshot in progress",
            status_queries=["Clusters[].ClusterStatus"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)


class RedshiftResumeClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for RedshiftResumeClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `available` state.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int | None = None,
        max_attempts: int | None = None,
        aws_conn_id: str = "aws_default",
        waiter_delay: int = 15,
        waiter_max_attempts: int = 999999,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempt.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"cluster_identifier": cluster_identifier},
            waiter_name="cluster_resumed",
            waiter_args={"ClusterIdentifier": cluster_identifier},
            failure_message="Resume Cluster Snapshot Failed",
            status_message="Redshift Cluster resuming in progress",
            status_queries=["Clusters[].ClusterStatus"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)


class RedshiftDeleteClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for RedshiftDeleteClusterOperator.

    :param cluster_identifier:  A unique identifier for the cluster.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    """

    def __init__(
        self,
        cluster_identifier: str,
        poll_interval: int | None = None,
        max_attempts: int | None = None,
        aws_conn_id: str = "aws_default",
        waiter_delay: int = 30,
        waiter_max_attempts: int = 30,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempt.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"cluster_identifier": cluster_identifier},
            waiter_name="cluster_deleted",
            waiter_args={"ClusterIdentifier": cluster_identifier},
            failure_message="Delete Cluster Failed",
            status_message="Redshift Cluster deletion in progress",
            status_queries=["Clusters[].ClusterStatus"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)
