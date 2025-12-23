#
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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.neptune import (
    NeptuneClusterAvailableTrigger,
    NeptuneClusterInstancesAvailableTrigger,
    NeptuneClusterStoppedTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.utils.context import Context


def handle_waitable_exception(
    operator: NeptuneStartDbClusterOperator | NeptuneStopDbClusterOperator, err: str
):
    """
    Handle client exceptions for invalid cluster or invalid instance status that are temporary.

    After status change, it's possible to retry. Waiter will handle terminal status.
    """
    code = err

    if code in ("InvalidDBInstanceStateFault", "InvalidDBInstanceState"):
        if operator.deferrable:
            operator.log.info("Deferring until instances become available: %s", operator.cluster_id)
            operator.defer(
                trigger=NeptuneClusterInstancesAvailableTrigger(
                    aws_conn_id=operator.aws_conn_id,
                    db_cluster_id=operator.cluster_id,
                    region_name=operator.region_name,
                    botocore_config=operator.botocore_config,
                    verify=operator.verify,
                ),
                method_name="execute",
            )
        else:
            operator.log.info("Need to wait for instances to become available: %s", operator.cluster_id)
            operator.hook.wait_for_cluster_instance_availability(cluster_id=operator.cluster_id)
    if code in ["InvalidClusterState", "InvalidDBClusterStateFault"]:
        if operator.deferrable:
            operator.log.info("Deferring until cluster becomes available: %s", operator.cluster_id)
            operator.defer(
                trigger=NeptuneClusterAvailableTrigger(
                    aws_conn_id=operator.aws_conn_id,
                    db_cluster_id=operator.cluster_id,
                    region_name=operator.region_name,
                    botocore_config=operator.botocore_config,
                    verify=operator.verify,
                ),
                method_name="execute",
            )
        else:
            operator.log.info("Need to wait for cluster to become available: %s", operator.cluster_id)
            operator.hook.wait_for_cluster_availability(operator.cluster_id)


class NeptuneStartDbClusterOperator(AwsBaseOperator[NeptuneHook]):
    """
    Starts an Amazon Neptune DB cluster.

    Amazon Neptune Database is a serverless graph database designed for superior scalability
    and availability. Neptune Database provides built-in security, continuous backups, and
    integrations with other AWS services

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStartDbClusterOperator`

    :param db_cluster_id: The DB cluster identifier of the Neptune DB cluster to be started.
    :param wait_for_completion: Whether to wait for the cluster to start. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the cluster to start.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune cluster id
    """

    aws_hook_class = NeptuneHook
    template_fields: Sequence[str] = aws_template_fields("cluster_id")

    def __init__(
        self,
        db_cluster_id: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = db_cluster_id
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context, event: dict[str, Any] | None = None, **kwargs) -> dict[str, str]:
        self.log.info("Starting Neptune cluster: %s", self.cluster_id)

        # Check to make sure the cluster is not already available.
        status = self.hook.get_cluster_status(self.cluster_id)
        if status.lower() in NeptuneHook.AVAILABLE_STATES:
            self.log.info("Neptune cluster %s is already available.", self.cluster_id)
            return {"db_cluster_id": self.cluster_id}
        if status.lower() in NeptuneHook.ERROR_STATES:
            # some states will not allow you to start the cluster
            self.log.error(
                "Neptune cluster %s is in error state %s and cannot be started", self.cluster_id, status
            )
            raise AirflowException(f"Neptune cluster {self.cluster_id} is in error state {status}")

        """
        A cluster and its instances must be in a valid state to send the start request.
        This loop covers the case where the cluster is not available and also the case where
        the cluster is available, but one or more of the instances are in an invalid state.
        If either are in an invalid state, wait for the availability and retry.
        Let the waiters handle retries and detecting the error states.
        """
        try:
            self.hook.conn.start_db_cluster(DBClusterIdentifier=self.cluster_id)
        except ClientError as ex:
            code = ex.response["Error"]["Code"]
            self.log.warning("Received client error when attempting to start the cluster: %s", code)

            if code in ["InvalidDBInstanceState", "InvalidClusterState", "InvalidDBClusterStateFault"]:
                handle_waitable_exception(operator=self, err=code)

            else:
                # re raise for any other type of client error
                raise

        if self.deferrable:
            self.log.info("Deferring for cluster start: %s", self.cluster_id)

            self.defer(
                trigger=NeptuneClusterAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    db_cluster_id=self.cluster_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info("Waiting for Neptune cluster %s to start.", self.cluster_id)
            self.hook.wait_for_cluster_availability(
                self.cluster_id, self.waiter_delay, self.waiter_max_attempts
            )

        return {"db_cluster_id": self.cluster_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        status = ""
        cluster_id = ""

        if event:
            status = event.get("status", "")
            cluster_id = event.get("cluster_id", "")

        self.log.info("Neptune cluster %s available with status: %s", cluster_id, status)

        return {"db_cluster_id": cluster_id}


class NeptuneStopDbClusterOperator(AwsBaseOperator[NeptuneHook]):
    """
    Stops an Amazon Neptune DB cluster.

    Amazon Neptune Database is a serverless graph database designed for superior scalability
    and availability. Neptune Database provides built-in security, continuous backups, and
    integrations with other AWS services

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStartDbClusterOperator`

    :param db_cluster_id: The DB cluster identifier of the Neptune DB cluster to be stopped.
    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the cluster to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune cluster id
    """

    aws_hook_class = NeptuneHook
    template_fields: Sequence[str] = aws_template_fields("cluster_id")

    def __init__(
        self,
        db_cluster_id: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = db_cluster_id
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context, event: dict[str, Any] | None = None, **kwargs) -> dict[str, str]:
        self.log.info("Stopping Neptune cluster: %s", self.cluster_id)

        # Check to make sure the cluster is not already stopped or that its not in a bad state
        status = self.hook.get_cluster_status(self.cluster_id)
        self.log.info("Current status: %s", status)

        if status.lower() in NeptuneHook.STOPPED_STATES:
            self.log.info("Neptune cluster %s is already stopped.", self.cluster_id)
            return {"db_cluster_id": self.cluster_id}
        if status.lower() in NeptuneHook.ERROR_STATES:
            # some states will not allow you to stop the cluster
            self.log.error(
                "Neptune cluster %s is in error state %s and cannot be stopped", self.cluster_id, status
            )
            raise AirflowException(f"Neptune cluster {self.cluster_id} is in error state {status}")

        """
        A cluster and its instances must be in a valid state to send the stop request.
        This loop covers the case where the cluster is not available and also the case where
        the cluster is available, but one or more of the instances are in an invalid state.
        If either are in an invalid state, wait for the availability and retry.
        Let the waiters handle retries and detecting the error states.
        """

        try:
            self.hook.conn.stop_db_cluster(DBClusterIdentifier=self.cluster_id)

        # cluster must be in available state to stop it
        except ClientError as ex:
            code = ex.response["Error"]["Code"]
            self.log.warning("Received client error when attempting to stop the cluster: %s", code)

            # these can be handled by a waiter
            if code in [
                "InvalidDBInstanceState",
                "InvalidDBInstanceStateFault",
                "InvalidClusterState",
                "InvalidDBClusterStateFault",
            ]:
                handle_waitable_exception(self, code)
            else:
                # re raise for any other type of client error
                raise

        if self.deferrable:
            self.log.info("Deferring for cluster stop: %s", self.cluster_id)

            self.defer(
                trigger=NeptuneClusterStoppedTrigger(
                    aws_conn_id=self.aws_conn_id,
                    db_cluster_id=self.cluster_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info("Waiting for Neptune cluster %s to stop.", self.cluster_id)

            self.hook.wait_for_cluster_stopped(self.cluster_id, self.waiter_delay, self.waiter_max_attempts)

        return {"db_cluster_id": self.cluster_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        status = ""
        cluster_id = ""
        self.log.info(event)
        if event:
            status = event.get("status", "")
            cluster_id = event.get("cluster_id", "")

        self.log.info("Neptune cluster %s stopped with status: %s", cluster_id, status)

        return {"db_cluster_id": cluster_id}
