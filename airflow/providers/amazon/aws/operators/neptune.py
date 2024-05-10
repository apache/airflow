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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.neptune import (
    NeptuneClusterAvailableTrigger,
    NeptuneClusterStoppedTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class NeptuneStartDbClusterOperator(AwsBaseOperator[NeptuneHook]):
    """Starts an Amazon Neptune DB cluster.

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
        self.delay = waiter_delay
        self.max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict[str, str]:
        self.log.info("Starting Neptune cluster: %s", self.cluster_id)

        # Check to make sure the cluster is not already available.
        status = self.hook.get_cluster_status(self.cluster_id)
        if status.lower() in NeptuneHook.AVAILABLE_STATES:
            self.log.info("Neptune cluster %s is already available.", self.cluster_id)
            return {"db_cluster_id": self.cluster_id}

        resp = self.hook.conn.start_db_cluster(DBClusterIdentifier=self.cluster_id)
        status = resp.get("DBClusters", {}).get("Status", "Unknown")

        if self.deferrable:
            self.log.info("Deferring for cluster start: %s", self.cluster_id)

            self.defer(
                trigger=NeptuneClusterAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    db_cluster_id=self.cluster_id,
                    waiter_delay=self.delay,
                    waiter_max_attempts=self.max_attempts,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info("Waiting for Neptune cluster %s to start.", self.cluster_id)
            self.hook.wait_for_cluster_availability(self.cluster_id, self.delay, self.max_attempts)

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
        self.delay = waiter_delay
        self.max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict[str, str]:
        self.log.info("Stopping Neptune cluster: %s", self.cluster_id)

        # Check to make sure the cluster is not already stopped.
        status = self.hook.get_cluster_status(self.cluster_id)
        if status.lower() in NeptuneHook.STOPPED_STATES:
            self.log.info("Neptune cluster %s is already stopped.", self.cluster_id)
            return {"db_cluster_id": self.cluster_id}

        resp = self.hook.conn.stop_db_cluster(DBClusterIdentifier=self.cluster_id)
        status = resp.get("DBClusters", {}).get("Status", "Unknown")

        if self.deferrable:
            self.log.info("Deferring for cluster stop: %s", self.cluster_id)

            self.defer(
                trigger=NeptuneClusterStoppedTrigger(
                    aws_conn_id=self.aws_conn_id,
                    db_cluster_id=self.cluster_id,
                    waiter_delay=self.delay,
                    waiter_max_attempts=self.max_attempts,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info("Waiting for Neptune cluster %s to start.", self.cluster_id)
            self.hook.wait_for_cluster_stopped(self.cluster_id, self.delay, self.max_attempts)

        return {"db_cluster_id": self.cluster_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        status = ""
        cluster_id = ""

        if event:
            status = event.get("status", "")
            cluster_id = event.get("cluster_id", "")

        self.log.info("Neptune cluster %s stopped with status: %s", cluster_id, status)

        return {"db_cluster_id": cluster_id}
