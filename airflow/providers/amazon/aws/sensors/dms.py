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

from typing import TYPE_CHECKING, Iterable, Sequence

from deprecated import deprecated

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DmsTaskBaseSensor(AwsBaseSensor[DmsHook]):
    """
    Contains general sensor behavior for DMS task.

    Subclasses should set ``target_statuses`` and ``termination_statuses`` fields.

    :param replication_task_arn: AWS DMS replication task ARN
    :param target_statuses: the target statuses, sensor waits until
        the task reaches any of these states
    :param termination_statuses: the termination statuses, sensor fails when
        the task reaches any of these states
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_task_arn")

    def __init__(
        self,
        replication_task_arn: str,
        target_statuses: Iterable[str] | None = None,
        termination_statuses: Iterable[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.target_statuses: Iterable[str] = target_statuses or []
        self.termination_statuses: Iterable[str] = termination_statuses or []

    @deprecated(reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning)
    def get_hook(self) -> DmsHook:
        """Get DmsHook."""
        return self.hook

    def poke(self, context: Context):
        if not (status := self.hook.get_task_status(self.replication_task_arn)):
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = f"Failed to read task status, task with ARN {self.replication_task_arn} not found"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)

        self.log.info("DMS Replication task (%s) has status: %s", self.replication_task_arn, status)

        if status in self.target_statuses:
            return True

        if status in self.termination_statuses:
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = f"Unexpected status: {status}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)

        return False


class DmsTaskCompletedSensor(DmsTaskBaseSensor):
    """
    Pokes DMS task until it is completed.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:DmsTaskCompletedSensor`

    :param replication_task_arn: AWS DMS replication task ARN
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(self, **kwargs):
        kwargs["target_statuses"] = ["stopped"]
        kwargs["termination_statuses"] = [
            "creating",
            "deleting",
            "failed",
            "failed-move",
            "modifying",
            "moving",
            "ready",
            "testing",
        ]
        super().__init__(**kwargs)
