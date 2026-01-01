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
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.redshift_cluster import RedshiftClusterTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class RedshiftClusterSensor(AwsBaseSensor[RedshiftHook]):
    """
    Waits for a Redshift cluster to reach a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RedshiftClusterSensor`

    :param cluster_identifier: The identifier for the cluster being pinged.
    :param target_status: The cluster status desired.
    :param deferrable: Run operator in the deferrable mode.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
         If this is ``None`` or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields("cluster_identifier", "target_status")
    aws_hook_class = RedshiftHook

    def __init__(
        self,
        *,
        cluster_identifier: str,
        target_status: str = "available",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        current_status = self.hook.cluster_status(self.cluster_identifier)
        self.log.info(
            "Poked cluster %s for status '%s', found status '%s'",
            self.cluster_identifier,
            self.target_status,
            current_status,
        )
        return current_status == self.target_status

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=RedshiftClusterTrigger(
                    aws_conn_id=self.aws_conn_id,
                    cluster_identifier=self.cluster_identifier,
                    target_status=self.target_status,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        status = validated_event["status"]
        if status == "error":
            raise AirflowException(f"{validated_event['status']}: {validated_event['message']}")
        if status == "success":
            self.log.info("%s completed successfully.", self.task_id)
            self.log.info("Cluster Identifier %s is in %s state", self.cluster_identifier, self.target_status)
