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

from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.triggers.redshift_cluster import RedshiftClusterTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftClusterSensor(BaseSensorOperator):
    """
    Waits for a Redshift cluster to reach a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RedshiftClusterSensor`

    :param cluster_identifier: The identifier for the cluster being pinged.
    :param target_status: The cluster status desired.
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = ("cluster_identifier", "target_status")

    def __init__(
        self,
        *,
        cluster_identifier: str,
        target_status: str = "available",
        aws_conn_id: str | None = "aws_default",
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.aws_conn_id = aws_conn_id
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

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        event = validate_execute_complete_event(event)

        status = event["status"]
        if status == "error":
            raise AirflowException(f"{event['status']}: {event['message']}")
        elif status == "success":
            self.log.info("%s completed successfully.", self.task_id)
            self.log.info(
                "Cluster Identifier %s is in %s state",
                self.cluster_identifier,
                self.target_status,
            )

    @cached_property
    def hook(self) -> RedshiftHook:
        return RedshiftHook(aws_conn_id=self.aws_conn_id)
