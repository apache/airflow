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

from typing import TYPE_CHECKING, Sequence

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
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
    """

    template_fields: Sequence[str] = ("cluster_identifier", "target_status")

    def __init__(
        self,
        *,
        cluster_identifier: str,
        target_status: str = "available",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.aws_conn_id = aws_conn_id
        self.hook: RedshiftHook | None = None

    def poke(self, context: Context):
        self.log.info("Poking for status : %s\nfor cluster %s", self.target_status, self.cluster_identifier)
        return self.get_hook().cluster_status(self.cluster_identifier) == self.target_status

    def get_hook(self) -> RedshiftHook:
        """Create and return a RedshiftHook"""
        if self.hook:
            return self.hook

        self.hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        return self.hook
