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
"""Sensor waiting for a SQL query to return a truthy first cell in InfluxDB 3.x."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowFailException, BaseSensorOperator, conf
from airflow.providers.influxdb.hooks.influxdb3 import InfluxDB3Hook
from airflow.providers.influxdb.triggers.influxdb3 import InfluxDB3SensorTrigger
from airflow.providers.influxdb.utils import _first_cell_is_truthy

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class InfluxDB3Sensor(BaseSensorOperator):
    """
    Wait until an InfluxDB 3.x SQL query returns a truthy first cell.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:InfluxDB3Sensor`

    :param sql: The SQL query to poll.
    :param influxdb3_conn_id: Reference to :ref:`InfluxDB 3 connection id <howto/connection:influxdb3>`.
        Defaults to ``influxdb3_default``.
    :param fail_on_empty: Fail instead of waiting when the query returns no rows. Defaults to ``False``.
    :param deferrable: Run polling in the triggerer. Defaults to the
        ``operators.default_deferrable`` configuration (``False`` if unset).
    """

    template_fields: Sequence[str] = ("sql", "influxdb3_conn_id")
    template_ext: Sequence[str] = (".sql",)

    def __init__(
        self,
        *,
        sql: str,
        influxdb3_conn_id: str = "influxdb3_default",
        fail_on_empty: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.influxdb3_conn_id = influxdb3_conn_id
        self.fail_on_empty = fail_on_empty
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        """Return whether the query result meets the sensor condition."""
        self.log.info("Poking with SQL query: %s", self.sql)
        dataframe = InfluxDB3Hook(conn_id=self.influxdb3_conn_id).query(self.sql)
        if dataframe.empty and self.fail_on_empty:
            raise AirflowFailException("No rows returned, raising as per fail_on_empty flag")
        return _first_cell_is_truthy(dataframe)

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context)
            return

        if self.poke(context):
            return

        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=InfluxDB3SensorTrigger(
                sql=self.sql,
                influxdb3_conn_id=self.influxdb3_conn_id,
                poll_interval=self.poke_interval,
                fail_on_empty=self.fail_on_empty,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        """Complete after the trigger reports that the condition was met."""
        if event is None:
            raise RuntimeError("InfluxDB 3 sensor did not return an event")

        status = event.get("status")
        if status == "fail":
            raise AirflowFailException(event.get("message", "InfluxDB 3 sensor failed"))
        if status == "error":
            raise RuntimeError(event.get("message", "InfluxDB 3 sensor failed"))
        if status != "success":
            raise RuntimeError(f"InfluxDB 3 sensor returned unexpected status: {status!r}")

        self.log.info("InfluxDB 3 sensor condition met")
