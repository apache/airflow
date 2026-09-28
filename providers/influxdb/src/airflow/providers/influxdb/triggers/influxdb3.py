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
"""Trigger for running InfluxDB 3.x SQL queries from the triggerer."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from airflow.providers.influxdb.hooks.influxdb3 import InfluxDB3Hook
from airflow.providers.influxdb.utils import _convert_dataframe_to_records, _first_cell_is_truthy
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class InfluxDB3QueryTrigger(BaseTrigger):
    """
    Run a SQL query against InfluxDB 3.x without occupying a worker slot.

    InfluxDB 3 executes queries over a single Apache Arrow Flight stream: there is no
    server-side job to submit and then poll for completion, and therefore no query state
    and no poll interval. The trigger instead awaits the query coroutine once and emits a
    single event when the stream has been fully read.

    ``influxdb3-python`` implements ``query_async`` by running the blocking Flight calls in
    the event loop's default executor, so concurrency in the triggerer is bounded by that
    thread pool rather than by native async IO. This is a limitation of the upstream client,
    not of this trigger.

    :param sql: The SQL query to be executed.
    :param influxdb3_conn_id: Reference to :ref:`InfluxDB 3 connection id <howto/connection:influxdb3>`.
    """

    def __init__(
        self,
        sql: str,
        influxdb3_conn_id: str = "influxdb3_default",
    ) -> None:
        super().__init__()
        self.sql = sql
        self.influxdb3_conn_id = influxdb3_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.influxdb.triggers.influxdb3.InfluxDB3QueryTrigger",
            {
                "sql": self.sql,
                "influxdb3_conn_id": self.influxdb3_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = InfluxDB3Hook(conn_id=self.influxdb3_conn_id)
        try:
            dataframe = await hook.query_async(self.sql)
            records = await asyncio.to_thread(_convert_dataframe_to_records, dataframe)
        except Exception as error:
            self.log.exception("InfluxDB 3 query failed in trigger")
            yield TriggerEvent({"status": "error", "message": str(error)})
            return

        yield TriggerEvent({"status": "success", "records": records})


class InfluxDB3SensorTrigger(BaseTrigger):
    """Poll an InfluxDB 3.x SQL query until its first cell meets the sensor condition."""

    def __init__(
        self,
        sql: str,
        influxdb3_conn_id: str = "influxdb3_default",
        poll_interval: float = 60,
        fail_on_empty: bool = False,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.influxdb3_conn_id = influxdb3_conn_id
        self.poll_interval = poll_interval
        self.fail_on_empty = fail_on_empty

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.influxdb.triggers.influxdb3.InfluxDB3SensorTrigger",
            {
                "sql": self.sql,
                "influxdb3_conn_id": self.influxdb3_conn_id,
                "poll_interval": self.poll_interval,
                "fail_on_empty": self.fail_on_empty,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = InfluxDB3Hook(conn_id=self.influxdb3_conn_id)
        while True:
            try:
                dataframe = await hook.query_async(self.sql)
            except Exception as error:
                self.log.exception("InfluxDB 3 sensor query failed")
                yield TriggerEvent({"status": "error", "message": str(error)})
                return

            if dataframe.empty and self.fail_on_empty:
                yield TriggerEvent(
                    {"status": "fail", "message": "No rows returned, raising as per fail_on_empty flag"}
                )
                return

            if _first_cell_is_truthy(dataframe):
                yield TriggerEvent({"status": "success"})
                return

            await asyncio.sleep(self.poll_interval)
