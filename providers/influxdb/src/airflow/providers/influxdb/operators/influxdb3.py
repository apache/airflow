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
"""Operator for executing SQL queries in InfluxDB 3.x."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.influxdb.hooks.influxdb3 import InfluxDB3Hook, _convert_dataframe_to_records
from airflow.providers.influxdb.triggers.influxdb3 import InfluxDB3QueryTrigger

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class InfluxDB3Operator(BaseOperator):
    """
    Execute SQL query in InfluxDB 3.x database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:InfluxDB3Operator`

    :param sql: The SQL query to be executed
    :param influxdb3_conn_id: Reference to :ref:`InfluxDB 3 connection id <howto/connection:influxdb3>`.
    :param deferrable: Run the query from the triggerer so the worker slot is released while the
        query runs. This is most useful for long-running queries that return small-to-moderate
        result sets because the full result still flows back through XCom.
    """

    template_fields: Sequence[str] = ("sql",)

    def __init__(
        self,
        *,
        sql: str,
        influxdb3_conn_id: str = "influxdb3_default",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.influxdb3_conn_id = influxdb3_conn_id
        self.sql = sql
        self.deferrable = deferrable

    def execute(self, context: Context) -> list[dict[str, Any]] | None:
        """
        Execute SQL query and return results as JSON-serializable list of dictionaries.

        :param context: Airflow context
        :return: List of dictionaries representing query results, or ``None`` when deferring
        """
        self.log.info("Executing SQL query: %s", self.sql)

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=InfluxDB3QueryTrigger(
                    sql=self.sql,
                    influxdb3_conn_id=self.influxdb3_conn_id,
                ),
                method_name="execute_complete",
            )

        hook = InfluxDB3Hook(conn_id=self.influxdb3_conn_id)
        result = hook.query(self.sql)

        self.log.info("Query executed successfully. Rows returned: %d", len(result))
        return _convert_dataframe_to_records(result)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Return the query results produced by :class:`InfluxDB3QueryTrigger`."""
        if event is None:
            raise RuntimeError("InfluxDB 3 query did not return an event")

        status = event.get("status")
        if status == "error":
            raise RuntimeError(event.get("message", "InfluxDB 3 query failed"))
        if status != "success":
            raise RuntimeError(f"InfluxDB 3 query returned unexpected status: {status!r}")

        records = event["records"]
        self.log.info("Query executed successfully. Rows returned: %d", len(records))
        return records
