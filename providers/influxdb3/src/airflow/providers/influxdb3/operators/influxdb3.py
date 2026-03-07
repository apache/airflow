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

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pandas as pd

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.influxdb3.hooks.influxdb3 import InfluxDB3Hook

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class InfluxDB3Operator(BaseOperator):
    """
    Execute SQL query in InfluxDB 3.x database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:InfluxDB3Operator`

    :param sql: The SQL query to be executed
    :param influxdb3_conn_id: Reference to :ref:`InfluxDB 3 connection id <howto/connection:influxdb3>`.
    """

    template_fields: Sequence[str] = ("sql",)

    def __init__(
        self,
        *,
        sql: str,
        influxdb3_conn_id: str = "influxdb3_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.influxdb3_conn_id = influxdb3_conn_id
        self.sql = sql

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """
        Execute SQL query and return results as JSON-serializable list of dictionaries.

        :param context: Airflow context
        :return: List of dictionaries representing query results
        """
        self.log.info("Executing SQL query: %s", self.sql)
        hook = InfluxDB3Hook(conn_id=self.influxdb3_conn_id)
        result = hook.query(self.sql)

        self.log.info("Query executed successfully. Rows returned: %d", len(result))

        json_str = result.to_json(orient="records", date_format="iso")
        return json.loads(json_str)
