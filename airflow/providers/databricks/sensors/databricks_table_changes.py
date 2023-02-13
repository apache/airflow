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
#
"""This module contains Databricks sensors."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.utils.context import Context


class DatabricksTableChangesSensor(DatabricksSqlSensor):
    """Sensor to detect changes in a Delta table.


    :param databricks_conn_id: Reference to :ref:`Databricks
        connection id<howto/connection:databricks>` (templated), defaults to
        DatabricksSqlHook.default_conn_name
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's
        extra parameters, or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint. If not specified, ``http_path`` must
        be provided as described above, defaults to None
    :param session_configuration: An optional dictionary of Spark session parameters. If not specified,
        it could be specified in the Databricks connection's extra parameters., defaults to None
    :param http_headers: An optional list of (k, v) pairs
        that will be set as HTTP headers on every request. (templated).
    :param catalog: An optional initial catalog to use.
        Requires DBR version 9.0+ (templated), defaults to ""
    :param schema: An optional initial schema to use.
        Requires DBR version 9.0+ (templated), defaults to "default"
    :param table_name: Table name to generate the SQL query, defaults to ""
    :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_all_handler
    :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters.
    :param timestamp: Timestamp to check event history for a Delta table.
    :param change_filter_operator: Operator to specify filter condition to check table changes,
        defaults to >=.
    """

    template_fields: Sequence[str] = ("databricks_conn_id", "catalog", "schema", "table_name")

    def __init__(
        self,
        table_name: str,
        timestamp: datetime | None = None,
        change_filter_operator: str = ">=",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.timestamp = timestamp
        self.caller = "DatabricksTableChangesSensor"
        self.change_filter_operator = change_filter_operator
        self.table_name = table_name

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_endpoint_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    def _sql_sensor(self, sql):
        hook = self._get_hook()
        sql_result = hook.run(
            sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        return sql_result

    def _generate_query(
        self,
        prefix: str,
        suffix: str,
        table_name: str,
        time_range_filter: str,
    ) -> str:
        formatted_opts = f"{prefix} {table_name}{suffix}{time_range_filter}"
        self.log.debug("Formatted options: %s", formatted_opts)

        return formatted_opts.strip()

    @staticmethod
    def get_previous_version(context: Context, lookup_key):
        return context["ti"].xcom_pull(key=lookup_key, include_prior_dates=True)

    @staticmethod
    def set_version(context: Context, lookup_key, version):
        context["ti"].xcom_push(key=lookup_key, value=version)

    def get_current_table_version(self, table_name):
        _prefix = "SELECT MAX(version) AS versions FROM (DESCRIBE HISTORY"
        _data_operations_filter = ") WHERE operation NOT IN ('CONVERT', 'OPTIMIZE', 'CLONE', \
        'RESTORE', 'FSCK', 'VACUUM START', 'VACUUM END')"
        if self.timestamp is not None:
            if self.change_filter_operator not in ("=", ">", "<", ">=", "<=", "!="):
                raise AirflowException("Invalid comparison operator specified for time range filter.")
            _timestamp_literal = f" AND timestamp {self.change_filter_operator} '{self.timestamp}'"
        else:
            _timestamp_literal = ""
        query = self._generate_query(
            prefix=_prefix,
            suffix=_data_operations_filter,
            time_range_filter=_timestamp_literal,
            table_name=table_name,
        )
        self.log.debug("Query to be executed: %s", query)
        result = self._sql_sensor(query)[0][0]
        self.log.debug("Query result: %s", result)
        return result

    def _get_results_table_changes(self, context) -> bool:
        complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
        self.log.debug("Table name generated from arguments: %s", complete_table_name)

        prev_version = -1
        if context is not None:
            lookup_key = complete_table_name
            prev_data = self.get_previous_version(lookup_key=lookup_key, context=context)
            self.log.debug("prev_data: %s, type=%s", str(prev_data), type(prev_data))
            if isinstance(prev_data, int):
                prev_version = prev_data
            elif prev_data is not None:
                raise AirflowException("Incorrect type for previous XCom data: %s", type(prev_data))
            version = self.get_current_table_version(table_name=complete_table_name)
            self.log.debug("Current version: %s", version)
            if version is None:
                raise AirflowException("No current version of the table found for the specified timeframe.")
            if prev_version < version:
                result = True
            else:
                raise AirflowException("No new version found.")
            if prev_version != version:
                self.set_version(lookup_key=lookup_key, version=version, context=context)
            self.log.debug("Result: %s", result)
            return result
        return False

    def poke(self, context: Context) -> bool:
        return self._get_results_table_changes(context=context)
