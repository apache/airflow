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

import asyncio
import time
from typing import Any

from asgiref.sync import sync_to_async

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DatabricksDeltaTableVersionTrigger(BaseTrigger):
    """
    Asynchronously poll a Databricks Delta table until a newer committed version is observed.

    :param table_name: Fully qualified or local Delta table name.
    :param databricks_conn_id: Reference to Databricks connection ID.
    :param baseline_version: The baseline Delta version to compare against.
    :param target_version: Optional explicit target Delta version.
    :param allow_recreation: Whether to treat a lower version (table recreation) as a version change.
    :param sql_warehouse_name: Optional name of Databricks SQL warehouse.
    :param http_path: Optional HTTP path of Databricks SQL warehouse.
    :param catalog: Initial catalog name.
    :param schema: Initial schema name.
    :param polling_period_seconds: Polling interval in seconds.
    :param end_time: Absolute epoch timestamp when the trigger must timeout.
    :param caller: Name of the caller for hook tracing.
    """

    def __init__(
        self,
        table_name: str,
        databricks_conn_id: str,
        baseline_version: int | None = None,
        target_version: int | None = None,
        allow_recreation: bool = True,
        sql_warehouse_name: str | None = None,
        http_path: str | None = None,
        catalog: str = "",
        schema: str = "default",
        session_configuration: dict[str, str] | None = None,
        http_headers: list[tuple[str, str]] | None = None,
        client_parameters: dict[str, Any] | None = None,
        hook_params: dict[str, Any] | None = None,
        query_tags: dict[str, str | None] | None = None,
        polling_period_seconds: int = 30,
        end_time: float | None = None,
        caller: str = "DatabricksDeltaTableVersionTrigger",
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.databricks_conn_id = databricks_conn_id
        self.baseline_version = baseline_version
        self._baseline_version = baseline_version
        self.target_version = target_version
        self.allow_recreation = allow_recreation
        self.sql_warehouse_name = sql_warehouse_name
        self.http_path = http_path
        self.catalog = catalog
        self.schema = schema
        self.session_configuration = session_configuration
        self.http_headers = http_headers
        self.client_parameters = client_parameters or {}
        self.hook_params = hook_params or {}
        self.query_tags = query_tags
        self.polling_period_seconds = polling_period_seconds
        self.end_time = end_time if end_time is not None else (time.time() + 3600)
        self.caller = caller

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.databricks_delta_table.DatabricksDeltaTableVersionTrigger",
            {
                "table_name": self.table_name,
                "databricks_conn_id": self.databricks_conn_id,
                "baseline_version": self._baseline_version,
                "target_version": self.target_version,
                "allow_recreation": self.allow_recreation,
                "sql_warehouse_name": self.sql_warehouse_name,
                "http_path": self.http_path,
                "catalog": self.catalog,
                "schema": self.schema,
                "session_configuration": self.session_configuration,
                "http_headers": self.http_headers,
                "client_parameters": self.client_parameters,
                "hook_params": self.hook_params,
                "query_tags": self.query_tags,
                "polling_period_seconds": self.polling_period_seconds,
                "end_time": self.end_time,
                "caller": self.caller,
            },
        )

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            databricks_conn_id=self.databricks_conn_id,
            http_path=self.http_path,
            sql_endpoint_name=self.sql_warehouse_name,
            session_configuration=self.session_configuration,
            http_headers=self.http_headers,
            catalog=self.catalog,
            schema=self.schema,
            caller=self.caller,
            query_tags=self.query_tags,
            **self.client_parameters,
            **self.hook_params,
        )

    def _get_version(self) -> tuple[int | None, str | None, str | None]:
        hook = self._get_hook()
        sql = f"DESCRIBE HISTORY {self.table_name} LIMIT 1"
        result = hook.run(sql, handler=fetch_all_handler)
        if not result:
            return None, None, None
        row = result[0]
        version = int(row[0])
        timestamp = str(row[1]) if len(row) > 1 and row[1] is not None else None
        operation = str(row[4]) if len(row) > 4 and row[4] is not None else None
        return version, timestamp, operation

    @staticmethod
    def _is_permanent_error(exc: Exception) -> bool:
        """
        Check if an error querying Delta table version is permanent.

        Permanent failures include missing table/view errors and authorization/permission failures.
        Transient failures (e.g. connection drops, network timeouts) should be retried.
        """
        if isinstance(exc, PermissionError):
            return True
        msg = str(exc).upper()
        permanent_patterns = (
            "TABLE_OR_VIEW_NOT_FOUND",
            "TABLE OR VIEW NOT FOUND",
            "PERMISSION_DENIED",
            "PERMISSION DENIED",
            "ACCESS_DENIED",
            "ACCESS DENIED",
            "DOES NOT EXIST",
            "NOT FOUND",
            "UNAUTHORIZED",
            "FORBIDDEN",
            "ANALYSISEXCEPTION",
            "PARSE_SYNTAX_ERROR",
        )
        return any(pattern in msg for pattern in permanent_patterns)

    async def run(self):
        while time.time() < self.end_time:
            try:
                version, timestamp, operation = await sync_to_async(self._get_version)()
                if version is not None:
                    if self._baseline_version is None and self.target_version is None:
                        self._baseline_version = version
                        self.log.info(
                            "Initialized baseline version to %s for table %s; waiting for newer version.",
                            version,
                            self.table_name,
                        )
                    else:
                        satisfied = False
                        if self.target_version is not None:
                            satisfied = version >= self.target_version
                        elif self._baseline_version is not None:
                            if version > self._baseline_version:
                                satisfied = True
                            elif version < self._baseline_version:
                                if self.allow_recreation:
                                    self.log.info(
                                        "Detected table recreation for %s: current version %s is less than baseline %s",
                                        self.table_name,
                                        version,
                                        self._baseline_version,
                                    )
                                    satisfied = True
                                else:
                                    yield TriggerEvent(
                                        {
                                            "status": "error",
                                            "message": (
                                                f"Table '{self.table_name}' was recreated: current version "
                                                f"{version} is less than baseline version {self._baseline_version}"
                                            ),
                                        }
                                    )
                                    return

                        if satisfied:
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "version": version,
                                    "baseline_version": self._baseline_version,
                                    "table_name": self.table_name,
                                    "timestamp": timestamp,
                                    "operation": operation,
                                }
                            )
                            return
            except Exception as e:
                if self._is_permanent_error(e):
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": (
                                f"Failed to query Delta table '{self.table_name}' version due to "
                                f"permanent error: {e}"
                            ),
                        }
                    )
                    return
                self.log.warning(
                    "Transient error querying Delta table version for %s: %s", self.table_name, e
                )

            now = time.time()
            if now >= self.end_time:
                break
            await asyncio.sleep(min(self.polling_period_seconds, max(0.1, self.end_time - now)))

        yield TriggerEvent(
            {
                "status": "timeout",
                "message": f"Timed out waiting for new Delta table version on '{self.table_name}'.",
            }
        )
