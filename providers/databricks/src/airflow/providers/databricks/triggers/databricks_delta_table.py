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
from collections.abc import Sequence
from typing import Any, NamedTuple

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.sdk import AirflowFailException
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

DEFAULT_DELTA_HISTORY_LIMIT: int = 50

IGNORED_DELTA_OPERATIONS: frozenset[str] = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
    }
)


def parse_raw_delta_version(result: Sequence[Any] | None) -> int | None:
    """
    Extract the newest raw Delta version from history rows (row 0), regardless of operation.

    :param result: Sequence of rows returned from DESCRIBE HISTORY.
    :return: Integer version of the latest commit, or None if history is empty/invalid.
    """
    if not isinstance(result, Sequence) or not result:
        return None

    for row in result:
        if not isinstance(row, Sequence) or not row:
            continue
        version_raw = row[0]
        if not isinstance(version_raw, (int, str)):
            continue
        try:
            return int(version_raw)
        except (ValueError, TypeError):
            continue

    return None


class DeltaVersionInfo(NamedTuple):
    """Container for parsed Delta table version metadata."""

    effective_version: int | None
    timestamp: str | None
    operation: str | None
    raw_version: int | None


def parse_oldest_raw_delta_version(result: Sequence[Any] | None) -> int | None:
    """
    Extract the oldest raw Delta version from the end of history rows.

    :param result: Sequence of rows returned from DESCRIBE HISTORY.
    :return: Integer version of the oldest commit in the batch, or None if history is empty/invalid.
    """
    if not isinstance(result, Sequence) or not result:
        return None

    for row in reversed(result):
        if not isinstance(row, Sequence) or not row:
            continue
        version_raw = row[0]
        if not isinstance(version_raw, (int, str)):
            continue
        try:
            return int(version_raw)
        except (ValueError, TypeError):
            continue

    return None


def scan_delta_history_version(
    hook: DatabricksSqlHook,
    table_name: str,
    *,
    baseline_version: int | None = None,
    target_version: int | None = None,
    initial_limit: int = DEFAULT_DELTA_HISTORY_LIMIT,
) -> DeltaVersionInfo:
    """
    Adaptively scan Delta table history to identify relevant version changes.

    Starts with a bounded query (initial_limit) to keep standard polling cheap.
    If only maintenance operations are present in the initial window, adaptively expands
    the query window (e.g. 50 -> 100 -> 200 -> ...) until either:
    1. A relevant data-changing commit is found;
    2. History proves no relevant commit newer than the comparison boundary exists; or
    3. The beginning of the table history is reached.

    :param hook: DatabricksSqlHook instance used to query history.
    :param table_name: Qualified or local Delta table name.
    :param baseline_version: Optional baseline version boundary.
    :param target_version: Optional target version boundary.
    :param initial_limit: Initial query limit (default 50).
    :return: DeltaVersionInfo containing effective and raw version metadata.
    """
    limit = initial_limit
    first_query = True
    newest_raw_version: int | None = None

    while True:
        sql = f"DESCRIBE HISTORY {table_name} LIMIT {limit}"
        result = hook.run(sql, handler=fetch_all_handler)

        if not isinstance(result, Sequence) or not result:
            if first_query:
                return DeltaVersionInfo(None, None, None, None)
            return DeltaVersionInfo(None, None, None, newest_raw_version)

        raw_ver = parse_raw_delta_version(result)
        if first_query:
            newest_raw_version = raw_ver
            if newest_raw_version is None:
                return DeltaVersionInfo(None, None, None, None)

        eff_ver, ts, op = parse_effective_delta_version(result)

        # 1. Automatic enrollment (before baseline is established):
        # We only need the newest raw version from the initial window.
        if baseline_version is None and target_version is None:
            return DeltaVersionInfo(eff_ver, ts, op, newest_raw_version)

        # 2. Target version mode:
        if target_version is not None:
            if newest_raw_version is not None and newest_raw_version < target_version:
                return DeltaVersionInfo(eff_ver, ts, op, newest_raw_version)

            if eff_ver is not None:
                return DeltaVersionInfo(eff_ver, ts, op, newest_raw_version)

            oldest_raw = parse_oldest_raw_delta_version(result)
            if (oldest_raw is not None and oldest_raw <= target_version) or len(result) < limit:
                return DeltaVersionInfo(None, None, None, newest_raw_version)

            limit *= 2
            first_query = False
            continue

        # 3. Baseline version mode:
        if baseline_version is not None:
            if newest_raw_version is not None and newest_raw_version == baseline_version:
                return DeltaVersionInfo(eff_ver, ts, op, newest_raw_version)

            if eff_ver is not None:
                return DeltaVersionInfo(eff_ver, ts, op, newest_raw_version)

            oldest_raw = parse_oldest_raw_delta_version(result)
            reached_boundary = (
                (
                    oldest_raw is not None
                    and oldest_raw <= baseline_version
                    and newest_raw_version is not None
                    and newest_raw_version > baseline_version
                )
                or (oldest_raw is not None and oldest_raw <= 0)
                or len(result) < limit
            )
            if reached_boundary:
                return DeltaVersionInfo(None, None, None, newest_raw_version)

            limit *= 2
            first_query = False
            continue


def parse_effective_delta_version(
    result: Sequence[Any] | None,
) -> tuple[int | None, str | None, str | None]:
    """
    Extract the newest data-changing Delta version, timestamp, and operation from history rows.

    Inspects recent history rows in reverse chronological order (as returned by
    ``DESCRIBE HISTORY``) and skips maintenance-only operations such as OPTIMIZE
    and VACUUM.

    :param result: Sequence of rows returned from ``DESCRIBE HISTORY``.
    :return: Tuple of ``(version, timestamp, operation)`` for the newest relevant
        data-changing commit, or ``(None, None, None)`` if no matching commit is found.
    """
    if not isinstance(result, Sequence) or not result:
        return None, None, None

    for row in result:
        if not isinstance(row, Sequence) or not row:
            continue
        version_raw = row[0]
        if not isinstance(version_raw, (int, str)):
            continue
        try:
            version = int(version_raw)
        except (ValueError, TypeError):
            continue

        timestamp = str(row[1]) if len(row) > 1 and row[1] is not None else None
        operation = str(row[4]) if len(row) > 4 and row[4] is not None else None

        if operation is not None and operation.strip().upper() in IGNORED_DELTA_OPERATIONS:
            continue

        return version, timestamp, operation

    return None, None, None


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
        if baseline_version is not None:
            try:
                self.baseline_version: int | None = int(baseline_version)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid baseline_version {baseline_version!r}: must be an integer.") from e
        else:
            self.baseline_version = None
        self._baseline_version = self.baseline_version

        if target_version is not None:
            try:
                self.target_version: int | None = int(target_version)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid target_version {target_version!r}: must be an integer.") from e
        else:
            self.target_version = None
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

    def _get_version(
        self,
        baseline_version: int | None = None,
        target_version: int | None = None,
    ) -> DeltaVersionInfo:
        hook = self._get_hook()
        effective_baseline = baseline_version if baseline_version is not None else self._baseline_version
        effective_target = target_version if target_version is not None else self.target_version
        return scan_delta_history_version(
            hook,
            self.table_name,
            baseline_version=effective_baseline,
            target_version=effective_target,
        )

    @staticmethod
    def _is_permanent_error(exc: Exception) -> bool:
        """
        Check if an error querying Delta table version is permanent.

        Permanent failures include missing table/view errors and authorization/permission failures.
        Transient failures (e.g. connection drops, network timeouts) should be retried.
        """
        if isinstance(exc, (PermissionError, AirflowFailException)):
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
                version_info = await sync_to_async(self._get_version)()
                raw_version = version_info.raw_version
                version = version_info.effective_version
                timestamp = version_info.timestamp
                operation = version_info.operation

                if self._baseline_version is None and self.target_version is None:
                    if raw_version is not None:
                        self._baseline_version = raw_version
                        self.log.info(
                            "Initialized baseline version to %s for table %s; waiting for newer version.",
                            raw_version,
                            self.table_name,
                        )
                else:
                    satisfied = False
                    if self.target_version is not None:
                        satisfied = version is not None and version >= self.target_version
                    elif self._baseline_version is not None:
                        if version is not None and version > self._baseline_version:
                            satisfied = True
                        elif version is not None and version < self._baseline_version:
                            effective_raw = raw_version if raw_version is not None else version
                            if effective_raw < self._baseline_version:
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
