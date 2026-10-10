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

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    AirflowFailException,
    AirflowSensorTimeout,
    BaseSensorOperator,
    conf,
)
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.triggers.databricks_delta_table import DatabricksDeltaTableVersionTrigger
from airflow.providers.databricks.utils.query_tags import build_query_tags

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.databricks.assets.databricks import UnityTableIdentity


class DatabricksDeltaTableVersionSensor(BaseSensorOperator):
    """
    Sensor to observe Databricks Delta table commits and detect version changes.

    Monitors a Delta table by polling its commit history via ``DESCRIBE HISTORY``.
    Succeeds when a commit version newer than ``baseline_version`` is detected,
    or when ``target_version`` is reached.

    If ``baseline_version`` is not specified and ``target_version`` is None, the sensor
    captures the current version on its first poke as the baseline and waits for a newer
    commit on subsequent evaluations.

    :param table_name: Name of the Delta table (e.g. ``catalog.schema.table``, ``schema.table``, or ``table``).
    :param unity_table: Optional static :class:`~airflow.providers.databricks.assets.databricks.UnityTableIdentity`.
        If specified and ``outlets`` is not provided, defaults the outlet asset to ``unity_table.to_asset()``.
    :param baseline_version: Baseline Delta version to observe changes against.
    :param target_version: Optional explicit target Delta version to wait for.
    :param allow_recreation: Whether to treat a lower version (table dropped and recreated) as a change.
        Defaults to True.
    :param databricks_conn_id: Reference to Databricks connection ID, defaults to DatabricksSqlHook.default_conn_name.
    :param sql_warehouse_name: Optional name of Databricks SQL warehouse.
    :param http_path: Optional HTTP path of Databricks SQL warehouse or cluster.
    :param catalog: Initial catalog name, defaults to "".
    :param schema: Initial schema name, defaults to "default".
    :param session_configuration: Optional dictionary of Spark session parameters.
    :param http_headers: Optional list of (key, value) pairs for HTTP headers.
    :param client_parameters: Additional parameters passed to the Databricks SQL connector.
    :param query_tags: Optional dictionary of query tags.
    :param include_airflow_query_tags: Whether to attach Airflow metadata query tags. Defaults to True.
    :param deferrable: Whether to run the sensor in deferrable mode.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "catalog",
        "schema",
        "table_name",
        "baseline_version",
        "target_version",
        "http_headers",
        "query_tags",
    )

    def __init__(
        self,
        *,
        table_name: str | None = None,
        unity_table: UnityTableIdentity | None = None,
        baseline_version: int | None = None,
        target_version: int | None = None,
        allow_recreation: bool = True,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        sql_warehouse_name: str | None = None,
        http_path: str | None = None,
        catalog: str = "",
        schema: str = "default",
        session_configuration: dict | None = None,
        http_headers: list[tuple[str, str]] | None = None,
        client_parameters: dict[str, Any] | None = None,
        query_tags: dict[str, str | None] | None = None,
        include_airflow_query_tags: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        outlets: list | None = None,
        **kwargs,
    ) -> None:
        if table_name is None and unity_table is None:
            raise ValueError("One of 'table_name' or 'unity_table' must be provided.")

        self.unity_table = unity_table
        if outlets is None and unity_table is not None:
            outlets = [unity_table.to_asset()]

        hook_params = kwargs.pop("hook_params", {})
        super().__init__(outlets=outlets, **kwargs)

        self.table_name = table_name
        self.baseline_version = baseline_version
        self._baseline_version: int | None = None
        self._baseline_initialized: bool = False
        self.target_version = target_version
        self.allow_recreation = allow_recreation
        self.databricks_conn_id = databricks_conn_id
        self._sql_warehouse_name = sql_warehouse_name
        self._http_path = http_path
        self.catalog = catalog
        self.schema = schema
        self.session_config = session_configuration
        self.session_configuration = session_configuration
        self.http_headers = http_headers
        self.client_parameters = client_parameters or {}
        self.hook_params = hook_params
        self.query_tags = query_tags or {}
        self.include_airflow_query_tags = include_airflow_query_tags
        self.deferrable = deferrable
        self.caller = "DatabricksDeltaTableVersionSensor"

    def _ensure_baseline_initialized(self) -> None:
        """Initialize runtime baseline version from templated baseline_version if not yet initialized."""
        if not self._baseline_initialized:
            if self.baseline_version is not None:
                self._baseline_version = int(self.baseline_version)
            else:
                self._baseline_version = None
            self._baseline_initialized = True

    @cached_property
    def hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_warehouse_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            caller=self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    def _validate_workspace_host(self) -> None:
        """Validate connection host matches unity_table host when unity_table is specified."""
        if self.unity_table is not None:
            if (self.hook.host or "").lower() != self.unity_table.host:
                raise ValueError(
                    f"Databricks connection host {self.hook.host!r} does not match "
                    f"unity_table host {self.unity_table.host!r}."
                )

    def _resolve_table_name(self) -> str:
        """Resolve and validate the effective table name."""
        if self.unity_table is not None:
            unity_canonical = f"{self.unity_table.catalog}.{self.unity_table.schema}.{self.unity_table.table}"
            if self.table_name:
                parts = [p.lower() for p in self.table_name.split(".")]
                if len(parts) == 3:
                    if (parts[0], parts[1], parts[2]) != (
                        self.unity_table.catalog,
                        self.unity_table.schema,
                        self.unity_table.table,
                    ):
                        raise ValueError(
                            f"table_name {self.table_name!r} does not match unity_table {unity_canonical!r}."
                        )
                elif len(parts) == 2:
                    if (parts[0], parts[1]) != (self.unity_table.schema, self.unity_table.table):
                        raise ValueError(
                            f"table_name {self.table_name!r} does not match unity_table {unity_canonical!r}."
                        )
                elif len(parts) == 1:
                    if parts[0] != self.unity_table.table:
                        raise ValueError(
                            f"table_name {self.table_name!r} does not match unity_table {unity_canonical!r}."
                        )
                else:
                    raise ValueError(f"Invalid table_name format {self.table_name!r}.")
            return unity_canonical

        if not self.table_name:
            raise ValueError("table_name cannot be empty.")
        return self.table_name

    def _get_latest_version(self, table_name: str) -> tuple[int, str | None, str | None]:
        sql = f"DESCRIBE HISTORY {table_name} LIMIT 1"
        try:
            result = self.hook.run(sql, handler=fetch_all_handler)
        except Exception as e:
            if DatabricksDeltaTableVersionTrigger._is_permanent_error(e):
                raise AirflowFailException(
                    f"Failed to fetch Delta table history for '{table_name}'. "
                    "Ensure the table exists, is a Delta table, and the caller has read permissions: "
                    f"{e}"
                ) from e
            raise

        if not isinstance(result, Sequence) or not result:
            raise AirflowFailException(f"Delta table '{table_name}' returned empty history.")

        row = result[0]
        if not isinstance(row, Sequence) or not row:
            raise AirflowFailException(f"Delta table '{table_name}' returned malformed history.")

        version_raw = row[0]
        if not isinstance(version_raw, (int, str)):
            raise AirflowFailException(
                f"Delta table '{table_name}' returned invalid version {version_raw!r}."
            )
        version = int(version_raw)
        timestamp = str(row[1]) if len(row) > 1 and row[1] is not None else None
        operation = str(row[4]) if len(row) > 4 and row[4] is not None else None
        return version, timestamp, operation

    def _evaluate_version(self, current_version: int, table_name: str) -> bool:
        """Evaluate if the current version meets sensor criteria."""
        if self._baseline_version is None and self.target_version is None:
            self._baseline_version = current_version
            self.log.info(
                "Initialized baseline version to %s for table '%s'; waiting for subsequent commits.",
                current_version,
                table_name,
            )
            return False

        if self.target_version is not None:
            if current_version >= self.target_version:
                self.log.info(
                    "Target version %s reached for table '%s' (current version: %s).",
                    self.target_version,
                    table_name,
                    current_version,
                )
                return True
            return False

        if self._baseline_version is not None:
            if current_version > self._baseline_version:
                self.log.info(
                    "Observed new Delta version %s (baseline: %s) for table '%s'.",
                    current_version,
                    self._baseline_version,
                    table_name,
                )
                return True
            if current_version < self._baseline_version:
                if self.allow_recreation:
                    self.log.info(
                        "Observed table recreation: current version %s is less than baseline %s for table '%s'.",
                        current_version,
                        self._baseline_version,
                        table_name,
                    )
                    return True
                raise AirflowFailException(
                    f"Delta table '{table_name}' was recreated: current version {current_version} "
                    f"is less than baseline version {self._baseline_version}."
                )

        return False

    def _push_provenance(
        self,
        context: Context,
        current_version: int,
        table_name: str,
        timestamp: str | None,
        operation: str | None,
    ) -> dict[str, Any]:
        table_identity: dict[str, JsonValue]
        if self.unity_table:
            table_identity = {
                "host": self.unity_table.host,
                "catalog": self.unity_table.catalog,
                "schema": self.unity_table.schema,
                "table": self.unity_table.table,
            }
        else:
            parts = table_name.split(".")
            if len(parts) == 3:
                table_identity = {
                    "catalog": parts[0],
                    "schema": parts[1],
                    "table": parts[2],
                }
            elif len(parts) == 2:
                table_identity = {
                    "schema": parts[0],
                    "table": parts[1],
                }
            else:
                table_identity = {"table": table_name}

        provenance = {
            "version": current_version,
            "baseline_version": self._baseline_version,
            "table_name": table_name,
            "table_identity": table_identity,
            "timestamp": timestamp,
            "operation": operation,
        }
        if self.do_xcom_push and context is not None and "ti" in context and context["ti"]:
            context["ti"].xcom_push(key="delta_table_version", value=provenance)

        # Emit asset event metadata if outlet_events is supported in context.
        # As established on #74196 and #74195, the task-success asset event means
        # "a newer Delta version was observed," with version + table identity in extra.
        # It is not a vague "table refreshed" and not a promise that every intermediate
        # commit was delivered. Authors needing pinned reads must VERSION AS OF that observed version.
        if context is not None and "outlet_events" in context:
            extra: dict[str, JsonValue] = {
                "version": current_version,
                "observed_version": current_version,
                "table_identity": table_identity,
                "operation": operation,
            }
            if timestamp is not None:
                extra["timestamp"] = timestamp
            for outlet in self.outlets:
                context["outlet_events"][outlet].extra = extra
        return provenance

    def poke(self, context: Context) -> bool:
        self._ensure_baseline_initialized()
        self._validate_workspace_host()
        self.hook.query_tags = build_query_tags(context, self.query_tags, self.include_airflow_query_tags)
        table_name = self._resolve_table_name()
        current_version, timestamp, operation = self._get_latest_version(table_name)

        if self._evaluate_version(current_version, table_name):
            self._push_provenance(context, current_version, table_name, timestamp, operation)
            return True
        return False

    def execute(self, context: Context) -> Any:
        self._ensure_baseline_initialized()
        self._validate_workspace_host()
        table_name = self._resolve_table_name()
        if not self.deferrable:
            return super().execute(context)

        query_tags = build_query_tags(context, self.query_tags, self.include_airflow_query_tags)

        # If baseline is not provided and target_version is not set, capture baseline synchronously first
        if self._baseline_version is None and self.target_version is None:
            self.hook.query_tags = query_tags
            current_version, _, _ = self._get_latest_version(table_name)
            self._baseline_version = current_version
            self.log.info(
                "Initialized baseline version to %s for table '%s' before deferring.",
                current_version,
                table_name,
            )

        end_time = time.time() + self.timeout
        self.defer(
            trigger=DatabricksDeltaTableVersionTrigger(
                table_name=table_name,
                databricks_conn_id=self.databricks_conn_id,
                baseline_version=self._baseline_version,
                target_version=self.target_version,
                allow_recreation=self.allow_recreation,
                sql_warehouse_name=self._sql_warehouse_name,
                http_path=self._http_path,
                catalog=self.catalog,
                schema=self.schema,
                session_configuration=self.session_config,
                http_headers=self.http_headers,
                client_parameters=self.client_parameters,
                hook_params=self.hook_params,
                query_tags=query_tags,
                polling_period_seconds=int(self.poke_interval),
                end_time=end_time,
                caller=self.caller,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        self._ensure_baseline_initialized()
        if not event:
            raise AirflowFailException("Trigger did not return an event.")
        if event.get("status") == "success":
            table_name = event.get("table_name") or self._resolve_table_name()
            version = event["version"]
            if "baseline_version" in event:
                self._baseline_version = event["baseline_version"]
            self._push_provenance(
                context,
                version,
                table_name,
                event.get("timestamp"),
                event.get("operation"),
            )
            self.log.info("Successfully detected Delta table '%s' version: %s.", table_name, version)
            return version
        if event.get("status") == "timeout":
            raise AirflowSensorTimeout(event.get("message", "Sensor timed out waiting for Delta table version."))
        raise AirflowFailException(event.get("message", "Sensor deferred execution failed."))
