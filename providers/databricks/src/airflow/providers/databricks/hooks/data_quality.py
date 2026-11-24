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
"""Refresh Databricks data profiling monitors."""

from __future__ import annotations

import asyncio
import math
from time import monotonic
from typing import Any
from urllib.parse import quote

from airflow.providers.databricks.exceptions import DatabricksDataQualityError, DatabricksDataQualityTimeout
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

_REFRESH_ENDPOINT = "data-quality/v1/monitors/table"
_PENDING_STATES = frozenset({"MONITOR_REFRESH_STATE_PENDING", "MONITOR_REFRESH_STATE_RUNNING"})
_FAILURE_STATES = frozenset({"MONITOR_REFRESH_STATE_FAILED", "MONITOR_REFRESH_STATE_CANCELED"})


class DatabricksDataQualityHook(BaseDatabricksHook):
    """Use the Data Quality Monitoring REST API with a Databricks connection."""

    def get_table_id(self, table_name: str) -> str:
        """
        Resolve a Unity Catalog table name to its ID.

        :param table_name: Three-level Unity Catalog name.
        """
        if not table_name:
            raise ValueError("table_name must be provided.")
        table = self._do_api_call(("GET", f"2.1/unity-catalog/tables/{quote(table_name, safe='')}"))
        table_id = table.get("table_id")
        if not isinstance(table_id, str) or not table_id:
            raise DatabricksDataQualityError("Databricks returned a table without a table_id.")
        return table_id

    def create_refresh(self, table_id: str) -> int:
        """
        Start a refresh of an existing table monitor and return its refresh ID.

        :param table_id: Unity Catalog table ID.
        """
        if not table_id:
            raise ValueError("table_id must be provided.")
        refresh = self._do_api_call(("POST", f"{_REFRESH_ENDPOINT}/{quote(table_id, safe='')}/refreshes"), {})
        refresh_id = refresh.get("refresh_id")
        if type(refresh_id) is not int or refresh_id < 0:
            raise DatabricksDataQualityError("Databricks returned a refresh without a valid refresh_id.")
        return refresh_id

    async def a_get_refresh(self, table_id: str, refresh_id: int) -> dict[str, Any]:
        """
        Get the status of a specific table monitor refresh.

        :param table_id: Unity Catalog table ID.
        :param refresh_id: Refresh ID returned on submission.
        """
        return await self._a_do_api_call(
            ("GET", f"{_REFRESH_ENDPOINT}/{quote(table_id, safe='')}/refreshes/{refresh_id}")
        )

    @staticmethod
    def validate_wait_parameters(timeout: float, polling_period_seconds: float) -> None:
        """Validate the wait budget and polling interval before creating a refresh."""
        for name, value in (("timeout", timeout), ("polling_period_seconds", polling_period_seconds)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and greater than zero.")

    async def a_wait_for_refresh(
        self, table_id: str, refresh_id: int, *, timeout: float, polling_period_seconds: float = 30
    ) -> None:
        """
        Wait for the submitted refresh, managing the HTTP session within the timeout.

        :param table_id: Unity Catalog table ID.
        :param refresh_id: Refresh ID returned on submission.
        :param timeout: Maximum wait in seconds, including status requests and retries.
        :param polling_period_seconds: Interval between status requests, in seconds.
        """
        self.validate_wait_parameters(timeout, polling_period_seconds)
        deadline = monotonic() + timeout
        timeout_message = f"Data quality refresh {refresh_id} did not complete within {timeout}s."
        budget = asyncio.timeout(timeout)
        try:
            async with budget, self:
                while True:
                    if monotonic() >= deadline:
                        raise DatabricksDataQualityTimeout(timeout_message)
                    refresh = await self.a_get_refresh(table_id, refresh_id)
                    if monotonic() >= deadline:
                        raise DatabricksDataQualityTimeout(timeout_message)
                    if (
                        type(refresh.get("refresh_id")) is not int
                        or refresh["refresh_id"] != refresh_id
                        or refresh.get("object_id", table_id) != table_id
                        or refresh.get("object_type", "table") != "table"
                    ):
                        raise DatabricksDataQualityError("Databricks returned a different refresh identity.")
                    state = refresh.get("state")
                    if not isinstance(state, str):
                        state = None
                    if state == "MONITOR_REFRESH_STATE_SUCCESS":
                        return
                    if state in _FAILURE_STATES:
                        raise DatabricksDataQualityError(
                            f"Data quality refresh {refresh_id} entered {state}."
                        )
                    if state not in _PENDING_STATES:
                        raise DatabricksDataQualityError(
                            f"Data quality refresh {refresh_id} returned an unrecognized state."
                        )
                    self.log.info("Data quality refresh %s is %s; waiting.", refresh_id, state)
                    await asyncio.sleep(min(polling_period_seconds, max(0, deadline - monotonic())))
        except TimeoutError as err:
            if not budget.expired():
                raise
            raise DatabricksDataQualityTimeout(timeout_message) from err
