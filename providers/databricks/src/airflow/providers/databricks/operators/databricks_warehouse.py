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
"""Operators for managing Databricks SQL warehouse lifecycle state."""

from __future__ import annotations

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.databricks.exceptions import DatabricksWarehouseError
from airflow.providers.databricks.hooks.databricks import DatabricksHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class _DatabricksWarehouseBaseOperator(BaseOperator):
    """Share Databricks SQL warehouse connection and polling behavior."""

    template_fields: Sequence[str] = ("databricks_conn_id", "warehouse_id")
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        warehouse_id: str,
        *,
        databricks_conn_id: str = "databricks_default",
        wait_for_termination: bool = True,
        polling_period_seconds: int = 30,
        timeout: float = 3600,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.warehouse_id = warehouse_id
        self.databricks_conn_id = databricks_conn_id
        self.wait_for_termination = wait_for_termination
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args

    def _validate_warehouse_id(self) -> None:
        if not self.warehouse_id:
            raise ValueError("warehouse_id must be provided.")

    @cached_property
    def _hook(self) -> DatabricksHook:
        return self._get_hook(caller=self.__class__.__name__)

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def _wait_for_state(self, target: str, failure_states: set[str]) -> None:
        deadline = time.monotonic() + self.timeout
        last_state = "unknown"
        while time.monotonic() < deadline:
            state = self._hook.get_warehouse_state(self.warehouse_id)
            last_state = state.state
            now = time.monotonic()
            if now >= deadline:
                break
            if state.state == target:
                return
            if state.state in failure_states:
                raise DatabricksWarehouseError(
                    f"Databricks SQL warehouse {self.warehouse_id} entered {state.state} "
                    f"while waiting for {target}."
                )
            self.log.info(
                "Databricks SQL warehouse %s is %s; waiting for %s.",
                self.warehouse_id,
                state.state,
                target,
            )
            time.sleep(min(self.polling_period_seconds, deadline - now))
        raise DatabricksWarehouseError(
            f"Databricks SQL warehouse {self.warehouse_id} did not reach {target} "
            f"within {self.timeout}s; last state: {last_state}."
        )


class DatabricksStartWarehouseOperator(_DatabricksWarehouseBaseOperator):
    """
    Start a Databricks SQL warehouse and optionally wait for it to run.

    :param warehouse_id: ID of the Databricks SQL warehouse. (templated)
    :param databricks_conn_id: Reference to the Databricks connection. (templated)
    :param wait_for_termination: Wait until the warehouse reaches ``RUNNING``.
    :param polling_period_seconds: Number of seconds between state checks.
    :param timeout: Maximum number of seconds to wait for the target state.
    :param databricks_retry_limit: Number of times to retry unavailable Databricks requests.
    :param databricks_retry_delay: Number of seconds between Databricks request retries.
    :param databricks_retry_args: Additional arguments for ``tenacity.Retrying``.
    """

    def execute(self, context: Context) -> None:
        self._validate_warehouse_id()
        state = self._hook.get_warehouse_state(self.warehouse_id)
        if state.is_running:
            self.log.info("Databricks SQL warehouse %s is already running.", self.warehouse_id)
            return
        if state.state != "STARTING":
            self._hook.start_warehouse(self.warehouse_id)
        if self.wait_for_termination:
            self._wait_for_state("RUNNING", {"STOPPED", "DELETING", "DELETED"})


class DatabricksStopWarehouseOperator(_DatabricksWarehouseBaseOperator):
    """
    Stop a Databricks SQL warehouse and optionally wait for it to stop.

    :param warehouse_id: ID of the Databricks SQL warehouse. (templated)
    :param databricks_conn_id: Reference to the Databricks connection. (templated)
    :param wait_for_termination: Wait until the warehouse reaches ``STOPPED``.
    :param polling_period_seconds: Number of seconds between state checks.
    :param timeout: Maximum number of seconds to wait for the target state.
    :param databricks_retry_limit: Number of times to retry unavailable Databricks requests.
    :param databricks_retry_delay: Number of seconds between Databricks request retries.
    :param databricks_retry_args: Additional arguments for ``tenacity.Retrying``.
    """

    def execute(self, context: Context) -> None:
        self._validate_warehouse_id()
        state = self._hook.get_warehouse_state(self.warehouse_id)
        if state.is_stopped:
            self.log.info("Databricks SQL warehouse %s is already stopped.", self.warehouse_id)
            return
        if state.state != "STOPPING":
            self._hook.stop_warehouse(self.warehouse_id)
        if self.wait_for_termination:
            self._wait_for_state("STOPPED", {"DELETING", "DELETED"})
