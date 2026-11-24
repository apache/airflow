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
"""Operators for Databricks Data Quality Monitoring."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.databricks.hooks.data_quality import DatabricksDataQualityHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DatabricksDataQualityRefreshOperator(BaseOperator):
    """
    Refresh the metrics of an existing data profiling monitor on a Unity Catalog table.

    Return the submitted ``refresh_id``, after waiting for success by default.

    :param table_name: Three-level Unity Catalog table name. (templated)
    :param databricks_conn_id: Databricks connection ID. Defaults to ``databricks_default``. (templated)
    :param wait_for_completion: Wait for the submitted refresh to succeed. Defaults to ``True``.
    :param polling_period_seconds: Interval between status checks. Defaults to 30 seconds.
    :param timeout: Maximum wait after submission, including status requests and retries. Defaults to 3600 seconds.
    :param databricks_retry_limit: Maximum HTTP attempts, including the initial request. Defaults to 1
        to avoid replaying a refresh submission after an ambiguous failure.
    :param databricks_retry_delay: Minimum delay between retryable HTTP attempts. Defaults to 1 second.
    :param databricks_retry_args: Optional ``tenacity.Retrying`` configuration for HTTP retries.
    """

    template_fields: Sequence[str] = ("table_name", "databricks_conn_id")
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        *,
        table_name: str,
        databricks_conn_id: str = "databricks_default",
        wait_for_completion: bool = True,
        polling_period_seconds: float = 30,
        timeout: float = 3600,
        databricks_retry_limit: int = 1,
        databricks_retry_delay: float = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.databricks_conn_id = databricks_conn_id
        self.wait_for_completion = wait_for_completion
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        DatabricksDataQualityHook.validate_wait_parameters(timeout, polling_period_seconds)

    @cached_property
    def _hook(self) -> DatabricksDataQualityHook:
        return DatabricksDataQualityHook(
            databricks_conn_id=self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=self.__class__.__name__,
        )

    def execute(self, context: Context) -> int:
        table_id = self._hook.get_table_id(self.table_name)
        refresh_id = self._hook.create_refresh(table_id)
        self.log.info("Created data quality refresh %s for table %s.", refresh_id, self.table_name)
        if self.wait_for_completion:
            asyncio.run(
                self._hook.a_wait_for_refresh(
                    table_id,
                    refresh_id,
                    timeout=self.timeout,
                    polling_period_seconds=self.polling_period_seconds,
                )
            )
        return refresh_id
