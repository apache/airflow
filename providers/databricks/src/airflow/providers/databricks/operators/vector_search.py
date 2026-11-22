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
"""Operators for Databricks Vector Search indexes."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DatabricksVectorSearchSyncOperator(BaseOperator):
    """
    Request a synchronization of an existing triggered Delta Sync index.

    By default, wait for the index to report an online state without pending updates, then return ``None``.

    :param index_name: Three-level Unity Catalog name of the index. (templated)
    :param databricks_conn_id: Databricks connection ID. Defaults to ``databricks_default``. (templated)
    :param databricks_retry_limit: Maximum HTTP attempts, including the initial request. Defaults to 1
        to avoid replaying a sync after an ambiguous failure.
    :param databricks_retry_delay: Minimum delay between retryable HTTP attempts. Defaults to 1 second.
    :param databricks_retry_args: Optional ``tenacity.Retrying`` arguments. A non-empty dictionary
        overrides the default HTTP retry configuration.
    :param wait_for_completion: Wait for the index state after submitting the sync. Defaults to ``True``.
        This does not identify a particular sync execution.
    :param polling_period_seconds: Interval between status checks. Defaults to 30 seconds.
    :param timeout: Maximum wait after submission, including status requests and retries. Defaults to 3600 seconds.
    """

    template_fields: Sequence[str] = ("index_name", "databricks_conn_id")
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        *,
        index_name: str,
        databricks_conn_id: str = "databricks_default",
        databricks_retry_limit: int = 1,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        wait_for_completion: bool = True,
        polling_period_seconds: float = 30,
        timeout: float = 3600,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.index_name = index_name
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_completion = wait_for_completion
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        DatabricksHook.validate_vector_search_wait_parameters(timeout, polling_period_seconds)

    @cached_property
    def _hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=self.__class__.__name__,
        )

    def execute(self, context: Context) -> None:
        self._hook.sync_vector_search_index(self.index_name)
        self.log.info("Requested synchronization of Databricks Vector Search index %s.", self.index_name)
        if not self.wait_for_completion:
            return
        asyncio.run(
            self._hook.a_wait_for_vector_search_index(
                self.index_name, timeout=self.timeout, polling_period_seconds=self.polling_period_seconds
            )
        )
