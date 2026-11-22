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
"""Triggers for Databricks Vector Search indexes."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.databricks.exceptions import DatabricksVectorSearchError, DatabricksVectorSearchTimeout
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.utils.retry import validate_deferrable_databricks_retry_args
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DatabricksVectorSearchIndexTrigger(BaseTrigger):
    """
    Wait for an index to report an online state without pending updates.

    :param index_name: Three-level Unity Catalog name of the index.
    :param databricks_conn_id: Databricks connection ID.
    :param timeout: Maximum wait in seconds, including status requests and retries.
    :param polling_period_seconds: Interval between status checks, in seconds.
    :param retry_limit: Maximum HTTP attempts, including the initial request.
    :param retry_delay: Minimum delay between retryable HTTP attempts, in seconds.
    :param retry_args: Optional serializable Tenacity retry configuration.
    """

    def __init__(
        self,
        *,
        index_name: str,
        databricks_conn_id: str = "databricks_default",
        timeout: float = 3600,
        polling_period_seconds: float = 30,
        retry_limit: int = 1,
        retry_delay: int = 1,
        retry_args: dict[Any, Any] | None = None,
    ) -> None:
        super().__init__()
        DatabricksHook.validate_vector_search_wait_parameters(timeout, polling_period_seconds)
        validate_deferrable_databricks_retry_args(retry_args, owner=self.__class__.__name__)
        self.index_name = index_name
        self.databricks_conn_id = databricks_conn_id
        self.timeout = timeout
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.vector_search.DatabricksVectorSearchIndexTrigger",
            {
                "index_name": self.index_name,
                "databricks_conn_id": self.databricks_conn_id,
                "timeout": self.timeout,
                "polling_period_seconds": self.polling_period_seconds,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
            retry_args=self.retry_args,
            caller=self.__class__.__name__,
        )
        try:
            await hook.a_wait_for_vector_search_index(
                self.index_name, timeout=self.timeout, polling_period_seconds=self.polling_period_seconds
            )
        except DatabricksVectorSearchTimeout:
            status, message = "timeout", "Timed out waiting for the Vector Search index."
        except DatabricksVectorSearchError:
            status, message = (
                "error",
                "The Vector Search index failed to reach a state without pending updates.",
            )
        except Exception:
            status, message = "error", "Failed to read the Vector Search index status."
        else:
            status, message = "success", "The Vector Search index reports no pending updates."
        yield TriggerEvent({"status": status, "index_name": self.index_name, "message": message})
