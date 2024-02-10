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

from typing import TYPE_CHECKING, Any, Mapping

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

if TYPE_CHECKING:
    import pandas as pd
    from slack_sdk.http_retry import RetryHandler

    from airflow.providers.common.sql.hooks.sql import DbApiHook


class BaseSqlToSlackOperator(BaseOperator):
    """
    Operator implements base sql methods for SQL to Slack Transfer operators.

    :param sql: The SQL query to be executed
    :param sql_conn_id: reference to a specific DB-API Connection.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param parameters: The parameters to pass to the SQL query.
    :param slack_proxy: Proxy to make the Slack Incoming Webhook / API calls. Optional
    :param slack_timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. Optional
    :param slack_retry_handlers: List of handlers to customize retry logic. Optional
    """

    def __init__(
        self,
        *,
        sql: str,
        sql_conn_id: str,
        sql_hook_params: dict | None = None,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        slack_proxy: str | None = None,
        slack_timeout: int | None = None,
        slack_retry_handlers: list[RetryHandler] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_conn_id = sql_conn_id
        self.sql_hook_params = sql_hook_params
        self.sql = sql
        self.parameters = parameters
        self.slack_proxy = slack_proxy
        self.slack_timeout = slack_timeout
        self.slack_retry_handlers = slack_retry_handlers

    def _get_hook(self) -> DbApiHook:
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook(hook_params=self.sql_hook_params)
        if not callable(getattr(hook, "get_pandas_df", None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have get_pandas_df method."
            )
        return hook

    def _get_query_results(self) -> pd.DataFrame:
        sql_hook = self._get_hook()

        self.log.info("Running SQL query: %s", self.sql)
        df = sql_hook.get_pandas_df(self.sql, parameters=self.parameters)
        return df
