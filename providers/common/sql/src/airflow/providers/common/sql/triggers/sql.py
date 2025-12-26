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
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any


class SQLExecuteQueryTrigger(BaseTrigger):
    """
    A trigger that executes SQL code in async mode.

    :param sql: the sql statement to be executed (str) or a list of sql statements to execute
    :param conn_id: the connection ID used to connect to the database
    :param hook_params: hook parameters
    """

    def __init__(
        self,
        sql: str | list[str],
        conn_id: str,
        hook_params: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the SQLExecuteQueryTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "sql": self.sql,
                "conn_id": self.conn_id,
                "hook_params": self.hook_params,
            },
        )

    def get_hook(self) -> DbApiHook:
        """
        Return DbApiHook.

        :return: DbApiHook for this connection
        """
        connection = BaseHook.get_connection(self.conn_id)
        hook = connection.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"You are trying to use `common-sql` with {hook.__class__.__name__},"
                " but its provider does not support it. Please upgrade the provider to a version that"
                " supports `common-sql`. The hook class should be a subclass of"
                f" `{hook.__class__.__module__}.{hook.__class__.__name__}`."
                f" Got {hook.__class__.__name__} hook with class hierarchy: {hook.__class__.mro()}"
            )
        return hook

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            hook = self.get_hook()

            self.log.info("Extracting data from %s", self.conn_id)
            self.log.info("Executing: \n %s", self.sql)
            self.log.info("Reading records from %s", self.conn_id)

            results = hook.get_records(self.sql)

            self.log.info("Reading records from %s done!", self.conn_id)
            self.log.debug("results: %s", results)
            yield TriggerEvent({"status": "success", "results": results})
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
            yield TriggerEvent({"status": "failure", "message": str(e)})
