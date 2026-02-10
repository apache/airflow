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

import importlib
import sys
from typing import TYPE_CHECKING

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook, DbApiHookAsync
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any

from collections.abc import (
    Iterable,
    Mapping,
)


class SQLGenericTransferTrigger(BaseTrigger):
    """
    A SQL trigger that executes SQL to get records in async mode.

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
        """Serialize the SQLGenericTransferTrigger arguments and classpath."""
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


class SQLExecuteQueryTrigger(BaseTrigger):
    """
    A SQL trigger that executes SQL code in async mode.

    :param sql: the sql statement to be executed (str) or a list of sql statements to execute
    :param conn_id: the connection ID used to connect to the database
    :param hook_params: hook parameters
    """

    def __init__(
        self,
        sql: str | Iterable[str],
        conn_id: str,
        autocommit: bool | None = None,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler_path: str | None = None,
        split_statements: bool | None = None,
        return_last: bool | None = None,
    ):
        super().__init__()
        self.sql = sql
        self.conn_id = conn_id
        self.autocommit = (autocommit,)
        self.parameters = (parameters,)
        self.handler_path = (handler_path,)
        self.split_statements = (split_statements,)
        self.return_last = return_last

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the SQLExecuteQueryTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "sql": self.sql,
                "conn_id": self.conn_id,
                "autocommit": self.autocommit,
                "parameters": self.parameters,
                "handler_path": self.handler_path,
                "split_statements": self.split_statements,
                "return_last": self.return_last,
            },
        )

    async def _import_from_handler_path(self):
        """Import the handler callable from the path provided by the user."""
        module_path, func_name = self.handler_path.rsplit(".", 1)
        if module_path in sys.modules:
            module = await sync_to_async(importlib.reload)(sys.modules[module_path])
        module = await sync_to_async(importlib.import_module)(module_path)
        return getattr(module, func_name)

    async def get_hook(self) -> DbApiHookAsync:
        """
        Return DbApiHookAsync.

        :return: DbApiHookAsync for this connection
        """
        connection = await sync_to_async(BaseHook.get_connection)(self.conn_id)
        hook = await sync_to_async(connection.get_hook)()
        if not isinstance(hook, DbApiHookAsync):
            raise AirflowException(
                f"You are trying to use `common-sql` with {hook.__class__.__name__},"
                " but its provider does not support it. Please upgrade the provider to a version that"
                " supports `common-sql`. The hook class should be a subclass of DbApiHookAsync"
                f" Got {hook.__class__.__name__} hook with class hierarchy: {hook.__class__.mro()}"
            )
        return hook

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            hook = await self.get_hook()
            handler = None
            if self.handler_path:
                handler = await self._import_from_handler_path()

            self.log.info("Extracting data from %s", self.conn_id)
            self.log.info("Executing: \n %s", self.sql)

            results = await hook.run_async(
                sql=self.sql,
                autocommit=self.autocommit,
                parameters=self.parameters,
                handler=handler,
                split_statements=self.split_statements,
                return_last=self.return_last,
            )

            self.log.info("Executing query from %s done!", self.conn_id)
            self.log.debug("results: %s", results)
            yield TriggerEvent({"status": "success", "results": results})
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
            yield TriggerEvent({"status": "failure", "message": str(e)})
