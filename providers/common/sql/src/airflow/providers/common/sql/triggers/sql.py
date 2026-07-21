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

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.hook import get_async_hook
from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_2_PLUS
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any

from collections.abc import (
    Iterable,
    Mapping,
    Sequence,
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

    async def _get_records(self) -> Any:
        hook = self.get_hook()

        if AIRFLOW_V_3_2_PLUS:
            # This is only supported from Airflow 3.2 or higher due to added async support in CommsDecoder
            return await sync_to_async(hook.get_records)(self.sql)
        return hook.get_records(self.sql)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            self.log.info("Extracting data from %s", self.conn_id)
            self.log.info("Executing: \n %s", self.sql)
            self.log.info("Reading records from %s", self.conn_id)

            results = await self._get_records()

            self.log.info("Reading records from %s done!", self.conn_id)
            self.log.debug("results: %s", results)
            yield TriggerEvent({"status": "success", "results": results})
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
            yield TriggerEvent({"status": "failure", "message": str(e)})


class SQLExecuteQueryTrigger(BaseTrigger):
    """
    A SQL trigger that executes SQL code in async mode.

    The query runs in the triggerer, but no user code does: when ``fetch_results`` is set the rows are
    fetched with the built-in :func:`fetch_all_handler` and returned, together with the cursor
    descriptions, in the ``TriggerEvent``. Any user-provided ``handler`` is applied on the worker in
    ``SQLExecuteQueryOperator.execute_complete`` -- keeping user code out of the triggerer's event loop
    and out of its (bundle-less) import path.

    :param sql: the sql statement to be executed (str) or a list of sql statements to execute
    :param conn_id: the connection ID used to connect to the database
    :param fetch_results: whether the query results should be fetched and returned to the worker
    """

    def __init__(
        self,
        sql: str | Iterable[str],
        conn_id: str,
        autocommit: bool,
        split_statements: bool,
        return_last: bool,
        parameters: Iterable[Any] | Mapping[str, Any] | None = None,
        fetch_results: bool = False,
        read_only: bool = False,
    ):
        super().__init__()
        self.sql = sql
        self.conn_id = conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.fetch_results = fetch_results
        self.split_statements = split_statements
        self.return_last = return_last
        self.read_only = read_only

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the SQLExecuteQueryTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "sql": self.sql,
                "conn_id": self.conn_id,
                "autocommit": self.autocommit,
                "parameters": self.parameters,
                "fetch_results": self.fetch_results,
                "split_statements": self.split_statements,
                "return_last": self.return_last,
                "read_only": self.read_only,
            },
        )

    @staticmethod
    def _jsonsafe_descriptions(
        descriptions: list[Sequence[Sequence] | None],
    ) -> list[list[list[Any]] | None]:
        """
        Normalise cursor descriptions into a JSON-serializable form for the ``TriggerEvent``.

        ``cursor.description`` is a sequence of column 7-tuples whose ``type_code`` can be a
        driver-specific object that is not JSON-serializable; such values are stringified while
        JSON-native fields (column names, sizes, precision, ...) are preserved.
        """
        safe: list[list[list[Any]] | None] = []
        for description in descriptions:
            if description is None:
                safe.append(None)
                continue
            safe.append(
                [
                    [
                        field if isinstance(field, (str, int, float, bool, type(None))) else str(field)
                        for field in column
                    ]
                    for column in description
                ]
            )
        return safe

    async def aget_hook(self) -> DbApiHook:
        """
        Return DbApiHook.

        :return: DbApiHook for this connection
        """
        hook = await get_async_hook(self.conn_id)
        if not isinstance(hook, DbApiHook) or not hasattr(hook, "arun"):
            raise AirflowException(
                f"You are trying to use the SqlExecuteQueryOperator in deferrable mode with {hook.__class__.__name__},"
                f" but its provider does not support this. Please set deferrable=False"
                f" Got {hook.__class__.__name__} with class hierarchy: {hook.__class__.mro()}"
            )
        if self.read_only and not hook.supports_readonly_execution():
            raise AirflowException(
                f"{hook.__class__.__name__} does not support read-only execution, so it cannot run a"
                " deferred query safely (a triggerer restart could re-run it). Set"
                " enforce_read_only=False to run without the read-only guard if the query is"
                " idempotent, or deferrable=False to run it on the worker."
            )
        return hook

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            hook = await self.aget_hook()

            self.log.info("Extracting data from %s", self.conn_id)
            self.log.info("Executing: \n %s", self.sql)

            if self.fetch_results:
                # Fetch the raw rows with the built-in handler and return them with the cursor
                # descriptions; the operator applies any user handler on the worker.
                results = await hook.arun(
                    sql=self.sql,
                    autocommit=self.autocommit,
                    parameters=self.parameters,
                    handler=fetch_all_handler,
                    split_statements=self.split_statements,
                    return_last=self.return_last,
                    read_only=self.read_only,
                )

                self.log.info("Executing query from %s done!", self.conn_id)
                self.log.debug("results: %s", results)
                yield TriggerEvent(
                    {
                        "status": "success",
                        "results": results,
                        "descriptions": self._jsonsafe_descriptions(hook.descriptions),
                    }
                )

            else:
                await hook.arun(
                    sql=self.sql,
                    autocommit=self.autocommit,
                    parameters=self.parameters,
                    handler=None,
                    split_statements=self.split_statements,
                    return_last=self.return_last,
                    read_only=self.read_only,
                )

                self.log.info("Executing query from %s done!", self.conn_id)
                yield TriggerEvent({"status": "success"})

        except Exception as e:
            self.log.error("status: error, message: %s", str(e))
            yield TriggerEvent({"status": "error", "message": str(e)})
