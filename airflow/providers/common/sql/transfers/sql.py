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

import re
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

_PROVIDERS_MATCHER = re.compile(r"airflow\.providers\.(.*)\.hooks.*")

_MIN_SUPPORTED_PROVIDERS_VERSION = {
    "amazon": "4.1.0",
    "apache.drill": "2.1.0",
    "apache.druid": "3.1.0",
    "apache.hive": "3.1.0",
    "apache.pinot": "3.1.0",
    "databricks": "3.1.0",
    "elasticsearch": "4.1.0",
    "exasol": "3.1.0",
    "google": "8.2.0",
    "jdbc": "3.1.0",
    "mssql": "3.1.0",
    "mysql": "3.1.0",
    "odbc": "3.1.0",
    "oracle": "3.1.0",
    "postgres": "5.1.0",
    "presto": "3.1.0",
    "qubole": "3.1.0",
    "slack": "5.1.0",
    "snowflake": "3.1.0",
    "sqlite": "3.1.0",
    "trino": "3.1.0",
    "vertica": "3.1.0",
}


class SqlToSqlOperator(BaseOperator):
    """
    Copy sql output data from one base to another database table.

    :param source_conn_id: the connection ID used to connect to the source database
    :param destination_conn_id: the connection ID used to connect to the destination database
    :param destination_table: the name of the destination table
    :param source_sql: the SQL code or string pointing to a template file to be executed (templated).
        File must have a '.sql' extension.
    :param source_parameters: (optional) the parameters to render the SQL query with.
    :param destination_before_sql: (optional) the SQL code or string to a template file to be executed (templated) before data transfer.
        File must have a '.sql' extension.
    :param destination_after_sql: (optional)  the SQL code or string to a template file to be executed (templated) after data transfer.
        File must have a '.sql' extension.
    :param destination_hook_params: hook parameters dictionary for the destination database
    :param source_hook_params: hook parameters dictionary for the source database
    :param rows_chunk: number of rows per chunk to commit.
    """

    template_fields: Sequence[str] = (
        "source_sql",
        "destination_before_sql",
        "destination_after_sql",
        "source_sql_parameters",
    )
    template_fields_renderers = {
        "source_sql": "sql",
        "destination_before_sql": "sql",
        "destination_after_sql": "sql",
        "source_sql_parameters": "json",
    }

    def __init__(
        self,
        *,
        source_conn_id: str,
        destination_conn_id: str,
        destination_table: str,
        source_sql: str,
        source_sql_parameters: Mapping[str, Any] | list[Any] | None = None,
        destination_before_sql: str | list[str] | None = None,
        destination_after_sql: str | list[str] | None = None,
        destination_hook_params: dict | None = None,
        source_hook_params: dict | None = None,
        rows_chunk: int = 5000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.destination_table = destination_table
        self.source_sql = source_sql
        self.source_sql_parameters = source_sql_parameters
        self.destination_before_sql = destination_before_sql
        self.destination_after_sql = destination_after_sql
        self.destination_hook_params = destination_hook_params
        self.source_hook_params = source_hook_params
        self.rows_chunk = rows_chunk

    def _hook(self, conn_id: str, hook_params: Mapping | Iterable | None = None):
        self.log.debug("Get connection for %s", conn_id)
        conn = BaseHook.get_connection(conn_id)
        hook = conn.get_hook(hook_params=hook_params)

        if not isinstance(hook, DbApiHook):
            from airflow.hooks.dbapi_hook import DbApiHook as _DbApiHook

            if not isinstance(hook, _DbApiHook):
                class_module = hook.__class__.__module__
                match = _PROVIDERS_MATCHER.match(class_module)
                if match:
                    provider = match.group(1)
                    min_version = _MIN_SUPPORTED_PROVIDERS_VERSION.get(provider)
                    if min_version:
                        raise AirflowException(
                            f"You are trying to use common-sql with {hook.__class__.__name__},"
                            f" but the Hook class comes from provider {provider} that does not support it."
                            f" Please upgrade provider {provider} to at least {min_version}."
                        )

            raise AirflowException(
                f"You are trying to use `common-sql` with {hook.__class__.__name__},"
                " but its provider does not support it. Please upgrade the provider to a version that"
                " supports `common-sql`. The hook class should be a subclass of"
                " `airflow.providers.common.sql.hooks.sql.DbApiHook`."
                f" Got {hook.__class__.__name__} Hook with class hierarchy: {hook.__class__.mro()}"
            )

        return hook

    def get_db_hook(self, conn_id: str, hook_params: Mapping | Iterable | None = None) -> DbApiHook:
        return self._hook(conn_id, hook_params)

    def _execute(self, source_hook, destination_hook, context: Context) -> None:
        self.log.info("Transferring data from %s to %s", self.source_conn_id, self.destination_conn_id)

        if self.destination_before_sql:
            self.log.info("Running before SQL on destination")
            destination_hook.run(self.destination_before_sql)

        destination_hook.data_transfer(
            self.destination_table, source_hook, self.source_sql, self.source_sql_parameters, self.rows_chunk
        )

        if self.destination_after_sql:
            self.log.info("Running before SQL on destination")
            destination_hook.run(self.destination_after_sql)

    def execute(self, context: Context) -> None:
        source_hook = self.get_db_hook(self.source_conn_id, self.source_hook_params)
        destination_hook = self.get_db_hook(self.destination_conn_id, self.destination_hook_params)

        self._execute(source_hook, destination_hook, context)
