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

from typing import ClassVar

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.duckdb.hooks.duckdb import DuckDBHook


class DuckDBExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Run SQL against an in-process `DuckDB <https://duckdb.org/>`__ database.

    Unlike the other :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`
    subclasses this operator does not require an Airflow connection to exist: with none configured
    it runs against an in-memory database, which is the common case for a task that reads its input
    from object storage and writes its output back out.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DuckDBExecuteQueryOperator`

    :param sql: the SQL statement, list of statements, or ``.sql`` template file to run.
    :param conn_id: DuckDB :ref:`connection <howto/connection:duckdb>` to use. The connection need
        not exist, in which case an in-memory database is used.
    :param database: DuckDB database to open, for example ``/tmp/analytics.duckdb``. Overrides both the
        connection and ``hook_params["database"]``.
    :param hook_params: extra keyword arguments for
        :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook`, for example
        ``{"extensions": ["httpfs"], "memory_limit": "2GB"}``.
    """

    ui_color = "#ffe873"

    #: Hook this operator instantiates. Subclasses that add backend-specific behaviour override it.
    hook_class: ClassVar[type[DuckDBHook]] = DuckDBHook

    def __init__(self, *, conn_id: str = DuckDBHook.default_conn_name, **kwargs) -> None:
        super().__init__(conn_id=conn_id, **kwargs)

    def get_db_hook(self) -> DuckDBHook:
        """
        Build the hook directly instead of resolving it through the connection.

        ``BaseSQLOperator`` resolves the hook class via ``BaseHook.get_connection(conn_id).get_hook()``,
        which raises when the connection is absent. DuckDB needs no connection to be useful, so the
        hook is constructed here and left to decide what the missing connection means.
        """
        hook_params = dict(self.hook_params)
        # ``BaseSQLOperator`` applies ``database`` in ``_hook``, which this override bypasses, and it
        # would apply it as ``hook.schema`` — meaningless for DuckDB, where a database is a file path.
        if self.database:
            hook_params["database"] = self.database
        return self.hook_class(
            duckdb_conn_id=self.conn_id or self.hook_class.default_conn_name, **hook_params
        )
