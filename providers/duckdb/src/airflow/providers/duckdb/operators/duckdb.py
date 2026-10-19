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
from airflow.providers.duckdb.version_compat import AirflowNotFoundException


class DuckDBExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Run SQL against an in-process `DuckDB <https://duckdb.org/>`__ database.

    Unlike the other :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`
    subclasses this operator does not require an Airflow connection to exist: with no
    ``duckdb_default`` connection configured it runs against an in-memory database, which is the common
    case for a task that reads its input, transforms it and writes its output back out.

    When a connection does exist, its type selects the hook. A provider can therefore ship a DuckDB
    connection type that adds its own behaviour (the Amazon provider's ``duckdb_aws`` brokers AWS
    credentials, for example) and a Dag moves between them by changing connections.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DuckDBExecuteQueryOperator`

    :param sql: the SQL statement, list of statements, or ``.sql`` template file to run.
    :param conn_id: DuckDB :ref:`connection <howto/connection:duckdb>` to use, or a connection of
        another DuckDB type such as ``duckdb_aws``. The default connection need not exist, in which
        case an in-memory database is used. Any other id must exist, and raises if it does not.
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
        Resolve the hook from the connection, and build one directly when there is no connection.

        ``BaseSQLOperator`` resolves the hook through ``BaseHook.get_connection(conn_id).get_hook()``,
        which raises when the connection is absent. DuckDB needs no connection to be useful, so a
        missing one is handed to ``hook_class`` to interpret rather than being an error here.

        When the connection does exist its type chooses the hook. That is what lets another provider
        add cloud-specific behaviour, such as brokering credentials, without a Dag having to swap
        operator: pointing this operator at a connection of that type is enough.
        """
        hook_params = dict(self.hook_params)
        # ``BaseSQLOperator`` applies ``database`` in ``_hook``, which this override bypasses, and it
        # would apply it as ``hook.schema`` — meaningless for DuckDB, where a database is a file path.
        if self.database:
            hook_params["database"] = self.database

        conn_id = self.conn_id or self.hook_class.default_conn_name
        try:
            connection = self.hook_class.get_connection(conn_id)
        except AirflowNotFoundException:
            # The hook re-raises this exception unless conn_id is its own default_conn_name, the
            # one id allowed to be absent so that DuckDB works with no configuration (default local
            # DB). Deciding that here as well would put the same policy in two places, and the hook
            # needs it anyway because it is can also be used directly.
            return self.hook_class(duckdb_conn_id=conn_id, **hook_params)

        # Deliberately not BaseSQLOperator.get_hook: it folds every connection extra into the hook's
        # constructor arguments, which would turn an unrecognised extra into a TypeError. DuckDBHook
        # reads extras itself, so the connection only has to pick the class.
        hook = connection.get_hook(hook_params=hook_params)
        if not isinstance(hook, DuckDBHook):
            raise ValueError(
                f"Connection {conn_id!r} has type {connection.conn_type!r}, which resolves to "
                f"{type(hook).__name__}. {type(self).__name__} needs a DuckDB connection type, so "
                f"use a connection whose hook derives from DuckDBHook."
            )
        return hook
