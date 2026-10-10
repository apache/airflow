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
"""Microsoft SQLServer hook module."""

from __future__ import annotations

from functools import cached_property
from importlib import import_module
from typing import TYPE_CHECKING, Any

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.microsoft.mssql.dialects.mssql import MsSqlDialect

if TYPE_CHECKING:
    from pymssql import Connection as PymssqlConnection

    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.common.sql.dialects.dialect import Dialect
    from airflow.providers.openlineage.sqlparser import DatabaseInfo


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.

    The hook uses ``pymssql`` by default. Set the ``dbapi_driver`` connection extra to ``mssql_python``
    to use Microsoft's ``mssql-python`` driver instead (requires the ``mssql-python`` extra).

    :param args: passed to DBApiHook
    :param sqlalchemy_scheme: Scheme sqlalchemy connection.  Default is ``mssql+pymssql`` (or
      ``mssql+mssqlpython`` when ``dbapi_driver`` is ``mssql_python``). Only used for
      ``get_sqlalchemy_engine`` and ``get_sqlalchemy_connection`` methods.
    :param kwargs: passed to DbApiHook
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    conn_type = "mssql"
    hook_name = "Microsoft SQL Server"
    supports_autocommit = True
    DEFAULT_SQLALCHEMY_SCHEME = "mssql+pymssql"
    MSSQL_PYTHON_SQLALCHEMY_SCHEME = "mssql+mssqlpython"
    DBAPI_DRIVER_PYMSSQL = "pymssql"
    DBAPI_DRIVER_MSSQL_PYTHON = "mssql_python"
    SUPPORTED_DBAPI_DRIVERS = (DBAPI_DRIVER_PYMSSQL, DBAPI_DRIVER_MSSQL_PYTHON)
    # Connection extras that are handled by the hook itself and must never reach the DBAPI driver.
    _AIRFLOW_ONLY_EXTRAS = frozenset({"sqlalchemy_scheme", "dbapi_driver"})
    # mssql-python takes every other extra as a connection string keyword, so the extras that
    # ``DbApiHook`` reads for itself are filtered out as well.
    _MSSQL_PYTHON_IGNORED_EXTRAS = _AIRFLOW_ONLY_EXTRAS | frozenset(
        {
            "placeholder",
            "insert_statement_format",
            "replace_statement_format",
            "escape_word_format",
            "escape_column_names",
            "dialect",
        }
    )

    def __init__(
        self,
        *args,
        sqlalchemy_scheme: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **{**kwargs, **{"escape_word_format": "[{}]"}})
        self.schema = kwargs.pop("schema", None)
        self._sqlalchemy_scheme = sqlalchemy_scheme

    @property
    def dbapi_driver(self) -> str:
        """DBAPI driver, either ``pymssql`` (default) or ``mssql_python``, from the connection extra."""
        driver = str(self.connection_extra_lower.get("dbapi_driver") or self.DBAPI_DRIVER_PYMSSQL).lower()
        if driver not in self.SUPPORTED_DBAPI_DRIVERS:
            raise ValueError(
                f"Unsupported dbapi_driver {driver!r} in the connection extra. "
                f"Supported values: {', '.join(self.SUPPORTED_DBAPI_DRIVERS)}"
            )
        return driver

    @property
    def sqlalchemy_scheme(self) -> str:
        """Sqlalchemy scheme either from constructor, connection extras or the default of the driver."""
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self._default_sqlalchemy_scheme

    @property
    def _default_sqlalchemy_scheme(self) -> str:
        if self.dbapi_driver == self.DBAPI_DRIVER_MSSQL_PYTHON:
            return self.MSSQL_PYTHON_SQLALCHEMY_SCHEME
        return self.DEFAULT_SQLALCHEMY_SCHEME

    @cached_property
    def placeholder(self) -> str:
        """Return SQL placeholder, ``?`` for mssql_python (it has no plain ``%s`` support)."""
        if self.dbapi_driver == self.DBAPI_DRIVER_MSSQL_PYTHON and not self.connection_extra.get(
            "placeholder"
        ):
            return "?"
        return super().placeholder

    @property
    def dialect_name(self) -> str:
        return "mssql"

    @property
    def dialect(self) -> Dialect:
        return MsSqlDialect(self)

    def get_uri(self) -> str:
        from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

        r = list(urlsplit(super().get_uri()))
        # change the sqlalchemy driver:
        r[0] = self.sqlalchemy_scheme
        # remove query string parameters that are meant for the hook, not for the sqlalchemy dialect:
        qs = parse_qs(r[3], keep_blank_values=True)
        for k in list(qs.keys()):
            if k.lower() in self._AIRFLOW_ONLY_EXTRAS:
                qs.pop(k, None)
        r[3] = urlencode(qs, doseq=True)
        return urlunsplit(r)

    def get_sqlalchemy_connection(
        self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None
    ) -> Any:
        """Sqlalchemy connection object."""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        return engine.connect(**(connect_kwargs or {}))

    def get_conn(self) -> Any:
        """Return a ``pymssql`` or ``mssql_python`` connection object, depending on ``dbapi_driver``."""
        if self.dbapi_driver == self.DBAPI_DRIVER_MSSQL_PYTHON:
            return self._get_mssql_python_conn()
        return self._get_pymssql_conn()

    def _get_pymssql_conn(self) -> PymssqlConnection:
        """Return ``pymssql`` connection object."""
        import pymssql

        conn = self.connection
        extra_conn_args = {
            key: val for key, val in conn.extra_dejson.items() if key.lower() not in self._AIRFLOW_ONLY_EXTRAS
        }
        return pymssql.connect(
            server=conn.host or "",
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema or "",
            port=str(conn.port),
            **extra_conn_args,
        )

    def _get_mssql_python_conn(self) -> Any:
        """Return ``mssql_python`` connection object."""
        try:
            mssql_python = import_module("mssql_python")
        except ImportError as e:
            raise ImportError(
                "The 'mssql_python' dbapi_driver needs the mssql-python package. Install it with: "
                "pip install 'apache-airflow-providers-microsoft-mssql[mssql-python]'"
            ) from e

        conn = self.connection
        server = conn.host or ""
        if server and conn.port:
            server = f"{server},{conn.port}"
        params: dict[str, Any] = {
            "Server": server,
            "Database": self.schema or conn.schema,
            "UID": conn.login,
            "PWD": conn.password,
        }
        params.update(
            {
                key: val
                for key, val in conn.extra_dejson.items()
                if key.lower() not in self._MSSQL_PYTHON_IGNORED_EXTRAS
            }
        )
        # The keyword arguments are escaped by mssql-python when it builds the connection string.
        return mssql_python.connect(**{key: val for key, val in params.items() if val not in (None, "")})

    def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        if self.dbapi_driver == self.DBAPI_DRIVER_MSSQL_PYTHON:
            super().set_autocommit(conn, autocommit)
        else:
            conn.autocommit(autocommit)

    def get_autocommit(self, conn: Any) -> bool:
        if self.dbapi_driver == self.DBAPI_DRIVER_MSSQL_PYTHON:
            return super().get_autocommit(conn)
        return conn.autocommit_state

    def _make_common_data_structure(self, result: Any) -> tuple | list[tuple] | None:
        """Turn the ``Row`` objects of mssql_python into plain tuples."""
        if self.dbapi_driver != self.DBAPI_DRIVER_MSSQL_PYTHON:
            return super()._make_common_data_structure(result)
        if result is None:
            return None
        if isinstance(result, list):
            return [tuple(row) for row in result]
        return tuple(result)

    def get_openlineage_database_info(self, connection: Connection) -> DatabaseInfo:
        """Return MSSQL specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        return DatabaseInfo(
            scheme=self.get_openlineage_database_dialect(connection),
            authority=DbApiHook.get_openlineage_authority_part(connection, default_port=1433),
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "table_catalog",
            ],
            database=self.schema or self.connection.schema,
            is_information_schema_cross_db=True,
        )

    def get_openlineage_database_dialect(self, connection) -> str:
        """Return database dialect."""
        return "mssql"

    def get_openlineage_default_schema(self) -> str | None:
        """Return current schema."""
        return self.get_first("SELECT SCHEMA_NAME();")[0]
