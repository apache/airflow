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

import pymssql

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.microsoft.mssql.dialects.mssql import MsSqlDialect

if TYPE_CHECKING:
    from pymssql import Connection as PymssqlConnection
    from sqlalchemy.engine import URL

    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.common.sql.dialects.dialect import Dialect
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

# Connection extras that are handled by the hook itself and must never reach the DBAPI driver.
_AIRFLOW_ONLY_EXTRAS = frozenset({"sqlalchemy_scheme", "dbapi_driver"})


class _PymssqlDriver:
    """What ``MsSqlHook`` does differently when ``dbapi_driver`` is ``pymssql`` (the default)."""

    name = "pymssql"
    default_sqlalchemy_scheme = "mssql+pymssql"
    # None means the placeholder of ``DbApiHook``.
    placeholder: str | None = None

    def connect(self, hook: MsSqlHook) -> PymssqlConnection:
        conn = hook.connection
        extra_conn_args = {
            key: val for key, val in conn.extra_dejson.items() if key.lower() not in _AIRFLOW_ONLY_EXTRAS
        }
        return pymssql.connect(
            server=conn.host or "",
            user=conn.login,
            password=conn.password,
            database=hook.schema or conn.schema or "",
            port=str(conn.port),
            **extra_conn_args,
        )

    def sqlalchemy_url(self, hook: MsSqlHook) -> URL | None:
        # None means the default of ``DbApiHook``, so the engine is built from ``get_uri()``.
        return None

    def get_uri(self, hook: MsSqlHook) -> str:
        from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

        r = list(urlsplit(DbApiHook.get_uri(hook)))
        # change the sqlalchemy driver:
        r[0] = hook.sqlalchemy_scheme
        # remove query string parameters that are meant for the hook, not for the sqlalchemy dialect:
        qs = parse_qs(r[3], keep_blank_values=True)
        for k in list(qs.keys()):
            if k.lower() in _AIRFLOW_ONLY_EXTRAS:
                qs.pop(k, None)
        r[3] = urlencode(qs, doseq=True)
        return urlunsplit(r)

    def set_autocommit(self, hook: MsSqlHook, conn: Any, autocommit: bool) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, hook: MsSqlHook, conn: Any) -> bool:
        return conn.autocommit_state

    def make_common_data_structure(self, hook: MsSqlHook, result: Any) -> tuple | list[tuple] | None:
        return DbApiHook._make_common_data_structure(hook, result)


class _MssqlPythonDriver:
    """What ``MsSqlHook`` does differently when ``dbapi_driver`` is ``mssql_python``."""

    name = "mssql_python"
    default_sqlalchemy_scheme = "mssql+mssqlpython"
    # mssql-python has no plain ``%s`` support.
    placeholder: str | None = "?"
    # mssql-python takes every other extra as a connection string keyword, and the string built from a
    # SQLAlchemy URL fails on unknown keywords. So the extras that ``DbApiHook`` reads for itself are
    # filtered out for both the direct connection and the SQLAlchemy URL.
    ignored_extras = _AIRFLOW_ONLY_EXTRAS | frozenset(
        {
            "placeholder",
            "insert_statement_format",
            "replace_statement_format",
            "escape_word_format",
            "escape_column_names",
            "dialect",
        }
    )

    def extras(self, hook: MsSqlHook) -> dict[str, Any]:
        """Return the connection extras that are passed on to mssql-python, without the empty ones."""
        return {
            key: val
            for key, val in hook.connection.extra_dejson.items()
            if key.lower() not in self.ignored_extras and val not in (None, "")
        }

    def connect(self, hook: MsSqlHook) -> Any:
        try:
            mssql_python = import_module("mssql_python")
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(
                "The 'mssql_python' dbapi_driver needs the mssql-python package. Install it with: "
                "pip install 'apache-airflow-providers-microsoft-mssql[mssql-python]'"
            ) from e

        conn = hook.connection
        server = conn.host or ""
        if server and conn.port:
            server = f"{server},{conn.port}"
        params: dict[str, Any] = {
            "Server": server,
            "Database": hook.schema or conn.schema,
            "UID": conn.login,
            "PWD": conn.password,
        }
        params.update(self.extras(hook))
        # The keyword arguments are escaped by mssql-python when it builds the connection string.
        return mssql_python.connect(**{key: val for key, val in params.items() if val not in (None, "")})

    def sqlalchemy_url(self, hook: MsSqlHook) -> URL | None:
        return self.build_url(hook)

    def build_url(self, hook: MsSqlHook) -> URL:
        """
        Build the SQLAlchemy URL from the connection fields and extras.

        ``Connection.get_uri()`` packs extras that do not survive a round trip (a ``true`` for example)
        into one ``__extra__`` parameter, which mssql-python rejects as an unknown keyword.
        """
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "SQLAlchemy is not installed. Install it with: "
                "pip install 'apache-airflow-providers-common-sql[sqlalchemy]'"
            ) from err

        conn = hook.connection
        return URL.create(
            drivername=hook.sqlalchemy_scheme,
            username=conn.login or None,
            password=conn.password or None,
            host=conn.host or None,
            port=conn.port,
            database=hook.schema or conn.schema or None,
            query={key: str(val) for key, val in self.extras(hook).items()},
        )

    def get_uri(self, hook: MsSqlHook) -> str:
        return self.build_url(hook).render_as_string(hide_password=False)

    def set_autocommit(self, hook: MsSqlHook, conn: Any, autocommit: bool) -> None:
        DbApiHook.set_autocommit(hook, conn, autocommit)

    def get_autocommit(self, hook: MsSqlHook, conn: Any) -> bool:
        return DbApiHook.get_autocommit(hook, conn)

    def make_common_data_structure(self, hook: MsSqlHook, result: Any) -> tuple | list[tuple] | None:
        """Turn the ``Row`` objects of mssql_python into plain tuples and leave other results alone."""
        row_class = import_module("mssql_python").Row
        if isinstance(result, row_class):
            return tuple(result)
        if isinstance(result, list):
            return [tuple(row) if isinstance(row, row_class) else row for row in result]
        return result


_DRIVERS = {driver.name: driver for driver in (_PymssqlDriver(), _MssqlPythonDriver())}


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
    DEFAULT_SQLALCHEMY_SCHEME = _PymssqlDriver.default_sqlalchemy_scheme
    MSSQL_PYTHON_SQLALCHEMY_SCHEME = _MssqlPythonDriver.default_sqlalchemy_scheme
    DBAPI_DRIVER_PYMSSQL = _PymssqlDriver.name
    DBAPI_DRIVER_MSSQL_PYTHON = _MssqlPythonDriver.name
    SUPPORTED_DBAPI_DRIVERS = (DBAPI_DRIVER_PYMSSQL, DBAPI_DRIVER_MSSQL_PYTHON)

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
        driver = self.connection_extra_lower.get("dbapi_driver")
        if driver is None:
            return self.DBAPI_DRIVER_PYMSSQL

        if not isinstance(driver, str) or driver.lower() not in self.SUPPORTED_DBAPI_DRIVERS:
            raise ValueError(
                f"Unsupported dbapi_driver {driver!r} in the connection extra. "
                f"Supported values: {', '.join(sorted(self.SUPPORTED_DBAPI_DRIVERS))}"
            )

        return driver.lower()

    @cached_property
    def _driver(self) -> _PymssqlDriver | _MssqlPythonDriver:
        return _DRIVERS[self.dbapi_driver]

    @property
    def sqlalchemy_scheme(self) -> str:
        """Sqlalchemy scheme either from constructor, connection extras or the default of the driver."""
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self._driver.default_sqlalchemy_scheme

    @cached_property
    def placeholder(self) -> str:
        """Return SQL placeholder, ``?`` for mssql_python (it has no plain ``%s`` support)."""
        driver_placeholder = self._driver.placeholder
        if driver_placeholder and not self.connection_extra.get("placeholder"):
            return driver_placeholder
        return super().placeholder

    @property
    def dialect_name(self) -> str:
        return "mssql"

    @cached_property
    def dialect(self) -> Dialect:
        return MsSqlDialect(self)

    @property
    def sqlalchemy_url(self) -> URL:
        """Return the SQLAlchemy URL, built by the driver when it needs more than ``get_uri()``."""
        url = self._driver.sqlalchemy_url(self)
        if url is None:
            return super().sqlalchemy_url
        return url

    def get_uri(self) -> str:
        return self._driver.get_uri(self)

    def get_sqlalchemy_connection(
        self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None
    ) -> Any:
        """Sqlalchemy connection object."""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        return engine.connect(**(connect_kwargs or {}))

    def get_conn(self) -> Any:
        """Return a ``pymssql`` or ``mssql_python`` connection object, depending on ``dbapi_driver``."""
        return self._driver.connect(self)

    def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        self._driver.set_autocommit(self, conn, autocommit)

    def get_autocommit(self, conn: Any) -> bool:
        return self._driver.get_autocommit(self, conn)

    def _make_common_data_structure(self, result: Any) -> tuple | list[tuple] | None:
        return self._driver.make_common_data_structure(self, result)

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
